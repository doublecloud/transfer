package async

import (
	"context"
	"fmt"
	"sync"

	"github.com/cenkalti/backoff/v4"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/library/go/core/xerrors/multierr"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/clickhouse/async/dao"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/clickhouse/async/model/parts"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/clickhouse/columntypes"
	chsink "github.com/doublecloud/tross/transfer_manager/go/pkg/providers/clickhouse/errors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/clickhouse/sharding"
	"go.ytsaurus.tech/library/go/core/log"
)

type part struct {
	cluster     ClusterClient
	dao         *dao.DDLDAO
	sharder     sharding.Sharder
	shards      sharding.ShardMap[*shardPart]
	shardsMu    sync.RWMutex
	dbName      string
	id          abstract.TablePartID
	lgr         log.Logger
	query       string
	transferID  string
	tableCols   map[abstract.TableID]columntypes.TypeMapping
	tableColsMu sync.Mutex
}

const TMPPrefix = "dt_tmp_part"

func (p *part) tmpTableName() string {
	return fmt.Sprintf("%s_%s_%s_%s", TMPPrefix, p.transferID, p.id.Name, p.id.PartID)
}

func (p *part) Append(rows []abstract.ChangeItem) error {
	if len(rows) < 1 {
		return nil
	}

	shardMask := make([]sharding.ShardID, len(rows))
	shardIDs := make(map[sharding.ShardID]bool)
	for i, row := range rows {
		shardID := p.sharder(row)
		shardMask[i] = shardID
		shardIDs[shardID] = true
	}

	errCh := make(chan error, len(shardIDs))
	var wg sync.WaitGroup
	for shardID := range shardIDs {
		wg.Add(1)
		go func(sID sharding.ShardID) {
			defer wg.Done()
			for i, row := range rows {
				if shardMask[i] != sID {
					continue
				}
				if err := p.shardAppend(sID, row); err != nil {
					errCh <- err
					return
				}
			}
		}(shardID)
	}
	wg.Wait()
	close(errCh)

	var err error
	for shardErr := range errCh {
		err = multierr.Append(err, shardErr)
	}
	return err
}

func (p *part) Commit() error {
	var err error
	for shardID, shard := range p.shards {
		if shardErr := shard.Commit(); shardErr != nil {
			err = multierr.Append(err, xerrors.Errorf("error commiting part %s of table %s for shard %d: %w", p.id.PartID, p.id.Name, shardID, shardErr))
		}
	}
	p.shards = nil
	return err
}

func (p *part) Close() error {
	var err error
	for _, shard := range p.shards {
		logger.Log.Debug("part: closing shard")
		err = multierr.Append(err, shard.Close())
	}
	p.shards = nil
	return err
}

func (p *part) getOrCreateShardPart(shardID sharding.ShardID) (*shardPart, error) {
	p.shardsMu.RLock()
	shard, ok := p.shards[shardID]
	p.shardsMu.RUnlock()
	if ok {
		return shard, nil
	}

	logger.Log.Infof("Starting stream inserting for part %s of table %s.%s shard %d", p.id.PartID, p.dbName, p.id.Name, shardID)
	if err := backoff.Retry(func() error {
		s, err := p.createShardPart(shardID)
		if err != nil {
			if chsink.IsFatalClickhouseError(err) {
				err = backoff.Permanent(err)
			}
			return err
		}
		shard = s
		return nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)); err != nil {
		return nil, xerrors.Errorf("error creating part %s of table %s on shard %d: %w", p.id.PartID, p.id.Name, shardID, err)
	}
	p.shardsMu.Lock()
	defer p.shardsMu.Unlock()
	p.shards[shardID] = shard
	return shard, nil
}

func (p *part) getTableCols(client DDLStreamingClient) (columntypes.TypeMapping, error) {
	p.tableColsMu.Lock()
	defer p.tableColsMu.Unlock()

	tableID := *abstract.NewTableID(p.dbName, p.id.Name)
	cols, found := p.tableCols[tableID]
	if found {
		return cols, nil
	}

	p.lgr.Infof("No schema cache for table %s", p.id.Name)
	if err := p.initTableCols(client); err != nil {
		return nil, xerrors.Errorf("error reading table %s columns: %w", p.id.Name, err)
	}
	return p.tableCols[tableID], nil
}

func (p *part) initTableCols(client DDLStreamingClient) error {
	tableID := *abstract.NewTableID(p.dbName, p.id.Name)
	p.lgr.Infof("Loading columns for table %s", tableID.String())
	query := "SELECT name, type FROM system.columns WHERE database = ? AND table = ?;"
	rows, err := client.QueryContext(context.Background(), query, p.dbName, p.id.Name)
	if err != nil {
		return xerrors.Errorf("table %s column query failed: %w", tableID.String(), err)
	}
	colTypes := make(columntypes.TypeMapping)
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return xerrors.Errorf("error scanning columns of table %s: %w", tableID.String(), err)
		}
		colTypes[name] = columntypes.NewTypeDescription(typ)
	}
	if err := rows.Err(); err != nil {
		return xerrors.Errorf("error reading columns for table %s: %w", tableID.String(), err)
	}
	if len(colTypes) == 0 {
		return xerrors.Errorf("got empty schema for table %s", tableID.String())
	}
	p.tableCols[tableID] = colTypes
	return nil
}

func (p *part) createShardPart(shardID sharding.ShardID) (*shardPart, error) {
	hostDB, err := p.cluster.Shard(shardID).AliveHost()
	if err != nil {
		return nil, xerrors.Errorf("error getting host from shard client: %w", err)
	}

	cols, err := p.getTableCols(hostDB)
	if err != nil {
		return nil, xerrors.Errorf("error getting table cols for table '%s': %w", p.id.Name, err)
	}

	shardPart, err := newShardPart(p.dbName, p.id.Name, p.dbName, p.tmpTableName(), p.query, hostDB, cols)
	if err != nil {
		return nil, xerrors.Errorf("error making new part for shard %d: %w", shardID, err)
	}

	return shardPart, nil
}

func (p *part) shardAppend(shardID sharding.ShardID, row abstract.ChangeItem) error {
	shard, err := p.getOrCreateShardPart(shardID)
	if err != nil {
		return xerrors.Errorf("error creating part %s of table %s on shard %d: %w", p.id.PartID, p.id.Name, shardID, err)
	}
	return shard.Append(row)
}

func NewPart(
	partID abstract.TablePartID, dbName string, cl ClusterClient, dao *dao.DDLDAO,
	sharder sharding.Sharder, lgr log.Logger, transferID string,
) parts.Part {
	return &part{
		cluster:     cl,
		dao:         dao,
		sharder:     sharder,
		shards:      make(sharding.ShardMap[*shardPart]),
		shardsMu:    sync.RWMutex{},
		dbName:      dbName,
		id:          partID,
		lgr:         lgr,
		query:       "",
		transferID:  transferID,
		tableCols:   make(map[abstract.TableID]columntypes.TypeMapping),
		tableColsMu: sync.Mutex{},
	}
}
