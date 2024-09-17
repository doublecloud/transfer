package staticsink

import (
	"fmt"
	"time"

	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	yt2 "github.com/doublecloud/transfer/pkg/providers/yt"
	ytclient "github.com/doublecloud/transfer/pkg/providers/yt/client"
	"github.com/doublecloud/transfer/pkg/providers/yt/sink/v2/statictable"
	"github.com/doublecloud/transfer/pkg/providers/yt/sink/v2/transactions"
	"github.com/doublecloud/transfer/pkg/stats"
	"github.com/doublecloud/transfer/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

var (
	expectedKinds = util.NewSet(
		abstract.InitShardedTableLoad,
		abstract.InitTableLoad,
		abstract.InsertKind,
		abstract.DoneTableLoad,
		abstract.DoneShardedTableLoad,
	)
)

type staticTableWriter interface {
	Write(items []abstract.ChangeItem) error
	Commit() error
}

type mainTransaction interface {
	BeginTx() error
	Commit() error
	Close()

	BeginSubTx() (yt.Tx, error)
	ExecOrAbort(fn func(mainTxID yt.TxID) error) error
}

type sink struct {
	ytClient   yt.Client
	dir        ypath.Path
	config     yt2.YtDestinationModel
	transferID string

	mainTx mainTransaction
	partTx yt.Tx
	writer staticTableWriter

	handledSystemItems map[abstract.Kind]*util.Set[string]

	metrics *stats.SinkerStats
	logger  log.Logger
}

func (s *sink) Push(items []abstract.ChangeItem) error {
	if len(items) == 0 || !expectedKinds.Contains(items[0].Kind) {
		return nil
	}
	itemsKind := items[0].Kind
	tablePath := s.getTablePath(items[0])
	schema := items[0].TableSchema

	// deduplicate system items
	if handledPaths, ok := s.handledSystemItems[itemsKind]; ok {
		if handledPaths.Contains(tablePath.String()) {
			return nil
		}
	}

	if itemsKind == abstract.InitShardedTableLoad {
		if err := s.mainTx.BeginTx(); err != nil {
			return xerrors.Errorf("preparing main tx error: %w", err)
		}
		if err := s.initTableLoad(tablePath, schema.Columns()); err != nil {
			return xerrors.Errorf("unable push InitShTableLoad item to %s: %w", tablePath, err)
		}
		s.handledSystemItems[abstract.InitShardedTableLoad].Add(tablePath.String())
		return nil
	}

	switch itemsKind {
	case abstract.InitTableLoad:
		var err error
		if err = s.beginPartTx(); err != nil {
			return xerrors.Errorf("unable to push InitTableLoad item to %s: %w", tablePath, err)
		}
		if s.writer, err = s.createWriter(tablePath); err != nil {
			return xerrors.Errorf("unable to push InitTableLoad item to %s: %w", tablePath, err)
		}
	case abstract.InsertKind:
		if err := s.writer.Write(items); err != nil {
			return xerrors.Errorf("unable to push Insert items to %s: %w", tablePath, err)
		}
	case abstract.DoneTableLoad:
		if err := s.writer.Commit(); err != nil {
			return xerrors.Errorf("unable to push DoneTableLoad item to %s: %w", tablePath, err)
		}
		if err := s.commitPartTx(); err != nil {
			return xerrors.Errorf("unable to push DoneTableLoad item to %s: %w", tablePath, err)
		}
	case abstract.DoneShardedTableLoad:
		if err := s.commitTable(tablePath, schema.Columns()); err != nil {
			return xerrors.Errorf("unable to push DoneShTableLoad item to %s: %w", tablePath, err)
		}
		s.handledSystemItems[abstract.DoneShardedTableLoad].Add(tablePath.String())
	}

	return nil
}

func (s *sink) Commit() error {
	return s.mainTx.Commit()
}

func (s *sink) Close() error {
	if s.partTx != nil {
		_ = s.partTx.Abort()
	}
	s.mainTx.Close()
	return nil
}

func (s *sink) initTableLoad(tablePath ypath.Path, schema abstract.TableColumns) error {
	fn := func(mainTxID yt.TxID) error {
		if err := statictable.Init(s.ytClient, &statictable.InitOptions{
			MainTxID:         mainTxID,
			TransferID:       s.transferID,
			Schema:           schema,
			Path:             tablePath,
			OptimizeFor:      s.config.OptimizeFor(),
			CustomAttributes: s.config.CustomAttributes(),
			Logger:           s.logger,
		}); err != nil {
			return err
		}

		return nil
	}

	return s.mainTx.ExecOrAbort(fn)
}

func (s *sink) beginPartTx() error {
	tx, err := s.mainTx.BeginSubTx()
	if err != nil {
		return err
	}
	s.partTx = tx
	return nil
}

func (s *sink) createWriter(tablePath ypath.Path) (staticTableWriter, error) {
	return statictable.NewWriter(statictable.WriterConfig{
		TransferID: s.transferID,
		TxClient:   s.partTx,
		Path:       tablePath,
		Spec:       s.config.Spec().GetConfig(),
		ChunkSize:  s.config.StaticChunkSize(),
		Logger:     s.logger,
		Metrics:    s.metrics,
	})
}

func (s *sink) commitPartTx() error {
	if s.partTx == nil {
		return xerrors.New("unable to commit part transaction: part transaction hasn't been started yet")
	}
	if err := s.partTx.Commit(); err != nil {
		return xerrors.Errorf("unable to commit part transaction: %w", err)
	}
	s.logger.Info("part transaction has been committed", log.Any("tx_id", s.partTx.ID()))
	return nil
}

func (s *sink) commitTable(tablePath ypath.Path, scheme abstract.TableColumns) error {
	fn := func(mainTxID yt.TxID) error {
		startMoment := time.Now()
		if err := statictable.Commit(s.ytClient, &statictable.CommitOptions{
			MainTxID:         mainTxID,
			TransferID:       s.transferID,
			Schema:           scheme,
			Path:             tablePath,
			CleanupType:      s.config.CleanupMode(),
			AllowedSorting:   s.config.SortedStatic(),
			Pool:             s.config.Pool(),
			OptimizeFor:      s.config.OptimizeFor(),
			CustomAttributes: s.config.CustomAttributes(),
			Logger:           s.logger,
		}); err != nil {
			return err
		}
		s.logger.Info("table was committed", log.String("table_path", tablePath.String()),
			log.Duration("elapsed_time", time.Since(startMoment)))

		return nil
	}

	return s.mainTx.ExecOrAbort(fn)
}

func (s *sink) getTablePath(item abstract.ChangeItem) ypath.Path {
	tableName := getNameFromTableID(item.TableID())
	if s.config == nil {
		return yt2.SafeChild(s.dir, tableName)
	}
	return yt2.SafeChild(s.dir, s.config.GetTableAltName(tableName))
}

func getNameFromTableID(id abstract.TableID) string {
	if id.Namespace == "public" {
		return id.Name
	}
	return fmt.Sprintf("%s_%s", id.Namespace, id.Name)
}

func NewStaticSink(cfg yt2.YtDestinationModel, cp coordinator.Coordinator, transferID string, registry metrics.Registry, logger log.Logger) (abstract.Sinker, error) {
	ytClient, err := ytclient.FromConnParams(cfg, logger)
	if err != nil {
		return nil, err
	}

	return &sink{
		ytClient:   ytClient,
		dir:        ypath.Path(cfg.Path()),
		config:     cfg,
		transferID: transferID,
		mainTx:     transactions.NewMainTxClient(transferID, cp, ytClient, logger),
		partTx:     nil,
		writer:     nil,
		handledSystemItems: map[abstract.Kind]*util.Set[string]{
			abstract.InitShardedTableLoad: util.NewSet[string](),
			abstract.DoneShardedTableLoad: util.NewSet[string](),
		},
		metrics: stats.NewSinkerStats(registry),
		logger:  logger,
	}, nil
}
