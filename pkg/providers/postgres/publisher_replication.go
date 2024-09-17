package postgres

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/format"
	"github.com/doublecloud/transfer/pkg/parsequeue"
	sequencer2 "github.com/doublecloud/transfer/pkg/providers/postgres/sequencer"
	"github.com/doublecloud/transfer/pkg/stats"
	"github.com/doublecloud/transfer/pkg/util"
	"github.com/dustin/go-humanize"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.ytsaurus.tech/library/go/core/log"
)

type replication struct {
	logger          log.Logger
	conn            *pgxpool.Pool
	replConn        *mutexedPgConn
	metrics         *stats.SourceStats
	wal2jsonParser  *Wal2JsonParser
	error           chan error
	once            sync.Once
	config          *PgSource
	transferID      string
	schema          abstract.DBSchema
	altNames        map[abstract.TableID]abstract.TableID
	wg              sync.WaitGroup
	slotMonitor     *SlotMonitor
	stopCh          chan struct{}
	mutex           *sync.Mutex
	maxLsn          uint64
	slot            AbstractSlot
	pgVersion       PgVersion
	lastKeeperTime  time.Time
	includeCache    map[abstract.TableID]bool
	cp              coordinator.Coordinator
	sharedCtx       context.Context
	sharedCtxCancel context.CancelFunc
	changeProcessor *changeProcessor
	objects         *server.DataObjects
	sequencer       *sequencer2.Sequencer
	parseQ          *parsequeue.ParseQueue[[]abstract.ChangeItem]
	objectsMap      map[abstract.TableID]bool //tables to include in transfer

	skippedTables map[abstract.TableID]bool
}

var pgFatalCode = map[string]bool{
	"XX000": true, // TM-1332
	"58P01": true, // TM-2082
	"55000": true, // TRANSFER-145 Object_not_in_prerequisite_state
}

const BufferLimit = 16 * humanize.MiByte

func (p *replication) Run(sink abstract.AsyncSink) error {
	var err error
	//level of parallelism combined with hardcoded buffer size in receiver(16mb) prevent OOM in parsequeue
	p.parseQ = parsequeue.New(p.logger, 10, sink, p.WithIncludeFilter, p.ack)

	if err = p.slot.Init(sink); err != nil {
		return xerrors.Errorf("unable to init slot: %w", err)
	}
	if err = p.reloadSchema(); err != nil {
		return xerrors.Errorf("failed to load schema: %w", err)
	}

	includedObjects := p.objects.GetIncludeObjects()
	if len(includedObjects) > 0 {
		includedObjects = append(includedObjects, p.config.AuxTables()...)
	}

	if p.objectsMap, err = abstract.BuildIncludeMap(includedObjects); err != nil {
		return xerrors.Errorf("unable to build transfer data-objects: %w", err)
	}

	slotTroubleCh := p.slotMonitor.StartSlotMonitoring(int64(p.config.SlotByteLagLimit))

	p.wg.Add(2)
	go p.receiver(slotTroubleCh)
	go p.standbyStatus()
	select {
	case err := <-p.error:
		return err
	case <-p.stopCh:
		return nil
	}
}

func (p *replication) Stop() {
	p.once.Do(func() {
		close(p.stopCh)
		p.sharedCtxCancel()
		if err := p.replConn.Close(context.Background()); err != nil {
			p.logger.Error("Cannot close replication connection", log.Error(err))
		}
		if tracker, ok := p.slot.(*LsnTrackedSlot); ok {
			tracker.Close()
		}
		p.wg.Wait()
		p.slotMonitor.Close()
		p.wal2jsonParser.Close()
		p.conn.Close()
		p.parseQ.Close()
	})
}

func (p *replication) sendError(err error) {
	select {
	case p.error <- err:
		p.logger.Error("error, stop replication", log.Error(err))
	case <-p.stopCh:
		if err != nil {
			p.logger.Warn("error after replication stopped", log.Error(err))
		}
	}
}

func (p *replication) ack(data []abstract.ChangeItem, pushSt time.Time, err error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !util.IsOpen(p.stopCh) {
		return
	}

	if err != nil {
		p.sendError(err)
		return
	}

	if committedLsn, err := p.sequencer.Pushed(data); err != nil {
		logger.Log.Error("sequence of processed changeItems is incorrect", log.Error(err))
		p.sendError(err)
		return
	} else {
		p.maxLsn = committedLsn
	}

	p.metrics.PushTime.RecordDuration(time.Since(pushSt))
}

func (p *replication) WithIncludeFilter(items []abstract.ChangeItem) []abstract.ChangeItem {
	var changes []abstract.ChangeItem
	for _, change := range items {
		if _, ok := p.includeCache[change.TableID()]; !ok {
			// while CollapseInheritTables rename items inplace replication, we have to check both names: original and new
			p.includeCache[change.TableID()] = p.config.Include(change.TableID()) || p.config.Include(origTableID(&change))
		}
		if len(p.objectsMap) > 0 { // if we have transfer include objects we should strictly push only them
			_, ok := p.objectsMap[change.TableID()]
			if !ok {
				continue
			}
		}
		if p.includeCache[change.TableID()] {
			changes = append(changes, change)
		}
	}
	return changes
}

// ChangeItem could be renamed(when Namespace and Name are overriden) by enabling CollapseInheritTables
// For some reasons we need to know original Namespace and Name while final filtering change items before sending to sinker.
// Original Namespace and Name are stored in TableSchema even after renaming.
func origTableID(c *abstract.ChangeItem) abstract.TableID {
	if len(c.TableSchema.Columns()) > 0 {
		col := c.TableSchema.Columns()[0]
		return abstract.TableID{Namespace: col.TableSchema, Name: col.TableName}
	}
	return c.TableID()
}

func (p *replication) fillAltNamesForInheritedTables(ctx context.Context, includedSchema abstract.DBSchema) (ignoredParentTables []abstract.TableID, err error) {
	rows, err := p.conn.Query(ctx, "select inhparent::regclass::text, inhrelid::regclass::text from pg_inherits;")
	if err != nil {
		return nil, xerrors.Errorf("failed inherited tables query: %w", err)
	}
	defer rows.Close()

	p.altNames = map[abstract.TableID]abstract.TableID{}
	invertSch := map[abstract.TableID]abstract.TableID{}
	for rows.Next() {
		var from, to string
		if err := rows.Scan(&to, &from); err != nil {
			return nil, xerrors.Errorf("cannot parse inherited tables query result: %w", err)
		}
		fromID, ok := makeTableID(from)
		if !ok {
			return nil, xerrors.Errorf("cannot parse regclass text: %s", from)
		}
		toID, ok := makeTableID(to)
		if !ok {
			return nil, xerrors.Errorf("cannot parse regclass text: %s", to)
		}
		p.altNames[fromID] = toID
		if _, ok := invertSch[toID]; !ok {
			if _, ok := p.schema[fromID]; ok {
				invertSch[toID] = fromID
			}
		}
	}
	ignoredParentTables = []abstract.TableID{}
	for to, from := range invertSch {
		if s, ok := p.schema[to]; !ok || !s.Columns().HasPrimaryKey() || s.Columns().HasFakeKeys() {
			// 1. Inherit tables do not have any real schema, but it still need to be schemitezed
			//    All child table must have same schema i.e. parent can be infered from any child
			// 2. It`s impossible to define PK on parent(partitioned) tables in early PG versions
			if _, included := includedSchema[to]; included {
				ignoredParentTables = append(ignoredParentTables, to)
				delete(includedSchema, to)
			}
			p.schema[to] = p.schema[from]
		}
	}
	return ignoredParentTables, nil
}

func (p *replication) reloadSchema() error {
	storage, err := NewStorage(p.config.ToStorageParams(nil)) // source includes all data transfer system tables
	if err != nil {
		return xerrors.Errorf("failed to create PostgreSQL storage object at source endpoint: %w", err)
	}
	defer storage.Close()
	storage.IsHomo = true // exclude VIEWs. This is a nasty solution which should be replaced when an Accessor is introduced instead of the jack of all trades Storage

	tableMap, err := storage.TableList(nil)
	if err != nil {
		return xerrors.Errorf("failed to list tables (with schema) at source endpoint: %w", err)
	}

	dbSchema := tableMapToDBSchemaForTables(tableMap)
	p.schema = dbSchema

	dbSchemaToCheck, err := abstract.SchemaFilterByObjects(dbSchema, p.objects.GetIncludeObjects())
	if err != nil {
		return xerrors.Errorf("failed to filter table list extracted from source by objects set in transfer: %w", err)
	}

	if p.config.CollapseInheritTables {
		invalidParentTables, err := p.fillAltNamesForInheritedTables(p.sharedCtx, dbSchema)
		if err != nil {
			return xerrors.Errorf("failed to find all inherited tables: %w", err)
		}
		if err := coordinator.ReportFakePKey(p.cp, p.transferID, FakeParentPKeyStatusMessageCategory, invalidParentTables); err != nil {
			return xerrors.Errorf("cannot report transfer warning: %w", err)
		}
	}

	if err := dbSchemaToCheck.CheckPrimaryKeys(p.config); err != nil {
		p.metrics.Fatal.Inc()
		return xerrors.Errorf("primary key check failed: %w", abstract.NewFatalError(err))
	}
	if err := coordinator.ReportFakePKey(p.cp, p.transferID, coordinator.FakePKeyStatusMessageCategory, dbSchemaToCheck.FakePkeyTables(p.config)); err != nil {
		return xerrors.Errorf("Cannot report transfer warning: %w", err)
	}

	conn, err := storage.Conn.Acquire(p.sharedCtx)
	if err != nil {
		return xerrors.Errorf("failed to acquire a connection from connection pool: %w", err)
	}
	defer conn.Release()

	changeProcessor, err := newChangeProcessor(conn.Conn(), p.schema, storage.sExTime, p.altNames, p.config)
	if err != nil {
		return xerrors.Errorf("unable to initialize change processor: %w", err)
	}

	p.changeProcessor = changeProcessor
	return nil
}

const FakeParentPKeyStatusMessageCategory string = "fake_primary_key_parent"

// tableMapToDBSchemaForTables converts one type of schema to another ONLY for tables (dropping VIEWs)
func tableMapToDBSchemaForTables(tableMap abstract.TableMap) abstract.DBSchema {
	result := make(abstract.DBSchema)
	for id, info := range tableMap {
		if info.IsView {
			continue
		}
		result[id] = info.Schema
	}
	return result
}

func (p *replication) standbyStatus() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	defer p.wg.Done()

	var lastLsn uint64

	for range ticker.C {
		select {
		case <-p.stopCh:
			return
		default:
		}
		p.mutex.Lock()
		copiedMaxLsn := p.maxLsn
		p.mutex.Unlock()

		if lastLsn != copiedMaxLsn {
			lastLsn = copiedMaxLsn
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
		statusUpdate := pglogrepl.StandbyStatusUpdate{WALWritePosition: pglogrepl.LSN(copiedMaxLsn)}
		if err := p.replConn.SendStandbyStatusUpdate(ctx, statusUpdate); err != nil {
			logger.Log.Warn("Unable to send standby status", log.Error(err))
		} else {
			p.logger.Infof("Heartbeat send %v", copiedMaxLsn)
		}
		cancel()
		if tracker, ok := p.slot.(*LsnTrackedSlot); ok && copiedMaxLsn > 0 {
			if err := tracker.Move(pglogrepl.LSN(copiedMaxLsn).String()); err != nil {
				logger.Log.Warn("Unable to move lsn", log.Error(err))
			}
		}
	}
}

func (p *replication) receiver(slotTroubleCh <-chan error) {
	defer p.wg.Done()
	defer logger.Log.Info("Receiver stopped")
	var lastLsn uint64
	var cTime time.Time
	parsed := true
	var lastMessageTime time.Time
	var messageCounter int
	var data []*pglogrepl.XLogData
	bufferSize := server.BytesSize(0)
	for {
		select {
		case <-p.stopCh:
			logger.Log.Warn("Force stop")
			return
		case err := <-slotTroubleCh:
			p.logger.Error("Replication slotID error", log.Error(err))
			p.metrics.Fatal.Inc()
			p.sendError(err)
			return
		default:
		}
		p.metrics.Master.Set(1)

		backendMessage, err := p.replConn.ReceiveMessage(p.sharedCtx)
		if err != nil {
			if xerrors.Is(err, context.Canceled) {
				return
			}
			var pgErr *pgconn.PgError
			if xerrors.As(err, &pgErr) && pgFatalCode[pgErr.Code] {
				p.logger.Error("Pg fatal error", log.Error(err))
				p.metrics.Fatal.Inc()
				p.sendError(abstract.NewFatalError(err))
				return
			}
			p.logger.Warn("Connection dropped", log.Error(err))
			p.sendError(err)
			return
		}

		message, ok := backendMessage.(*pgproto3.CopyData)
		if !ok {
			// Happens when PostgreSQL sends an information message, like a warning
			continue
		}

		switch message.Data[0] {
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(message.Data[1:])
			if err != nil {
				p.sendError(xerrors.Errorf("unknown pg message: %w", err))
				return
			}
			messageCounter++
			p.metrics.Size.Add(int64(len(xld.WALData)))
			p.metrics.Count.Inc()
			p.metrics.DelayTime.RecordDuration(time.Since(xld.ServerTime))

			transactionComplete := string(xld.WALData) == "]}"
			shouldFlush := (bufferSize > BufferLimit) || transactionComplete

			if !parsed && bufferSize > BufferLimit {
				for !parsed {
					time.Sleep(time.Second)
					p.logger.Infof("buffer size too large while data inflight, throttle read: %v", format.SizeInt(int(bufferSize)))
				}
			}

			data = append(data, &xld)
			if shouldFlush && parsed {
				parsed = false
				go func(data []*pglogrepl.XLogData) {
					defer func() {
						parsed = true
					}()
					var res []abstract.ChangeItem
					for _, d := range data {
						changeItems, err := p.parseWal2JsonChanges(p.changeProcessor, d)
						if err != nil {
							p.sendError(xerrors.Errorf("Cannot parse logical replication message: %w", err))
							return
						}
						res = append(res, changeItems...)
					}
					if len(res) == 0 {
						return
					}
					if time.Since(lastMessageTime) > 5*time.Second {
						xldString := fmt.Sprintf("Wal: %s Time: %s Lag: %d", xld.WALStart, xld.ServerTime, uint64(xld.ServerWALEnd-xld.WALStart))
						p.logger.Infof(
							"Read rows %v %v (oldest %v newest %v count %d)",
							xldString,
							humanize.Bytes(uint64(len(xld.WALData))),
							time.Unix(0, int64(res[0].CommitTime)),
							time.Unix(0, int64(res[len(res)-1].CommitTime)),
							messageCounter,
						)
						lastMessageTime = time.Now()
					}
					p.metrics.ChangeItems.Add(int64(len(res)))
					for _, ci := range res {
						if ci.IsRowEvent() {
							p.metrics.Parsed.Inc()
						}
					}

					if err = p.sequencer.StartProcessing(res); err != nil {
						p.sendError(xerrors.Errorf("unable to start processing: %w", err))
						return
					}
					if err = p.parseQ.Add(res); err != nil {
						p.sendError(xerrors.Errorf("unable to add to pusher q: %w", err))
						return
					}
					cTime = time.Unix(0, int64(res[0].CommitTime))
				}(data)
				data = []*pglogrepl.XLogData{}
				bufferSize = 0
				messageCounter = 0
			} else {
				bufferSize += server.BytesSize(len(xld.WALData))
			}
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			p.mutex.Lock()
			copiedMaxLsn := p.maxLsn
			p.mutex.Unlock()

			if lastLsn != copiedMaxLsn {
				p.logger.Infof("Heartbeat %v at %v", copiedMaxLsn, cTime)
				lastLsn = copiedMaxLsn
			}
		}
	}
}

func (p *replication) parseWal2JsonChanges(cp *changeProcessor, xld *pglogrepl.XLogData) ([]abstract.ChangeItem, error) {
	st := time.Now()
	items, err := p.wal2jsonParser.Parse(xld.WALData)
	if err != nil {
		logger.Log.Error("Cannot parse wal2json message", log.Error(err), log.String("data", walDataSample(xld.WALData)))
		return nil, xerrors.Errorf("Cannot parse wal2json message: %w", err)
	}
	objIncleadable, err := abstract.BuildIncludeMap(p.objects.GetIncludeObjects())
	if err != nil {
		return nil, xerrors.Errorf("failed to filter table list extracted from source by objects set in transfer: %w", err)
	}
	if err := validateChangeItemsPtrs(items); err != nil {
		p.logger.Error(err.Error())
		//nolint:descriptiveerrors
		return nil, err
	}
	changes := make([]abstract.ChangeItem, 0, 1)
	for i, item := range items {
		changeItem := item.toChangeItem()
		if err := abstract.ValidateChangeItem(&changeItem); err != nil {
			logger.Log.Error(err.Error())
		}
		if !cp.hasSchemaForTable(changeItem.TableID()) {
			if p.skippedTables[changeItem.TableID()] {
				p.logger.Debug("skipping changes for a table added after replication had started", log.String("table", changeItem.TableID().String()))
				continue
			}
			if p.config.CollapseInheritTables {
				parentID, err := p.changeProcessor.resolveParentTable(p.sharedCtx, p.conn, changeItem.TableID())
				if err != nil {
					return nil, xerrors.Errorf("unable to resolve parent: %w", err)
				}
				if p.objects != nil && len(p.objects.IncludeObjects) > 0 && !objIncleadable[parentID] {
					p.skippedTables[changeItem.TableID()] = true // to prevent next time resolve for parent
					p.logger.Warn(
						"skipping changes for a table, since itself or its parent is not included in data-objects",
						log.String("table", changeItem.TableID().String()),
						log.String("parent_table", parentID.String()),
					)
					continue
				}
			}
			if err := p.reloadSchema(); err != nil {
				return nil, xerrors.Errorf("failed to reload schema: %w", abstract.NewFatalError(err))
			}
			if !cp.hasSchemaForTable(changeItem.TableID()) {
				if !p.config.IgnoreUnknownTables {
					return nil, xerrors.Errorf("failed to load schema for a table %s added after replication had started", changeItem.TableID().String())
				}
				p.logger.Warn("failed to get a schema for a table added after replication started, skipping changes for this table", log.String("table", changeItem.TableID().String()))
				p.skippedTables[changeItem.TableID()] = true
				continue
			}
		}
		if err := cp.fixupChange(&changeItem, item.ColumnTypeOIDs, item.OldKeys.KeyTypeOids, i, xld.WALStart); err != nil {
			//nolint:descriptiveerrors
			return nil, err
		}

		if err := abstract.ValidateChangeItem(&changeItem); err != nil {
			logger.Log.Error(err.Error())
		}
		changes = append(changes, changeItem)
	}
	if p.pgVersion.Is9x {
		p.lastKeeperTime = assignKeeperLag(items, p.config.SlotID, p.lastKeeperTime)
	}

	p.metrics.DecodeTime.RecordDuration(time.Since(st))
	return changes, nil
}

func NewReplicationPublisher(version PgVersion, replConn *mutexedPgConn, connPool *pgxpool.Pool, slot AbstractSlot, stats *stats.SourceStats, source *PgSource, transferID string, lgr log.Logger, cp coordinator.Coordinator, objects *server.DataObjects) (abstract.Source, error) {
	mutex := &sync.Mutex{}
	if tracker, ok := slot.(*LsnTrackedSlot); ok {
		// Start mole finder
		go func() {
			defer tracker.Close()
			tracker.Run()
		}()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &replication{
		logger:          lgr,
		conn:            connPool,
		replConn:        replConn,
		metrics:         stats,
		wal2jsonParser:  NewWal2JsonParser(),
		error:           make(chan error, 1),
		once:            sync.Once{},
		config:          source,
		transferID:      transferID,
		schema:          nil,
		altNames:        nil,
		wg:              sync.WaitGroup{},
		slotMonitor:     NewSlotMonitor(connPool, source.SlotID, source.Database, stats, lgr),
		stopCh:          make(chan struct{}),
		mutex:           mutex,
		maxLsn:          0,
		slot:            slot,
		pgVersion:       version,
		lastKeeperTime:  time.Now(),
		includeCache:    map[abstract.TableID]bool{},
		cp:              cp,
		changeProcessor: nil,
		sharedCtx:       ctx,
		sharedCtxCancel: cancel,
		objects:         objects,
		sequencer:       sequencer2.NewSequencer(),
		parseQ:          nil,
		objectsMap:      nil,

		skippedTables: make(map[abstract.TableID]bool),
	}, nil
}
