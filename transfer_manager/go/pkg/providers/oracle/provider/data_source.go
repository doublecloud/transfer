package provider

import (
	"time"

	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/library/go/core/metrics"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/coordinator"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/base"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/oracle"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/oracle/common"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/oracle/logtracker"
	logminer "github.com/doublecloud/tross/transfer_manager/go/pkg/providers/oracle/replication/log_miner"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/oracle/schema"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/oracle/snapshot"
	"github.com/jmoiron/sqlx"
)

type OracleDataProvider struct {
	sqlxDB                  *sqlx.DB
	snaphotSplitTransaction *sqlx.Tx
	logger                  log.Logger
	registry                metrics.Registry
	config                  oracle.OracleSource
	databaseSchema          *schema.Database
	tracker                 logtracker.LogTracker
}

// To verify providers contract implementation
var (
	_ base.SnapshotProvider = (*OracleDataProvider)(nil)
)

func NewOracleDataProvider(
	logger log.Logger,
	registry metrics.Registry,
	controlPlaneClient coordinator.Coordinator,
	config *oracle.OracleSource,
	transferID string,
) (*OracleDataProvider, error) {
	sqlxDB, err := common.CreateConnection(config)
	if err != nil {
		return nil, xerrors.Errorf("Can't create connection: %w", err)
	}

	schemaRepo, err := schema.NewDatabase(sqlxDB, config, logger)
	if err != nil {
		return nil, xerrors.Errorf("Can't create schema repository: %w", err)
	}

	tracker, err := createLogTracker(sqlxDB, controlPlaneClient, config, transferID)
	if err != nil {
		return nil, xerrors.Errorf("Can't create log tracker: %w", err)
	}

	dataSource := &OracleDataProvider{
		sqlxDB:                  sqlxDB,
		snaphotSplitTransaction: nil,
		logger:                  logger,
		registry:                registry,
		config:                  *config,
		databaseSchema:          schemaRepo,
		tracker:                 tracker,
	}

	return dataSource, nil
}

func createLogTracker(
	sqlxDB *sqlx.DB,
	controlPlaneClient coordinator.Coordinator,
	config *oracle.OracleSource,
	transferID string,
) (logtracker.LogTracker, error) {
	switch config.TrackerType {
	case oracle.OracleNoLogTracker:
		return nil, nil
	case oracle.OracleInMemoryLogTracker:
		inMemoryLogTracker, err := logtracker.NewInMemoryLogTracker(transferID)
		if err != nil {
			return nil, xerrors.Errorf("Can't create in memory log tracker: %w", err)
		}
		return inMemoryLogTracker, nil
	case oracle.OracleEmbeddedLogTracker:
		embeddedLogTracker, err := logtracker.NewEmbeddedLogTracker(sqlxDB, config, transferID)
		if err != nil {
			return nil, xerrors.Errorf("Can't create embedded log tracker: %w", err)
		}
		return embeddedLogTracker, nil
	case oracle.OracleInternalLogTracker:
		internalLogTracker, err := logtracker.NewInternalLogTracker(controlPlaneClient, transferID)
		if err != nil {
			return nil, xerrors.Errorf("Can't create embedded log tracker: %w", err)
		}
		return internalLogTracker, nil
	default:
		return nil, xerrors.Errorf("Unknown tracker type '%v'", config.TrackerType)
	}
}

func (data *OracleDataProvider) DatabaseSchema() (*schema.Database, error) {
	return data.databaseSchema, nil
}

func (data *OracleDataProvider) Tracker() (logtracker.LogTracker, error) {
	return data.tracker, nil
}

func (data *OracleDataProvider) getCurrentLogPosition(transferType abstract.TransferType) (*common.LogPosition, error) {
	sql := "SELECT CURRENT_SCN, sys_extract_utc(systimestamp) as CURRENT_TIMESTAMP FROM v$database"
	var sqlResult struct {
		SCN              uint64    `db:"CURRENT_SCN"`
		CurrentTimestamp time.Time `db:"CURRENT_TIMESTAMP"`
	}
	if err := data.sqlxDB.Get(&sqlResult, sql); err != nil {
		return nil, xerrors.Errorf("Can't select current SCN from DB: %w", err)
	}

	var positionType common.PositionType
	switch transferType {
	case abstract.TransferTypeSnapshotOnly, abstract.TransferTypeSnapshotAndIncrement:
		positionType = common.PositionSnapshotStarted
	case abstract.TransferTypeIncrementOnly:
		positionType = common.PositionReplication
	default:
		return nil, xerrors.Errorf("Transfer type '%v' is not supported", transferType)
	}

	return common.NewLogPosition(sqlResult.SCN, nil, nil, positionType, sqlResult.CurrentTimestamp)
}

// Begin of base DataProvider interface

func (data *OracleDataProvider) Init() error {
	if err := data.databaseSchema.LoadMetadata(); err != nil {
		return xerrors.Errorf("Can't init data source, can't load metadata: %w", err)
	}

	if err := data.databaseSchema.LoadTablesFromConfig(); err != nil {
		return xerrors.Errorf("Can't init data source, can't load schema: %w", err)
	}

	if data.tracker != nil {
		if err := data.tracker.Init(); err != nil {
			return xerrors.Errorf("Can't init data source, can't init tracker: %w", err)
		}
	}

	return nil
}

func (data *OracleDataProvider) Ping() error {
	if err := data.sqlxDB.Ping(); err != nil {
		return xerrors.Errorf("Can't ping DB: %w", err)
	}
	return nil
}

func (data *OracleDataProvider) Close() error {
	if err := data.sqlxDB.Close(); err != nil {
		return xerrors.Errorf("Can't close DB: %w", err)
	}
	return nil
}

// End of base DataProvider interface

// Begin of base SnapshotProvider interface

func (data *OracleDataProvider) BeginSnapshot() error {
	if !data.config.UseParallelTableLoad {
		return nil
	}

	if data.snaphotSplitTransaction != nil {
		return xerrors.New("Snapshot already started")
	}

	transaction, err := data.sqlxDB.Beginx()
	if err != nil {
		return xerrors.Errorf("Can't create snapshot split transaction: %w", err)
	}

	data.snaphotSplitTransaction = transaction
	return nil
}

func (data *OracleDataProvider) DataObjects(filter base.DataObjectFilter) (base.DataObjects, error) {
	return snapshot.NewOracleDataObjects(data.databaseSchema, filter), nil
}

func (data *OracleDataProvider) TableSchema(part base.DataObjectPart) (*abstract.TableSchema, error) {
	oracleDataPart, ok := part.(*snapshot.OracleDataObject)
	if !ok {
		return nil, xerrors.New("Part must be Oracle data part")
	}

	return oracleDataPart.Table().ToOldTable()
}

func (data *OracleDataProvider) DataObjectsToTableParts(filter base.DataObjectFilter) ([]abstract.TableDescription, error) {
	objects, err := data.DataObjects(filter)
	if err != nil {
		return nil, xerrors.Errorf("Can't get data objects: %w", err)
	}

	tableDescriptions, err := base.DataObjectsToTableParts(objects, filter)
	if err != nil {
		return nil, xerrors.Errorf("Can't convert data objects to table descriptions: %w", err)
	}

	return tableDescriptions, nil
}

func (data *OracleDataProvider) CreateSnapshotSource(part base.DataObjectPart) (base.ProgressableEventSource, error) {
	oracleDataPart, ok := part.(*snapshot.OracleDataObject)
	if !ok {
		return nil, xerrors.New("Part must be Oracle data part")
	}

	var position *common.LogPosition
	if data.tracker != nil {
		var err error
		position, err = data.tracker.ReadPosition()
		if err != nil {
			return nil, xerrors.Errorf("Can't read current SCN: %w", err)
		}
	} else {
		var err error
		position, err = common.NewLogPosition(0, nil, nil, common.PositionSnapshotStarted, time.Now())
		if err != nil {
			return nil, xerrors.Errorf("cannot make current log position: %w", err)
		}
	}

	// Parallel table source
	if data.config.UseParallelTableLoad {
		tableSource, err := snapshot.NewParallelTableSource(
			data.sqlxDB, data.snaphotSplitTransaction, &data.config, position, oracleDataPart.Table(), data.logger)
		if err != nil {
			return nil, xerrors.Errorf("Can't create parallel table source: %w", err)
		}
		return tableSource, nil
	}

	// Default table source
	tableSource, err := snapshot.NewTableSource(
		data.sqlxDB, &data.config, position, oracleDataPart.Table(), data.logger)
	if err != nil {
		return nil, xerrors.Errorf("Can't create table source: %w", err)
	}
	return tableSource, nil
}

func (data *OracleDataProvider) EndSnapshot() error {
	if !data.config.UseParallelTableLoad {
		return nil
	}

	if data.snaphotSplitTransaction == nil {
		return xerrors.New("Snapshot not started")
	}

	if err := data.snaphotSplitTransaction.Rollback(); err != nil {
		return xerrors.Errorf("Can't rollback snapshot split transaction: %w", err)
	}

	return nil
}

func (data *OracleDataProvider) ResolveOldTableDescriptionToDataPart(tableDesc abstract.TableDescription) (base.DataObjectPart, error) {
	db, err := data.DatabaseSchema()
	if err != nil {
		return nil, xerrors.Errorf("Cannot get database schema: %w", err)
	}

	schema := db.OracleSchemaByName(tableDesc.Schema)
	if schema == nil {
		return nil, xerrors.Errorf("Cannot find schema '%v'", tableDesc.Schema)
	}

	table := schema.OracleTableByName(tableDesc.Name)
	if table == nil {
		return nil, xerrors.Errorf("Cannot find table '%v.%v'", tableDesc.Schema, tableDesc.Name)
	}

	return snapshot.NewOracleDataObject(table), nil
}

func (data *OracleDataProvider) TablePartToDataObjectPart(tableDescription *abstract.TableDescription) (base.DataObjectPart, error) {
	if tableDescription == nil {
		return nil, nil
	}
	return data.ResolveOldTableDescriptionToDataPart(*tableDescription)
}

// End of base SnapshotProvider interface

// Begin of base ReplicationProvider interface

func (data *OracleDataProvider) CreateReplicationSource() (base.EventSource, error) {
	logMinerSource, err := logminer.NewLogMinerSource(data.sqlxDB, &data.config, data.databaseSchema, data.tracker, data.logger, data.registry)
	if err != nil {
		return nil, xerrors.Errorf("Can't create replication source: %w", err)
	}
	return logMinerSource, nil
}

// End of base ReplicationProvider interface

// Begin of base TrackerProvider interface

func (data *OracleDataProvider) ResetTracker(typ abstract.TransferType) error {
	if data.tracker != nil {
		current, err := data.getCurrentLogPosition(typ)
		if err != nil {
			return xerrors.Errorf("Can't read current SCN: %w", err)
		}
		if err := data.tracker.WritePosition(current); err != nil {
			return xerrors.Errorf("Can't write current SCN to tracker: %w", err)
		}
	}
	return nil
}

// End of base TrackerProvider interface
