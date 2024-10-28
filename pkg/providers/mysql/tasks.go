package mysql

import (
	"strings"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/middlewares"
	"github.com/doublecloud/transfer/pkg/sink"
)

func CheckMySQLBinlogRowImageFormat(source *MysqlSource) error {
	storage, err := NewStorage(source.ToStorageParams())
	if err != nil {
		return xerrors.Errorf("Cannot connect to the source database: %w", err)
	}
	defer func() { _ = storage.DB.Close() }()

	rows, err := storage.DB.Query("SELECT @@GLOBAL.binlog_row_image;")
	if err != nil {
		return xerrors.Errorf("Unable to check 'binlog_row_image': %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return xerrors.Errorf("Unable to check 'binlog_row_image': %w", err)
	}

	var binlogRowImageFormat string
	if err := rows.Scan(&binlogRowImageFormat); err != nil {
		return xerrors.Errorf("Unable to parse 'binlog_row_image': %w", err)
	}
	if strings.ToLower(binlogRowImageFormat) == "minimal" {
		return xerrors.Errorf("Unsupported 'binlog_row_image': '%v', preferred value is 'full'", binlogRowImageFormat)
	}

	return nil
}

func RemoveTracker(src *MysqlSource, id string, cp coordinator.Coordinator) error {
	tracker, err := NewTracker(src, id, cp)
	if err != nil {
		return err
	}

	storage, err := NewStorage(src.ToStorageParams())
	if err != nil {
		return xerrors.Errorf("failed to connect to the source database: %w", err)
	}
	defer func() { _ = storage.DB.Close() }()

	flavor, _ := CheckMySQLVersion(storage)
	enabled, err := IsGtidModeEnabled(storage, flavor)
	if err != nil {
		return xerrors.Errorf("Unable to check gtid mode: %w", err)
	}
	if enabled {
		gtid, _ := tracker.GetGtidset()
		logger.Log.Infof("Last gtid: %v", gtid)
		if err := tracker.RemoveGtidset(); err != nil {
			return xerrors.Errorf("unable to remove gtidset tracker: %w", err)
		}
	} else {
		file, pos, _ := tracker.Get()
		logger.Log.Infof("Last binlog position: %v:%v", file, pos)
		if err := tracker.Remove(); err != nil {
			return xerrors.Errorf("unable to remove binlog tracker: %w", err)
		}
	}
	return nil
}

func LoadMysqlSchema(transfer *model.Transfer, registry metrics.Registry, isAfter bool) error {
	mysqlSource, ok := transfer.Src.(*MysqlSource)
	if !ok {
		return nil
	}
	if transfer.SrcType() != transfer.DstType() {
		return nil
	}
	sink, err := sink.MakeAsyncSink(transfer, logger.Log, registry, coordinator.NewFakeClient(), middlewares.MakeConfig(middlewares.WithNoData))
	if err != nil {
		return xerrors.Errorf("unable to make sinker: %w", err)
	}
	defer sink.Close()

	var steps *MysqlDumpSteps
	if isAfter {
		steps = mysqlSource.PostSteps
	} else {
		steps = mysqlSource.PreSteps
	}

	if err = CopySchema(mysqlSource, steps, abstract.PusherFromAsyncSink(sink)); err != nil {
		return xerrors.Errorf("unable to copy mysql schema: %w", err)
	}
	return nil
}

// See TM-4581
func checkRestrictedColumnTypes(transfer *model.Transfer, tables abstract.TableMap) error {
	if transfer.SrcType() != ProviderType {
		return nil // Only MySQL sources affected by the decimal bug
	}
	if transfer.DstType() == ProviderType {
		return nil // Only heterogeneous transfers from MySQL are affected by the decimal bug
	}
	if transfer.Src.(*MysqlSource).AllowDecimalAsFloat {
		return nil
	}
	for tableID, table := range tables {
		for _, column := range table.Schema.Columns() {
			if strings.HasPrefix(strings.ToLower(column.OriginalType), "mysql:decimal") {
				return xerrors.Errorf("table %s contains column %q of type %s. Columns of decimal types currently are not supported. Please exclude the table from the transfer", tableID.Fqtn(), column.ColumnName, column.OriginalType)
			}
		}
	}
	return nil
}
