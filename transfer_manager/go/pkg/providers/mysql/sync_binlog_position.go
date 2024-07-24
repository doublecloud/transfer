package mysql

import (
	"math"

	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/coordinator"
	"github.com/go-mysql-org/go-mysql/mysql"
)

func GetLogFilePosition(storage *Storage) (string, uint32, string, error) {
	var file string
	var position uint64
	var doDB string
	var ignoreDB string
	var gtid string
	if err := storage.DB.QueryRow("show master status;").Scan(&file, &position, &doDB, &ignoreDB, &gtid); err != nil {
		if err = storage.DB.QueryRow("show master status;").Scan(&file, &position, &doDB, &ignoreDB); err != nil {
			return "", 0, "", xerrors.Errorf("Failed to get master status: %w", err)
		}

		if err = storage.DB.QueryRow("SELECT BINLOG_GTID_POS(?, ?);", file, position).Scan(&gtid); err != nil {
			return "", 0, "", xerrors.Errorf("Failed to get gtid: %w", err)
		}
	}

	if position > math.MaxUint32 {
		return "", 0, "", xerrors.Errorf("Last binlog file %v:%v is more than 4GB", file, position)
	}

	smallPosition := uint32(position)
	logger.Log.Infof("Last binlog position: %v:%v", file, smallPosition)
	return file, smallPosition, gtid, nil
}

func SyncBinlogPosition(src *MysqlSource, id string, cp coordinator.Coordinator) error {
	storage, err := NewStorage(src.ToStorageParams())
	if err != nil {
		return xerrors.Errorf("failed to connect to the source database: %w", err)
	}
	defer func() { _ = storage.DB.Close() }()

	tracker, err := NewTracker(src, id, cp)
	if err != nil {
		return xerrors.Errorf("failed to create MySQL binlog tracker: %w", err)
	}

	logger.Log.Infof("sync binlog into %T", tracker)
	file, pos, gtid, err := GetLogFilePosition(storage)
	if err != nil {
		return xerrors.Errorf("failed to get log file position: %w", err)
	}

	flavor := CheckMySQLVersion(storage)
	gtidModeEnabled, err := IsGtidModeEnabled(storage, flavor)
	if err != nil {
		return xerrors.Errorf("Unable to check gtid mode: %w", err)
	}

	if gtidModeEnabled {
		logger.Log.Infof("GTID mode is ON")

		gtidSet, err := mysql.ParseGTIDSet(flavor, gtid)
		if err != nil {
			return xerrors.Errorf("failed to parse gtidset: %w", err)
		}
		logger.Log.Infof("Store GTID: %v", gtidSet.String())
		if err := tracker.StoreGtidset(gtidSet); err != nil {
			return xerrors.Errorf("failed to save GTIDSet in the tracker: %w", err)
		}
	} else {
		if err := tracker.Store(file, pos); err != nil {
			return xerrors.Errorf("failed to save binlog position in the tracker: %w", err)
		}
	}

	return nil
}
