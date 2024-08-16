package logtracker

import (
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/oracle/common"
)

type LogTracker interface {
	TransferID() string
	Init() error
	ClearPosition() error
	ReadPosition() (*common.LogPosition, error)
	WritePosition(position *common.LogPosition) error
}
