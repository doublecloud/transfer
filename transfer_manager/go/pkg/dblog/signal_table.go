package dblog

import (
	"context"

	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
)

type WatermarkType string

const (
	LowWatermarkType  = "L"
	HighWatermarkType = "H"
)

// The SignalTable is used to create watermarks in the wal,
// and check whether the read item is a watermark
//
// It is assumed that the low_watermark will be placed before reading data from the table,
// and the high_watermark after reading data from the table
//
// When reading the wal, we have 2 types of watermarks (low and high):
//   - low - means that we are now just before the moment, when we start reading from the database
//   - high - means that we have completed reading
type SignalTable interface {
	CreateWatermark(ctx context.Context, tableID abstract.TableID, watermarkType WatermarkType) error
	IsWatermark(item *abstract.ChangeItem, tableID abstract.TableID) (bool, WatermarkType)
}
