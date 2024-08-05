package dblog

import (
	"context"

	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
)

type MockSignalTable struct {
	CreateWatermarkF func(ctx context.Context, tableID abstract.TableID, watermarkType WatermarkType) error
	IsWatermarkF     func(item *abstract.ChangeItem, tableID abstract.TableID) (bool, WatermarkType)
}

func (m *MockSignalTable) CreateWatermark(ctx context.Context, tableID abstract.TableID, watermarkType WatermarkType) error {
	return m.CreateWatermarkF(ctx, tableID, watermarkType)
}

func (m *MockSignalTable) IsWatermark(item *abstract.ChangeItem, tableID abstract.TableID) (bool, WatermarkType) {
	return m.IsWatermarkF(item, tableID)
}

func NewMockSignalTable() *MockSignalTable {
	return &MockSignalTable{
		CreateWatermarkF: func(ctx context.Context, tableID abstract.TableID, watermarkType WatermarkType) error {
			return nil
		},
		IsWatermarkF: func(item *abstract.ChangeItem, tableID abstract.TableID) (bool, WatermarkType) {
			return true, LowWatermarkType
		},
	}
}
