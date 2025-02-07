package iceberg

import (
	"context"
	"go.ytsaurus.tech/library/go/core/log"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/pkg/abstract"
)

func TestStorage(t *testing.T) {
	src, err := SourceRecipe()
	if err != nil {
		t.Skip("No recipe defined")
	}
	logger.Log.Info("recipe", log.Any("src", src))
	storage, err := NewStorage(src, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)
	tables, err := storage.TableList(nil)
	require.NoError(t, err)
	require.Len(t, tables, 19)
	for tid := range tables {
		t.Run(tid.String(), func(t *testing.T) {
			_, err := storage.EstimateTableRowsCount(tid)
			require.NoError(t, err)
			require.NoError(t, storage.LoadTable(context.Background(), abstract.TableDescription{
				Name:   tid.Name,
				Schema: tid.Namespace,
				Filter: "",
				EtaRow: 0,
				Offset: 0,
			}, func(items []abstract.ChangeItem) error {
				abstract.Dump(items)
				return nil
			}))
		})
	}
}
