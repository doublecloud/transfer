package tasks

import (
	"context"
	"testing"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/providers/postgres"
	"github.com/stretchr/testify/require"
)

func TestMergeWithIncrementalState(t *testing.T) {
	transfer := &model.Transfer{
		ID:   "transfer1",
		Type: abstract.TransferTypeSnapshotOnly,
		RegularSnapshot: &abstract.RegularSnapshot{
			Incremental: []abstract.IncrementalTable{
				{Namespace: "public", Name: "table1", CursorField: "field1", InitialState: "100500"},
				{Namespace: "public", Name: "table2", CursorField: "field1", InitialState: "100500"}, // new table
			},
		}}
	client := coordinator.NewFakeClientWithOpts(func(id string) (map[string]*coordinator.TransferStateData, error) {
		result := map[string]*coordinator.TransferStateData{}
		if id == "transfer1" {
			result[TablesFilterStateKey] = &coordinator.TransferStateData{
				IncrementalTables: []abstract.TableDescription{
					{Name: "table1", Schema: "public", Filter: "\"field1\" > 200500"},
				},
			}
		}
		return result, nil
	})
	tables := []abstract.TableDescription{
		{Name: "table1", Schema: "public"},
		{Name: "table2", Schema: "public"},
	}
	incrementalStorage := newFakeIncrementalStorage()
	snapshotLoader := NewSnapshotLoader(client, "test-operation", transfer, solomon.NewRegistry(nil))
	err := snapshotLoader.mergeWithIncrementalState(tables, incrementalStorage)
	require.NoError(t, err)
	require.Equal(t, []abstract.TableDescription{
		{Name: "table1", Schema: "public", Filter: "\"field1\" > 200500"},
		{Name: "table2", Schema: "public", Filter: "\"field1\" > 100500"},
	}, tables)
}

type fakeIncrementalStorage struct {
}

func newFakeIncrementalStorage() *fakeIncrementalStorage {
	return &fakeIncrementalStorage{}
}

func (f *fakeIncrementalStorage) GetIncrementalState(ctx context.Context, incremental []abstract.IncrementalTable) ([]abstract.TableDescription, error) {
	//TODO implement me
	panic("implement me")
}

func (f *fakeIncrementalStorage) SetInitialState(tables []abstract.TableDescription, incremental []abstract.IncrementalTable) {
	postgres.SetInitialState(tables, incremental)
}
