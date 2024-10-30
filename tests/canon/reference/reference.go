package reference

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/library/go/test/canon"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/abstract/typesystem"
	"github.com/doublecloud/transfer/pkg/middlewares"
	"github.com/doublecloud/transfer/pkg/sink"
	"github.com/doublecloud/transfer/pkg/worker/tasks"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
)

func constructSinkCleanupAndPush(t *testing.T, transfer *model.Transfer, items []abstract.ChangeItem, tables abstract.TableMap) {
	as, err := sink.MakeAsyncSink(transfer, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), coordinator.NewFakeClient(), middlewares.MakeConfig(middlewares.WithNoData))
	require.NoError(t, err)
	defer func() { require.NoError(t, as.Close()) }()

	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), "reference-test-cleanup-operation", transfer, helpers.EmptyRegistry())
	require.NoError(t, snapshotLoader.CleanupSinker(tables))

	errCh := as.AsyncPush(items)
	require.NoError(t, <-errCh)
}

// ReferenceTestFn returns a function which conducts a reference test with the given items and transfer for all transfer typesystem versions.
//
// Reference test is a canonization test which records the final state of the target database (sink) after a precanonized set of "reference" items has been pushed into the target.
// The final state of the target database is obtained as if it was a snapshot source; that is why a sink-as-source object is required.
//
// This method conducts a cleanup of the target database automatically before each test. Note that transfer's target endpoint should specify cleanup policy DROP or TRUNCATE for this feature to work.
func ReferenceTestFn(transfer *model.Transfer, sinkAsSource model.Source, items []abstract.ChangeItem) func(*testing.T) {
	tables := make(abstract.TableMap)
	for _, item := range items {
		if _, ok := tables[item.TableID()]; ok {
			continue
		}
		tables[item.TableID()] = abstract.TableInfo{
			EtaRow: 1,
			IsView: false,
			Schema: item.TableSchema,
		}
	}

	return func(t *testing.T) {
		for v := 1; v <= typesystem.LatestVersion; v++ {
			transfer.TypeSystemVersion = v
			t.Run(fmt.Sprintf("typesystem_%02d", v), func(t *testing.T) {
				constructSinkCleanupAndPush(t, transfer, items, tables)
				result := dumpToString(t, sinkAsSource)
				marshalledResult, err := json.Marshal(result)
				require.NoError(t, err)

				cwd, err := os.Getwd()
				require.NoError(t, err)
				fileForResult := filepath.Join(cwd, "result.txt")
				require.NoError(t, ioutil.WriteFile(fileForResult, marshalledResult, 0o666))

				canon.SaveFile(t, fileForResult)
			})
		}
	}
}
