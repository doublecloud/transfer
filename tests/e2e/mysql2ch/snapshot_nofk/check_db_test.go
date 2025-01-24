package snapshotnofk

import (
	"testing"

	"github.com/doublecloud/transfer/pkg/abstract"
	chrecipe "github.com/doublecloud/transfer/pkg/providers/clickhouse/recipe"
	"github.com/doublecloud/transfer/tests/e2e/mysql2ch"
	"github.com/doublecloud/transfer/tests/e2e/pg2ch"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
)

func TestSnapshot(t *testing.T) {
	source := helpers.RecipeMysqlSource()
	target := chrecipe.MustTarget(chrecipe.WithInitFile("ch.sql"), chrecipe.WithDatabase("source"))

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "MySQL source", Port: source.Port},
			helpers.LabeledPort{Label: "CH target", Port: target.NativePort},
		))
	}()

	t.Run("fake_keys", func(t *testing.T) {
		source.UseFakePrimaryKey = true
		transfer := helpers.MakeTransfer(helpers.TransferID, source, target, abstract.TransferTypeSnapshotAndIncrement)
		_, err := helpers.ActivateErr(transfer)
		require.NoError(t, err)
		require.NoError(t, helpers.CompareStorages(
			t,
			source,
			target,
			helpers.NewCompareStorageParams().WithEqualDataTypes(pg2ch.PG2CHDataTypesComparator).WithPriorityComparators(mysql2ch.MySQLBytesToStringOptionalComparator),
		))
	})
	t.Run("no_fake_keys", func(t *testing.T) {
		source.UseFakePrimaryKey = false
		transfer := helpers.MakeTransfer(helpers.TransferID, source, target, abstract.TransferTypeSnapshotAndIncrement)
		_, err := helpers.ActivateErr(transfer)
		require.Error(t, err)
	})
}
