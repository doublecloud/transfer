package cascadedeletespertrans

import (
	"testing"

	test "github.com/doublecloud/transfer/transfer_manager/go/tests/e2e/mysql2mysql/cascade_deletes/common"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/stretchr/testify/require"
)

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: test.Source.Port},
			helpers.LabeledPort{Label: "Mysql target", Port: test.Target.Port},
		))
	}()

	test.Target.PerTransactionPush = true
	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Existence", test.Existence)
		t.Run("Snapshot", test.Snapshot)
		t.Run("Replication", test.Load)
	})
}
