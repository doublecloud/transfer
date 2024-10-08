package canon

import (
	"testing"

	"github.com/doublecloud/transfer/pkg/providers/clickhouse"
	"github.com/doublecloud/transfer/pkg/providers/mongo"
	"github.com/doublecloud/transfer/pkg/providers/mysql"
	"github.com/doublecloud/transfer/pkg/providers/postgres"
	"github.com/doublecloud/transfer/pkg/providers/ydb"
	"github.com/doublecloud/transfer/pkg/providers/yt"
	"github.com/doublecloud/transfer/tests/canon/validator"
	"github.com/stretchr/testify/require"
)

func TestAll(t *testing.T) {
	cases := All(
		ydb.ProviderType,
		yt.ProviderType,
		mongo.ProviderType,
		clickhouse.ProviderType,
		mysql.ProviderType,
		postgres.ProviderType,
	)
	for _, tc := range cases {
		t.Run(tc.String(), func(t *testing.T) {
			require.NotEmpty(t, tc.Data)
			snkr := validator.Referencer(t)()
			require.NoError(t, snkr.Push(tc.Data))
			require.NoError(t, snkr.Close())
		})
	}
}
