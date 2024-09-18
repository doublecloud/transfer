package permissions

import (
	"testing"

	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/providers/mysql"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
)

func prepareSource() *mysql.MysqlSource {
	source := helpers.RecipeMysqlSource()
	source.User = "test_user"
	source.Password = "test_pass"
	return source
}

func TestTableListError(t *testing.T) {
	source := prepareSource()

	storage, err := mysql.NewStorage(source.ToStorageParams())
	require.NoError(t, err)

	_, err = storage.TableList(nil)
	require.Error(t, err)
}

func TestTableListNoError(t *testing.T) {
	source := helpers.WithMysqlInclude(prepareSource(), []string{"foo"})

	storage, err := mysql.NewStorage(source.ToStorageParams())
	require.NoError(t, err)

	tables, err := storage.TableList(nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(tables))
	_, containsFoo := tables[abstract.TableID{Namespace: "source", Name: "foo"}]
	require.True(t, containsFoo)
}
