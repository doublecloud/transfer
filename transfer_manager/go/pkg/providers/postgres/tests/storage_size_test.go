package tests

import (
	"context"
	"testing"

	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/postgres"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/postgres/pgrecipe"
	"github.com/stretchr/testify/require"
)

func TestInheritTableStorageSize(t *testing.T) {
	src := pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("test_scripts"))
	src.CollapseInheritTables = true
	storage, err := postgres.NewStorage(src.ToStorageParams(nil))
	require.NoError(t, err)
	err = storage.BeginPGSnapshot(context.TODO())
	require.NoError(t, err)
	logger.Log.Infof("create snapshot: %v, ts: %v", storage.ShardedStateLSN, storage.ShardedStateTS)
	tid := abstract.TableID{Name: "__test_parent", Namespace: "public"}
	size, err := storage.TableSizeInBytes(tid)
	require.NoError(t, err)
	require.Equal(t, 15319040, int(size))
}
