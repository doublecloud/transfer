package tests

import (
	"testing"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/pkg/providers/postgres"
	"github.com/doublecloud/transfer/pkg/providers/postgres/pgrecipe"
	"github.com/stretchr/testify/require"
)

func TestSlotHappyPath(t *testing.T) {
	src := pgrecipe.RecipeSource(pgrecipe.WithPrefix(""))

	connConfig, err := postgres.MakeConnConfigFromSrc(logger.Log, src)
	require.NoError(t, err)
	conn, err := postgres.NewPgConnPool(connConfig, logger.Log)
	require.NoError(t, err)

	slot, err := postgres.NewSlot(conn, logger.Log, src)
	require.NoError(t, err)
	require.NoError(t, slot.Create())

	exists, err := slot.Exist()
	require.NoError(t, err)
	require.True(t, exists)

	require.NoError(t, slot.Suicide())

	exists, err = slot.Exist()
	require.NoError(t, err)
	require.False(t, exists)
}

func TestSlotBrokenConnection(t *testing.T) {
	src := pgrecipe.RecipeSource(pgrecipe.WithPrefix(""))

	connConfig, err := postgres.MakeConnConfigFromSrc(logger.Log, src)
	require.NoError(t, err)
	conn, err := postgres.NewPgConnPool(connConfig, logger.Log)
	require.NoError(t, err)

	slot, err := postgres.NewSlot(conn, logger.Log, src)
	require.NoError(t, err)
	require.NoError(t, slot.Create())

	exists, err := slot.Exist()
	require.NoError(t, err)
	require.True(t, exists)

	// emulate problems with db.
	conn.Close()

	_, err = slot.Exist()
	require.Error(t, err)
}
