package tests

import (
	_ "embed"
	"testing"

	"github.com/doublecloud/transfer/transfer_manager/go/cmd/trcli/check"
	"github.com/doublecloud/transfer/transfer_manager/go/cmd/trcli/config"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres/pgrecipe"
	"github.com/stretchr/testify/require"
)

//go:embed transfer.yaml
var transferYaml []byte

func TestCheck(t *testing.T) {
	src := pgrecipe.RecipeSource(
		pgrecipe.WithPrefix(""),
		pgrecipe.WithFiles("dump/pg_init.sql"),
	)

	dst := pgrecipe.RecipeTarget(
		pgrecipe.WithPrefix(""),
	)
	dst.MaintainTables = true // forces table creation on push

	transfer, err := config.ParseTransfer(transferYaml)
	require.NoError(t, err)

	transfer.Src = src
	transfer.Dst = dst

	require.NoError(t, check.RunCheck(transfer))
}
