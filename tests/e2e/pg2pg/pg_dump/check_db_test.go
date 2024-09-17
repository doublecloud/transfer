package pgdump

import (
	"os"
	"strings"
	"testing"

	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/providers/postgres"
	"github.com/doublecloud/transfer/pkg/providers/postgres/pgrecipe"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
)

var (
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump"), pgrecipe.WithPrefix(""))
	Target       = *pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))
)

func init() {
	_ = os.Setenv("YC", "1")                                               // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	require.True(t, t.Run("Existence", Existence))
	require.True(t, t.Run("Snapshot", Snapshot))
}

func Existence(t *testing.T) {
	_, err := postgres.NewStorage(Source.ToStorageParams(nil))
	require.NoError(t, err)
	_, err = postgres.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)
}

func Snapshot(t *testing.T) {
	Source.PreSteps.Constraint = true
	Source.PreSteps.Trigger = true
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotOnly)

	// extract schema
	itemsSource, err := postgres.ExtractPgDumpSchema(transfer)
	require.NoError(t, err)

	// apply on target
	require.NoError(t, postgres.ApplyPgDumpPreSteps(itemsSource, transfer, helpers.EmptyRegistry()))

	// make target a source and extract its schema
	targetAsSource := postgres.PgSource{
		ClusterID:     Target.ClusterID,
		Hosts:         Target.Hosts,
		User:          Target.User,
		Password:      Target.Password,
		Database:      Target.Database,
		Port:          Target.Port,
		PgDumpCommand: Source.PgDumpCommand,
	}
	targetAsSource.WithDefaults()
	backwardFakeTransfer := helpers.MakeTransfer(helpers.TransferID, &targetAsSource, &Target, abstract.TransferTypeSnapshotOnly)
	itemsTarget, err := postgres.ExtractPgDumpSchema(backwardFakeTransfer)
	require.NoError(t, err)

	// compare schemas
	require.Less(t, 0, len(itemsSource))
	require.Equal(t, len(itemsSource), len(itemsTarget))
	setvalsCount := 0
	for i := 0; i < len(itemsSource); i++ {
		require.Equal(t, itemsSource[i].Typ, itemsTarget[i].Typ)
		require.Equal(t, itemsSource[i].Body, itemsTarget[i].Body)
		if strings.Contains(itemsSource[i].Body, "setval(") {
			setvalsCount += 1
		}
	}
	require.Equal(t, 2, setvalsCount, "The number of setval() calls for SEQUENCEs must be equal to the number of sequences in dump")

	// test extract dump with DBTables
	// with custom types
	Source.DBTables = []string{"santa.\"Ho-Ho-Ho\""}
	transfer = helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotOnly)

	itemsSource, err = postgres.ExtractPgDumpSchema(transfer)
	require.NoError(t, err)

	existEnumType := false
	for _, i := range itemsSource {
		if i.Typ == "TYPE" && i.Name == "my_enum" {
			existEnumType = true
			break
		}
	}
	require.True(t, existEnumType)

	// without custom types
	Source.DBTables = []string{"__test"}
	transfer = helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotOnly)

	itemsSource, err = postgres.ExtractPgDumpSchema(transfer)
	require.NoError(t, err)

	existEnumType = false
	for _, i := range itemsSource {
		if i.Typ == "TYPE" {
			existEnumType = true
			break
		}
	}
	require.False(t, existEnumType)
}
