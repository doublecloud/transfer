package replication

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/pkg/abstract"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	pgsink "github.com/doublecloud/transfer/pkg/providers/postgres"
	"github.com/doublecloud/transfer/pkg/providers/postgres/pgrecipe"
	"github.com/doublecloud/transfer/pkg/worker/tasks"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump"), pgrecipe.WithPrefix(""))
	dstPort, _   = strconv.Atoi(os.Getenv("DB0_PG_LOCAL_PORT"))
	Target       = *pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))
)

func init() {
	_ = os.Setenv("YC", "1")                                               // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, transferID
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Existence", Existence)
		t.Run("Verify", Verify)
		t.Run("Load", Load)
	})
}

func Existence(t *testing.T) {
	_, err := pgsink.NewStorage(Source.ToStorageParams(nil))
	require.NoError(t, err)
	_, err = pgsink.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)
}

func Verify(t *testing.T) {
	var transfer server.Transfer
	transfer.Src = &Source
	transfer.Dst = &Target
	transfer.Type = "INCREMENT_ONLY"

	err := tasks.VerifyDelivery(transfer, logger.Log, helpers.EmptyRegistry())
	require.NoError(t, err)

	dstStorage, err := pgsink.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)

	var result bool
	err = dstStorage.Conn.QueryRow(context.Background(), `
		SELECT EXISTS
        (
            SELECT 1
            FROM pg_tables
            WHERE schemaname = 'public'
            AND tablename = '_ping'
        );
	`).Scan(&result)
	require.NoError(t, err)
	require.Equal(t, false, result)
}

func Load(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "__test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 120*time.Second))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))

	//-----------------------------------------------------------------------------------------------------------------

	sink, err := pgsink.NewSink(logger.Log, helpers.TransferID, Source.ToSinkParams(), helpers.EmptyRegistry())
	require.NoError(t, err)

	arrColSchema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "aid", DataType: ytschema.TypeUint8.String(), PrimaryKey: true},
		{ColumnName: "str", DataType: ytschema.TypeString.String(), PrimaryKey: true},
		{ColumnName: "id", DataType: ytschema.TypeUint8.String(), PrimaryKey: true},
		{ColumnName: "jb", DataType: ytschema.TypeAny.String(), PrimaryKey: false},
	})
	changeItemBuilder := helpers.NewChangeItemsBuilder("public", "__test", arrColSchema)

	require.NoError(t, sink.Push(changeItemBuilder.Inserts(t, []map[string]interface{}{{"aid": 11, "str": "a", "id": 11, "jb": "{}"}, {"aid": 22, "str": "b", "id": 22, "jb": `{"x": 1, "y": -2}`}, {"aid": 33, "str": "c", "id": 33}})))
	require.NoError(t, sink.Push(changeItemBuilder.Updates(t, []map[string]interface{}{{"aid": 33, "str": "c", "id": 34, "jb": `{"test": "test"}`}}, []map[string]interface{}{{"aid": 33, "str": "c", "id": 33}})))
	require.NoError(t, sink.Push(changeItemBuilder.Deletes(t, []map[string]interface{}{{"aid": 22, "str": "b", "id": 22}})))
	require.NoError(t, sink.Push(changeItemBuilder.Deletes(t, []map[string]interface{}{{"aid": 33, "str": "c", "id": 34}})))

	//-----------------------------------------------------------------------------------------------------------------

	helpers.CheckRowsCount(t, Source, "public", "__test", 14)
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "__test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 120*time.Second))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}
