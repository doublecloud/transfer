package mongo

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	mongocommon "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/mongo"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/canon/validator"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/stretchr/testify/require"
)

func TestCanonSource(t *testing.T) {
	_ = os.Setenv("YC", "1") // to not go to vanga
	databaseName := "canondb"
	t.Run("vanilla hetero case", func(t *testing.T) {
		snapshotPlusIncrementScenario(t, databaseName, "hetero_repack", false, false)
	})
	t.Run("strange hetero case", func(t *testing.T) {
		snapshotPlusIncrementScenario(t, databaseName, "hetero_no_repack", false, true)
	})
}

func snapshotPlusIncrementScenario(t *testing.T, databaseName, collectionName string, isHomo, preventJSONRepack bool) {
	Source := &mongocommon.MongoSource{
		Hosts:    []string{"localhost"},
		Port:     helpers.GetIntFromEnv("MONGO_LOCAL_PORT"),
		User:     os.Getenv("MONGO_LOCAL_USER"),
		Password: server.SecretString(os.Getenv("MONGO_LOCAL_PASSWORD")),
		Collections: []mongocommon.MongoCollection{
			{DatabaseName: databaseName, CollectionName: collectionName},
		},
		IsHomo:            isHomo,
		PreventJSONRepack: preventJSONRepack,
	}
	Source.WithDefaults()
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mongo source", Port: Source.Port},
		))
	}()

	ctx := context.Background()
	require.NoError(t, DropCollection(ctx, Source, databaseName, collectionName))

	require.NoError(t, InsertDocs(ctx, Source, databaseName, collectionName, SnapshotDocuments...))
	if !preventJSONRepack {
		require.NoError(t, InsertDocs(ctx, Source, databaseName, collectionName, ExtraSnapshotDocuments...))
	}

	transfer := helpers.MakeTransfer(
		helpers.TransferID,
		Source,
		&server.MockDestination{
			SinkerFactory: validator.New(
				server.IsStrictSource(Source),
				validator.InitDone(t),
				validator.ValuesTypeChecker,
				validator.Referencer(t),
				validator.TypesystemChecker(mongocommon.ProviderType, func(colSchema abstract.ColSchema) string {
					return strings.TrimPrefix(colSchema.OriginalType, "mongo:")
				}),
			),
			Cleanup: server.Drop,
		},
		abstract.TransferTypeSnapshotAndIncrement,
	)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	time.Sleep(1 * time.Second)

	require.NoError(t, InsertDocs(ctx, Source, databaseName, collectionName, IncrementDocuments...))

	require.NoError(t, UpdateDocs(ctx, Source, databaseName, collectionName, IncrementUpdates...))

	time.Sleep(4 * mongocommon.DefaultBatchFlushInterval)
}
