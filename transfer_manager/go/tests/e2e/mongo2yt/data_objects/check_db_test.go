package snapshot

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	cpclient "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	mongodataagent "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/mongo"
	ytcommon "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yt"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/runtime/local"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/worker/tasks"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	yt_helpers "github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/yt"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yttest"
)

var (
	TransferType = abstract.TransferTypeIncrementOnly
	Source       = mongodataagent.MongoSource{
		Hosts:             []string{"localhost"},
		Port:              helpers.GetIntFromEnv("MONGO_LOCAL_PORT"),
		User:              os.Getenv("MONGO_LOCAL_USER"),
		Password:          server.SecretString(os.Getenv("MONGO_LOCAL_PASSWORD")),
		ReplicationSource: mongodataagent.MongoReplicationSourcePerDatabaseUpdateDocument,
	}
	Target = yt_helpers.RecipeYtTarget("//home/cdc/test/mongo2yt_e2e")
)

func init() {
	helpers.InitSrcDst(helpers.TransferID, &Source, Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestMain(m *testing.M) {
	ytcommon.InitExe()
	os.Exit(m.Run())
}

//---------------------------------------------------------------------------------------------------------------------
// utils

func LogMongoSource(s *mongodataagent.MongoSource) {
	fmt.Printf("Source.Hosts: %v\n", s.Hosts)
	fmt.Printf("Source.Port: %v\n", s.Port)
	fmt.Printf("Source.User: %v\n", s.User)
	fmt.Printf("Source.Password: %v\n", s.Password)
}

//---------------------------------------------------------------------------------------------------------------------

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mongo source", Port: Source.Port},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Ping", Ping)
		t.Run("Load", Load)
	})
}

func Ping(t *testing.T) {
	// ping src
	LogMongoSource(&Source)
	client, err := mongodataagent.Connect(context.Background(), Source.ConnectionOptions([]string{}), nil)
	defer func() { _ = client.Close(context.Background()) }()
	require.NoError(t, err)
	err = client.Ping(context.TODO(), nil)
	require.NoError(t, err)
}

type Trainer struct {
	Name string
	Age  int
	City string
}

func Load(t *testing.T) {
	client, err := mongodataagent.Connect(context.Background(), Source.ConnectionOptions([]string{}), nil)
	require.NoError(t, err)
	defer func() { _ = client.Close(context.Background()) }()

	//------------------------------------------------------------------------------------
	// insert one record

	db := client.Database("db")
	defer func() {
		// clear collection in the end (for local debug)
		_ = db.Collection("test_incl").Drop(context.Background())
		_ = db.Collection("test_excl").Drop(context.Background())
	}()

	err = db.CreateCollection(context.Background(), "test_incl")
	require.NoError(t, err)
	coll := db.Collection("test_incl")
	_, err = coll.InsertOne(context.Background(), Trainer{"a", 1, "aa"})
	require.NoError(t, err)

	err = db.CreateCollection(context.Background(), "test_excl")
	require.NoError(t, err)
	exclCol := db.Collection("test_excl")
	_, err = exclCol.InsertOne(context.Background(), Trainer{"a", 1, "aa"})
	require.NoError(t, err)
	//------------------------------------------------------------------------------------
	// start worker

	transfer := server.Transfer{
		Type: abstract.TransferTypeSnapshotAndIncrement,
		Src:  &Source,
		Dst:  Target,
		ID:   helpers.TransferID,
	}
	transfer.DataObjects = &server.DataObjects{IncludeObjects: []string{"db.test_incl"}}

	err = tasks.ActivateDelivery(context.TODO(), nil, cpclient.NewFakeClient(), transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(cpclient.NewFakeClient(), &transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	//------------------------------------------------------------------------------------
	// replicate one record

	_, err = coll.InsertOne(context.Background(), Trainer{"b", 2, "bb"})
	require.NoError(t, err)

	_, err = exclCol.InsertOne(context.Background(), Trainer{"b", 2, "bb"})
	require.NoError(t, err)

	//------------------------------------------------------------------------------------
	// check results

	require.NoError(t, helpers.WaitEqualRowsCount(
		t,
		"db",
		"test_incl",
		helpers.GetSampleableStorageByModel(t, Source),
		helpers.GetSampleableStorageByModel(t, Target.LegacyModel()),
		60*time.Second,
	))

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	exists, err := ytEnv.YT.NodeExists(context.Background(), ypath.Path(Target.Path()).Child("db_test_excl"), nil)
	require.NoError(t, err)
	require.False(t, exists)
}
