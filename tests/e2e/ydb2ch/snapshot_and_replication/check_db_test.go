package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/providers/clickhouse/model"
	"github.com/doublecloud/transfer/pkg/providers/ydb"
	"github.com/doublecloud/transfer/pkg/xtls"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
	ydb3 "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"go.ytsaurus.tech/yt/go/schema"
)

func NewYDBConnection(cfg *ydb.YdbDestination) (*ydb3.Driver, error) {
	var err error
	var tlsConfig *tls.Config
	if cfg.TLSEnabled {
		tlsConfig, err = xtls.FromPath(cfg.RootCAFiles)
		if err != nil {
			return nil, xerrors.Errorf("could not create TLS config: %w", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var creds credentials.Credentials
	creds, err = ydb.ResolveCredentials(
		cfg.UserdataAuth,
		string(cfg.Token),
		ydb.JWTAuthParams{
			KeyContent:      cfg.SAKeyContent,
			TokenServiceURL: cfg.TokenServiceURL,
		},
		cfg.ServiceAccountID,
		logger.Log,
	)
	if err != nil {
		return nil, xerrors.Errorf("cannot create YDB credentials: %w", err)
	}

	ydbDriver, err := ydb.NewYDBDriver(ctx, cfg.Database, cfg.Instance, creds, tlsConfig)
	if err != nil {
		return nil, xerrors.Errorf("unable to init ydb driver: %w", err)
	}

	return ydbDriver, nil
}

func customYDBInsertItem(t *testing.T, tablePath string, id int) *abstract.ChangeItem {
	res := helpers.YDBStmtInsert(t, tablePath, id)
	res.TableSchema = abstract.NewTableSchema(append(res.TableSchema.Columns(),
		abstract.ColSchema{PrimaryKey: false, Required: false, ColumnName: "brand_new_text_column", DataType: string(schema.TypeString), OriginalType: "ydb:Utf8"},
	))
	res.ColumnNames = append(res.ColumnNames, "brand_new_text_column")
	res.ColumnValues = append(res.ColumnValues, "POOOWEEEER")
	return res
}

func TestSnapshotAndReplication(t *testing.T) {
	for testName, changeFeedMode := range map[string]ydb.ChangeFeedModeType{
		"ModeUpdate":      ydb.ChangeFeedModeUpdates,
		"ModeNewImage":    ydb.ChangeFeedModeNewImage,
		"ModeOldNewImage": ydb.ChangeFeedModeNewAndOldImages,
	} {
		t.Run(testName, func(t *testing.T) {
			testSnapshotAndReplicationWithChangeFeedMode(t, testName, changeFeedMode)
		})
	}
}

func testSnapshotAndReplicationWithChangeFeedMode(t *testing.T, tableName string, mode ydb.ChangeFeedModeType) {
	currTableName := fmt.Sprintf("test_table_%v", tableName)

	source := &ydb.YdbSource{
		Token:              server.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             []string{currTableName},
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		ServiceAccountID:   "",
		ChangeFeedMode:     mode,
	}
	target := model.ChDestination{
		ShardsList: []model.ClickHouseShard{
			{
				Name: "_",
				Hosts: []string{
					"localhost",
				},
			},
		},
		User:                "default",
		Password:            "",
		Database:            "database",
		HTTPPort:            helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort:          helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
		ProtocolUnspecified: true,
		Cleanup:             server.Drop,
	}
	transferType := abstract.TransferTypeSnapshotAndIncrement
	helpers.InitSrcDst(helpers.TransferID, source, &target, transferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable

	//---

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "CH target Native", Port: target.NativePort},
			helpers.LabeledPort{Label: "CH target HTTP", Port: target.HTTPPort},
		))
	}()

	//---

	Target := &ydb.YdbDestination{
		Database: source.Database,
		Token:    source.Token,
		Instance: source.Instance,
	}
	Target.WithDefaults()
	srcSink, err := ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	// insert one rec - for snapshot uploading

	require.NoError(t, srcSink.Push([]abstract.ChangeItem{
		*helpers.YDBStmtInsert(t, currTableName, 1),
		*helpers.YDBStmtInsertNulls(t, currTableName, 2),
	}))

	// start snapshot & replication

	transfer := helpers.MakeTransfer(helpers.TransferID, source, &target, transferType)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	helpers.CheckRowsCount(t, target, target.Database, currTableName, 2)

	// insert two more records - it's three of them now

	require.NoError(t, srcSink.Push([]abstract.ChangeItem{
		*helpers.YDBStmtInsertNulls(t, currTableName, 3),
		*helpers.YDBStmtInsert(t, currTableName, 4),
	}))

	if mode == ydb.ChangeFeedModeNewImage || mode == ydb.ChangeFeedModeNewAndOldImages {
		ydbConn, err := NewYDBConnection(Target)
		require.NoError(t, err)
		err = ydbConn.Table().Do(context.Background(), func(ctx context.Context, session table.Session) (err error) {
			return session.ExecuteSchemeQuery(ctx, fmt.Sprintf(`
--!syntax_v1
ALTER TABLE %s ADD COLUMN brand_new_text_column Text;
`, currTableName))
		})
		require.NoError(t, err)

		err = ydbConn.Table().Do(context.Background(), func(ctx context.Context, session table.Session) (err error) {
			writeTx := table.TxControl(
				table.BeginTx(
					table.WithSerializableReadWrite(),
				),
				table.CommitTx(),
			)

			_, _, err = session.Execute(ctx, writeTx, fmt.Sprintf(`
	--!syntax_v1
	UPDATE %s SET brand_new_text_column = 'abc';
	`, currTableName), nil)
			require.NoError(t, err)
			return nil
		})
		require.NoError(t, err)

		// insert another two more records - it's five of them now

		require.NoError(t, srcSink.Push([]abstract.ChangeItem{
			*customYDBInsertItem(t, currTableName, 5),
			*customYDBInsertItem(t, currTableName, 6),
		}))
	}

	// update 2nd rec

	require.NoError(t, srcSink.Push([]abstract.ChangeItem{
		*helpers.YDBStmtUpdate(t, currTableName, 4, 666),
	}))

	// update 3rd rec by TOAST

	require.NoError(t, srcSink.Push([]abstract.ChangeItem{
		*helpers.YDBStmtUpdateTOAST(t, currTableName, 4, 777),
	}))

	// delete 1st rec

	require.NoError(t, srcSink.Push([]abstract.ChangeItem{
		*helpers.YDBStmtDelete(t, currTableName, 1),
	}))

	// check

	if mode == ydb.ChangeFeedModeNewImage || mode == ydb.ChangeFeedModeNewAndOldImages {
		require.NoError(t, helpers.WaitDestinationEqualRowsCount(target.Database, currTableName, helpers.GetSampleableStorageByModel(t, target), 60*time.Second, 5))
	} else {
		require.NoError(t, helpers.WaitDestinationEqualRowsCount(target.Database, currTableName, helpers.GetSampleableStorageByModel(t, target), 60*time.Second, 3))
	}
}
