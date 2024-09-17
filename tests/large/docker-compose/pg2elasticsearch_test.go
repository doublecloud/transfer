package dockercompose

import (
	"testing"

	"github.com/doublecloud/transfer/library/go/test/canon"
	"github.com/doublecloud/transfer/pkg/abstract"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/providers/elastic"
	"github.com/doublecloud/transfer/pkg/providers/postgres"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
)

var (
	pg2ElasticTransferID  = "pg2elastic"
	pg2ElasticElasticPort = 9202
	pg2ElasticSource      = postgres.PgSource{
		Hosts:    []string{"localhost"},
		User:     "postgres",
		Password: "123",
		Database: "postgres",
		DBTables: []string{"public.test_table"},
		Port:     6789,

		PgDumpCommand: dockerPgDump,
	}
	pg2ElasticTarget = elastic.ElasticSearchDestination{
		ClusterID:        "",
		DataNodes:        []elastic.ElasticSearchHostPort{{Host: "localhost", Port: pg2ElasticElasticPort}},
		User:             "user",
		Password:         "",
		SSLEnabled:       false,
		TLSFile:          "",
		SubNetworkID:     "",
		SecurityGroupIDs: nil,
		Cleanup:          server.DisabledCleanup,
	}
)

func init() {
	helpers.InitSrcDst(pg2ElasticTransferID, &pg2ElasticSource, &pg2ElasticTarget, abstract.TransferTypeSnapshotOnly)
}

func TestPgToElasticSnapshot(t *testing.T) {
	t.Parallel()

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Postgres source", Port: pg2ElasticSource.Port},
			helpers.LabeledPort{Label: "Elastic target", Port: pg2ElasticElasticPort},
		))
	}()

	transfer := helpers.MakeTransfer(pg2ElasticTransferID, &pg2ElasticSource, &pg2ElasticTarget, abstract.TransferTypeSnapshotOnly)

	helpers.Activate(t, transfer)

	client := createTestElasticClientFromDst(t, &pg2ElasticTarget)
	searchData, err := elasticGetAllDocuments(client, "public.test_table")
	require.NoError(t, err)
	canon.SaveJSON(t, searchData)
}
