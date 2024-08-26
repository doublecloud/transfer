package dockercompose

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/library/go/test/canon"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/elastic"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres/pgrecipe"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/stretchr/testify/require"
)

var (
	elastic2PgTransferID = "elastic2pg"
	elasticPort          = 9203

	elasticSource = elastic.ElasticSearchSource{
		ClusterID:        "",
		DataNodes:        []elastic.ElasticSearchHostPort{{Host: "localhost", Port: elasticPort}},
		User:             "user",
		Password:         "",
		SSLEnabled:       false,
		TLSFile:          "",
		SubNetworkID:     "",
		SecurityGroupIDs: nil,
	}

	pgDestination = postgres.PgDestination{
		Hosts:    []string{"localhost"},
		User:     "postgres",
		Password: "123",
		Database: "postgres",
		Port:     6790,
	}
)

func init() {
	helpers.InitSrcDst(elastic2PgTransferID, &elasticSource, &pgDestination, abstract.TransferTypeSnapshotOnly)
}

func TestElasticToPgSnapshot(t *testing.T) {
	t.Parallel()
	// Fill the source with documents
	createElasticTestDocs(t, "test_doc", 0, 10)

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Postgres target", Port: pgDestination.Port},
			helpers.LabeledPort{Label: "Elastic source", Port: elasticPort},
		))
	}()

	dumpTargetDB := func() string {
		return pgrecipe.PgDump(
			t,
			[]string{"docker", "exec", "elastic2pg-pg-target-1", "pg_dump", "--table", "public.test_doc"},
			[]string{"docker", "exec", "elastic2pg-pg-target-1", "psql"},
			"user=postgres dbname=postgres password=123 host=localhost port=6790",
			"public.test_doc",
		)
	}

	transfer := helpers.MakeTransfer(elastic2PgTransferID, &elasticSource, &pgDestination, abstract.TransferTypeSnapshotOnly)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)
	helpers.CheckRowsCount(t, pgDestination, "public", "test_doc", 10)

	var canonData CanonData
	canonData.AfterSnapshot = dumpTargetDB()
	canon.SaveJSON(t, &canonData)
}

func TestExactTableRowsCount(t *testing.T) {
	t.Parallel()
	createElasticTestDocs(t, "test_table_rows_count", 0, 7)

	storage, err := elastic.NewStorage(&elasticSource, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), elastic.ElasticSearch)
	require.NoError(t, err)
	val, err := storage.ExactTableRowsCount(abstract.TableID{
		Name: "test_table_rows_count",
	})
	require.NoError(t, err)
	require.Equal(t, uint64(7), val)
}

func TestTableExists(t *testing.T) {
	t.Parallel()

	storage, err := elastic.NewStorage(&elasticSource, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), elastic.ElasticSearch)
	require.NoError(t, err)
	exists, err := storage.TableExists(abstract.TableID{
		Name: "inexistent-index",
	})
	require.Error(t, err)
	require.False(t, exists)

	createElasticTestDocs(t, "new_index", 0, 2)

	exists, err = storage.TableExists(abstract.TableID{
		Name: "new_index",
	})
	require.NoError(t, err)
	require.True(t, exists)
}

func TestTableList(t *testing.T) {
	storage, err := elastic.NewStorage(&elasticSource, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), elastic.ElasticSearch)
	require.NoError(t, err)

	// first delete all possible indexes generated by other tests
	deleteAllElasticIndexes(t)

	res, err := storage.TableList(nil)
	require.NoError(t, err)
	require.Equal(t, 0, len(res))

	createElasticTestDocs(t, "test_table_1", 0, 2)
	createElasticTestDocs(t, "test_table_2", 0, 2)

	res, err = storage.TableList(nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(res))
}

func createElasticTestDocs(t *testing.T, tableName string, from, to int) {
	sink, err := elastic.NewSink(elasticSource.SourceToElasticSearchDestination(), logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	require.NoError(t, sink.Push(generateRawMessages(tableName, 0, from, to)))

	config, err := elastic.ConfigFromDestination(logger.Log, elasticSource.SourceToElasticSearchDestination(), elastic.ElasticSearch)
	require.NoError(t, err)
	client, err := elastic.WithLogger(*config, logger.Log, elastic.ElasticSearch)
	require.NoError(t, err)
	for {
		total, err := elasticGetRowsTotal(client, tableName)
		require.NoError(t, err)

		if total == to {
			break
		}
		time.Sleep(5 * time.Second)
	}
}

func deleteAllElasticIndexes(t *testing.T) {
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://localhost:%d/_all", elasticPort), nil)
	require.NoError(t, err)

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)
}

func elasticGetRowsTotal(esClient *elasticsearch.Client, index string) (int, error) {
	var resp *esapi.Response
	resp, err := esClient.Indices.Stats()
	if err != nil {
		return 0, xerrors.Errorf("can't get elastic total rows: %w", err)
	}
	var stat struct {
		Indices map[string]struct {
			Total struct {
				Docs struct {
					Count int `json:"count"`
				} `json:"docs"`
			} `json:"total"`
		} `json:"indices"`
	}
	err = json.NewDecoder(resp.Body).Decode(&stat)
	if err != nil {
		return 0, xerrors.Errorf("can't decode elastic stat response: %w", err)
	}
	for indexName, total := range stat.Indices {
		if indexName == index {
			return total.Total.Docs.Count, nil
		}
	}
	return 0, nil
}

func generateRawMessages(table string, part, from, to int) []abstract.ChangeItem {
	ciTime := time.Date(2022, time.Month(10), 19, 0, 0, 0, 0, time.UTC)
	var res []abstract.ChangeItem
	for i := from; i < to; i++ {
		res = append(res, abstract.MakeRawMessage(
			table,
			ciTime,
			"test-topic",
			part,
			int64(i),
			[]byte(fmt.Sprintf("test_part_%v_value_%v", part, i)),
		))
	}
	return res
}