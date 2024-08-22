package dockercompose

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers"
	jsonparser "github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry/json"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/elastic"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yds"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/runtime/local"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/lbenv"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/stretchr/testify/require"
)

func TestYDSToElasticPushLogs(t *testing.T) {
	t.Parallel()
	lbEnv, stop := lbenv.NewLbEnv(t)

	yds2elasticTransferID := "yds2elastic"
	loggerPort := lbEnv.ProducerOptions().Port
	sourcePort := lbEnv.Port
	elasticPort := 9201

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "LB logger", Port: loggerPort},
			helpers.LabeledPort{Label: "YDS source", Port: sourcePort},
			helpers.LabeledPort{Label: "Elastic target", Port: elasticPort},
		))
	}()
	defer stop()

	loggerLbWriter, err := logger.NewLogbrokerLoggerFromConfig(&logger.LogbrokerConfig{
		Instance:    lbEnv.ProducerOptions().Endpoint,
		Port:        loggerPort,
		Topic:       lbEnv.DefaultTopic,
		SourceID:    "test",
		Credentials: lbEnv.ProducerOptions().Credentials,
	}, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	parserConfigStruct := &jsonparser.ParserConfigJSONCommon{
		Fields: []abstract.ColSchema{
			{ColumnName: "ts", DataType: "String", PrimaryKey: true},
			{ColumnName: "level", DataType: "String"},
			{ColumnName: "caller", DataType: "String"},
			{ColumnName: "msg", DataType: "String"},
		},
		AddRest:       false,
		AddDedupeKeys: true,
	}
	parserConfigMap, err := parsers.ParserConfigStructToMap(parserConfigStruct)
	require.NoError(t, err)

	src := &yds.YDSSource{
		Endpoint:       lbEnv.Endpoint,
		Port:           sourcePort,
		Database:       "",
		Stream:         lbEnv.DefaultTopic,
		Consumer:       lbEnv.DefaultConsumer,
		Credentials:    lbEnv.Creds,
		S3BackupBucket: "",
		BackupMode:     "",
		Transformer:    nil,
		ParserConfig:   parserConfigMap,
	}
	dst := &elastic.ElasticSearchDestination{
		ClusterID:        "",
		DataNodes:        []elastic.ElasticSearchHostPort{{Host: "localhost", Port: elasticPort}},
		User:             "user",
		Password:         "",
		SSLEnabled:       false,
		TLSFile:          "",
		SubNetworkID:     "",
		SecurityGroupIDs: nil,
		Cleanup:          "",
	}
	transfer := &model.Transfer{
		ID:  yds2elasticTransferID,
		Src: src,
		Dst: dst,
	}

	helpers.InitSrcDst(yds2elasticTransferID, src, dst, abstract.TransferTypeIncrementOnly)

	// SEND TO LOGBROKER
	go func() {
		for i := 0; i < 50; i++ {
			loggerLbWriter.Infof(fmt.Sprintf(`{"ID": "--%d--", "Bytes": %d}`, i, i))
		}
	}()
	w := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.Log)
	w.Start()

	// CHECK DESTINATION
	client := createTestElasticClientFromDst(t, dst)
	checkElasticDestinationCount(t, client, lbEnv.DefaultTopic, 50)
}

func checkElasticDestinationCount(t *testing.T, esClient *elasticsearch.Client, indexName string, count int) {
	startTime := time.Now()
	maxWaitingTime := time.Second * 60
	sleepTime := time.Second * 2
	var err error
	for time.Now().Before(startTime.Add(maxWaitingTime)) {
		time.Sleep(sleepTime)
		var indexStat map[string]int
		indexStat, err = elasticGetStat(esClient)
		if err != nil {
			continue
		}
		if indexStat[indexName] != count {
			err = xerrors.Errorf("The number of pushed (%v) and received (%v) rows does not match", count, indexStat[indexName])
		}
		if err == nil {
			break
		}
	}
	require.NoError(t, err)
}

func elasticGetStat(esClient *elasticsearch.Client) (map[string]int, error) {
	var resp *esapi.Response
	resp, err := esClient.Indices.Stats()
	if err != nil {
		return nil, xerrors.Errorf("can't get elastic stat: %w", err)
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
		return nil, xerrors.Errorf("can't decode elastic stat response: %w", err)
	}
	resultMap := make(map[string]int)
	for indexName, total := range stat.Indices {
		resultMap[indexName] = total.Total.Docs.Count
	}
	return resultMap, nil
}
