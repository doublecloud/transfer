package blankparser

import (
	"testing"
	"time"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/parsers"
	"github.com/doublecloud/transfer/pkg/parsers/registry/blank"
	"github.com/doublecloud/transfer/pkg/parsers/registry/json"
	chrecipe "github.com/doublecloud/transfer/pkg/providers/clickhouse/recipe"
	"github.com/doublecloud/transfer/pkg/providers/kafka"
	"github.com/doublecloud/transfer/pkg/runtime/local"
	"github.com/doublecloud/transfer/pkg/transformer"
	"github.com/doublecloud/transfer/pkg/transformer/registry/jsonparser"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func TestLogs(t *testing.T) {
	src, err := kafka.SourceRecipe()
	require.NoError(t, err)
	src.Topic = "logs"
	require.NoError(t, kafka.CreateSourceTopicIfNotExist(src, src.Topic, logger.Log))
	dst, err := chrecipe.Target(chrecipe.WithInitFile("ch_init.sql"), chrecipe.WithDatabase("mtmobproxy"))
	require.NoError(t, err)

	src.Topic = "logs"
	parserConfigMap, err := parsers.ParserConfigStructToMap(new(blank.ParserConfigBlankLb))
	require.NoError(t, err)
	src.ParserConfig = parserConfigMap
	require.NoError(t, err)
	transfer := &server.Transfer{
		ID:  "e2e_test",
		Src: src,
		Dst: dst,
	}
	transfer.Transformation = &server.Transformation{
		Transformers: &transformer.Transformers{Transformers: []transformer.Transformer{{
			jsonparser.TransformerType: &jsonparser.Config{
				Parser: &json.ParserConfigJSONCommon{
					Fields: []abstract.ColSchema{
						{ColumnName: "msg", DataType: ytschema.TypeString.String()},
					},
					AddRest:       false,
					AddDedupeKeys: true,
				},
				Topic: "logs",
			},
		}}},
	}

	lgr, closer, err := logger.NewKafkaLogger(&logger.KafkaConfig{
		Broker:   src.Connection.Brokers[0],
		Topic:    src.Topic,
		User:     src.Auth.User,
		Password: src.Auth.Password,
	})
	require.NoError(t, err)

	defer closer.Close()
	// SEND TO KAFKA
	go func() {
		for i := 0; i < 50; i++ {
			lgr.Infof("line:%v", i)
		}
	}()
	w := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.Log)
	w.Start()
	require.NoError(t, helpers.WaitDestinationEqualRowsCount(dst.Database, src.Topic, helpers.GetSampleableStorageByModel(t, dst), 60*time.Second, 50))
	require.NoError(t, w.Stop())
}
