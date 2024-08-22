package main

import (
	"context"
	"testing"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/controlplane"
	Ydb_Persqueue_Protos_V1 "github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/genproto/Ydb_PersQueue_V1"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/log/corelogadapter"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/session"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yds"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/stretchr/testify/require"
)

func TestReplication(t *testing.T) {
	stream := "mytopic"
	consumer := "custom_consumer"

	lbControlPlane, err := controlplane.NewControlPlaneClient(context.Background(), session.Options{
		Endpoint: helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Logger:   corelogadapter.New(logger.Log),
		Database: helpers.GetEnvOfFail(t, "YDB_DATABASE"),
	})
	require.NoError(t, err)

	require.NoError(t, lbControlPlane.CreateTopic(
		context.Background(),
		&Ydb_Persqueue_Protos_V1.CreateTopicRequest{
			Path: stream,
			Settings: &Ydb_Persqueue_Protos_V1.TopicSettings{
				PartitionsCount:   1,
				RetentionPeriodMs: 3600000,
				SupportedFormat:   Ydb_Persqueue_Protos_V1.TopicSettings_FORMAT_BASE,
				ReadRules: []*Ydb_Persqueue_Protos_V1.TopicSettings_ReadRule{
					&Ydb_Persqueue_Protos_V1.TopicSettings_ReadRule{
						ConsumerName:    consumer,
						SupportedFormat: Ydb_Persqueue_Protos_V1.TopicSettings_FORMAT_BASE,
					},
				},
			},
		},
	))

	src := yds.YDSSource{
		Endpoint:       helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Database:       helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Stream:         stream,
		Consumer:       consumer,
		S3BackupBucket: "",
		BackupMode:     "",
		Transformer:    nil,
		ParserConfig:   nil,
	}

	dst := server.MockDestination{
		SinkerFactory: makeMockSinker,
	}

	helpers.InitSrcDst(helpers.TransferID, &src, &dst, abstract.TransferTypeIncrementOnly)
	transfer := helpers.MakeTransfer(helpers.TransferID, &src, &dst, abstract.TransferTypeIncrementOnly)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)
	require.NoError(t, helpers.Deactivate(t, transfer, worker))

	dRes, err := lbControlPlane.DescribeTopic(context.Background(), stream)
	require.NoError(t, err)
	require.True(t, readRuleExists(dRes, consumer))
}

func readRuleExists(topicDescription *Ydb_Persqueue_Protos_V1.DescribeTopicResult, consumer string) bool {
	for _, rule := range topicDescription.Settings.ReadRules {
		if rule.ConsumerName == consumer {
			return true
		}
	}
	return false
}

func makeMockSinker() abstract.Sinker {
	return &mockSinker{}
}

type mockSinker struct {
}

func (s *mockSinker) Close() error {
	return nil
}

func (s *mockSinker) Push(input []abstract.ChangeItem) error {
	for i, item := range input {
		logger.Log.Infof("push item %v: %v", i, item)
	}
	return nil
}
