package main

import (
	"testing"
	"time"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/library/go/test/canon"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	kafkasink "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/kafka"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/runtime/local"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/stretchr/testify/require"
)

func TestReplication(t *testing.T) {
	src, err := kafkasink.SourceRecipe()
	require.NoError(t, err)
	src.IsHomo = true

	dst, err := kafkasink.DestinationRecipe()
	require.NoError(t, err)
	dst.FormatSettings = server.SerializationFormat{Name: server.SerializationFormatMirror}

	// write to source topic
	k := []byte(`my_key`)
	v := []byte(`blablabla`)

	pushData(t, *src, "topic1", *dst, k, v)
	pushData(t, *src, "topic2", *dst, k, v)

	// prepare additional transfer: from dst to mock

	result := make([]abstract.ChangeItem, 0)
	mockSink := &helpers.MockSink{
		PushCallback: func(in []abstract.ChangeItem) {
			result = append(result, in...)
		},
	}
	mockTarget := server.MockDestination{
		SinkerFactory: func() abstract.Sinker { return mockSink },
		Cleanup:       server.DisabledCleanup,
	}
	additionalTransfer := helpers.MakeTransfer("additional", &kafkasink.KafkaSource{
		Connection:  dst.Connection,
		Auth:        dst.Auth,
		GroupTopics: []string{"topic1", "topic2"},
		IsHomo:      true,
	}, &mockTarget, abstract.TransferTypeIncrementOnly)

	localAdditionalWorker := local.NewLocalWorker(coordinator.NewFakeClient(), additionalTransfer, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.Log)
	localAdditionalWorker.Start()
	defer localAdditionalWorker.Stop()

	//-----------------------------------------------------------------------------------------------------------------
	st := time.Now()
	for time.Since(st) < time.Minute {
		if len(result) < 2 {
			time.Sleep(time.Second)
			continue
		}
		break
	}
	readedData := map[string]map[string]string{}
	for _, ci := range result {
		readedData[ci.TableID().String()] = map[string]string{
			"key":  string(kafkasink.GetKafkaRawMessageKey(&ci)),
			"data": string(kafkasink.GetKafkaRawMessageData(&ci)),
		}
	}
	require.Len(t, result, 2)
	canon.SaveJSON(t, readedData)
}

func pushData(t *testing.T, src kafkasink.KafkaSource, srcTopic string, dst kafkasink.KafkaDestination, k []byte, v []byte) {
	srcSink, err := kafkasink.NewReplicationSink(
		&kafkasink.KafkaDestination{
			Connection:          src.Connection,
			Auth:                src.Auth,
			Topic:               srcTopic,
			FormatSettings:      dst.FormatSettings,
			ParralelWriterCount: 10,
		},
		solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()}),
		logger.Log,
	)
	require.NoError(t, err)
	err = srcSink.Push([]abstract.ChangeItem{kafkasink.MakeKafkaRawMessage(srcTopic, time.Time{}, srcTopic, 0, 0, k, v)})
	require.NoError(t, err)
	require.NoError(t, srcSink.Close())
}
