package main

import (
	"context"
	"testing"
	"time"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	kafkasink "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/kafka"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/runtime/local"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/util"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/library/go/core/log"
)

func TestReplication(t *testing.T) {
	srcTopic := "topic1"
	dstTopic := "topic2"

	src, err := kafkasink.SourceRecipe()
	require.NoError(t, err)
	src.Topic = srcTopic
	src.IsHomo = true

	dst, err := kafkasink.DestinationRecipe()
	require.NoError(t, err)
	dst.Topic = dstTopic
	dst.FormatSettings = server.SerializationFormat{Name: server.SerializationFormatMirror}

	// write to source topic

	k := []byte(`my_key`)
	v := []byte(`blablabla`)

	srcSink, err := kafkasink.NewReplicationSink(
		&kafkasink.KafkaDestination{
			Connection:          src.Connection,
			Auth:                src.Auth,
			Topic:               src.Topic,
			FormatSettings:      dst.FormatSettings,
			ParralelWriterCount: 10,
		},
		solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()}),
		logger.Log,
	)
	require.NoError(t, err)
	err = srcSink.Push([]abstract.ChangeItem{kafkasink.MakeKafkaRawMessage(srcTopic, time.Time{}, srcTopic, 0, 0, k, v)})
	require.NoError(t, err)

	// prepare additional transfer: from dst to mock

	result := make([]abstract.ChangeItem, 0)
	mockSink := &helpers.MockSink{
		PushCallback: func(in []abstract.ChangeItem) {
			abstract.Dump(in)
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
		GroupTopics: []string{dst.Topic},
		IsHomo:      true,
	}, &mockTarget, abstract.TransferTypeIncrementOnly)

	// activate main transfer

	helpers.InitSrcDst(helpers.TransferID, src, dst, abstract.TransferTypeIncrementOnly)
	transfer := helpers.MakeTransfer(helpers.TransferID, src, dst, abstract.TransferTypeIncrementOnly)

	localWorker := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, solomon.NewRegistry(solomon.NewRegistryOpts()), log.With(logger.Log, log.Any("transfer", "main")))
	localWorker.Start()
	defer localWorker.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	go func() {
		for {
			// restart transfer if error
			errCh := make(chan error, 1)
			w, err := helpers.ActivateErr(additionalTransfer, func(err error) {
				errCh <- err
			})
			require.NoError(t, err)
			_, ok := util.Receive(ctx, errCh)
			if !ok {
				return
			}
			w.Close(t)
		}
	}()

	st := time.Now()
	for time.Since(st) < time.Second*30 {
		if len(result) == 1 {
			require.Equal(t, k, kafkasink.GetKafkaRawMessageKey(&result[0]))
			require.Equal(t, v, kafkasink.GetKafkaRawMessageData(&result[0]))
			break
		}

		time.Sleep(time.Second)
	}
}
