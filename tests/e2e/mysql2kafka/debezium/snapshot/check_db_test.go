package main

import (
	"context"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/doublecloud/transfer/library/go/test/canon"
	"github.com/doublecloud/transfer/pkg/abstract"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	kafka2 "github.com/doublecloud/transfer/pkg/providers/kafka"
	"github.com/doublecloud/transfer/pkg/util"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
)

var (
	Source = helpers.RecipeMysqlSource()
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

func eraseMeta(in string) string {
	result := in
	tsmsRegexp := regexp.MustCompile(`"ts_ms":\d+`)
	result = tsmsRegexp.ReplaceAllString(result, `"ts_ms":0`)
	return result
}

func TestSnapshot(t *testing.T) {
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "Mysql source", Port: Source.Port},
	))
	//------------------------------------------------------------------------------
	//prepare dst

	dst, err := kafka2.DestinationRecipe()
	require.NoError(t, err)
	dst.Topic = "dbserver1"
	dst.FormatSettings = server.SerializationFormat{Name: server.SerializationFormatDebezium}
	//------------------------------------------------------------------------------
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
	additionalTransfer := helpers.MakeTransfer("additional", &kafka2.KafkaSource{
		Connection:  dst.Connection,
		Auth:        dst.Auth,
		GroupTopics: []string{dst.Topic},
		IsHomo:      true,
	}, &mockTarget, abstract.TransferTypeIncrementOnly)
	//------------------------------------------------------------------------------
	// activate main transfer

	helpers.InitSrcDst(helpers.TransferID, Source, dst, abstract.TransferTypeSnapshotOnly)
	transfer := helpers.MakeTransfer(helpers.TransferID, Source, dst, abstract.TransferTypeSnapshotOnly)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

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

	for {
		if len(result) == 1 {
			canonVal := eraseMeta(string(kafka2.GetKafkaRawMessageData(&result[0])))
			canon.SaveJSON(t, canonVal)
			break
		}
		time.Sleep(time.Second)
	}
}
