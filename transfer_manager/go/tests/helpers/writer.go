package helpers

import (
	"testing"
	"time"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/logbroker"
	"github.com/stretchr/testify/require"
)

type LBWriter struct {
	abstract.Sinker
}

func (w *LBWriter) WriteLines(t *testing.T, topic string, lines []string) {
	for _, line := range lines {
		w.WriteBytes(t, topic, []byte(line))
	}
}

func (w *LBWriter) WriteBytes(t *testing.T, topic string, data []byte) {
	err := w.Push(
		[]abstract.ChangeItem{
			abstract.MakeRawMessage(
				"",
				time.Time{},
				topic,
				0,
				0,
				data,
			),
		},
	)
	require.NoError(t, err)
}

func NewLBWriter(t *testing.T, instance, topic string, port int, modelWithCreds logbroker.LbDestination) *LBWriter {
	dstLbWriter := &logbroker.LbDestination{
		Instance:    instance,
		Port:        port,
		Database:    "",
		Topic:       topic,
		Credentials: modelWithCreds.Credentials,
		FormatSettings: server.SerializationFormat{
			Name: server.SerializationFormatMirror,
		},
		TLS: logbroker.DisabledTLS,
	}
	dstLbWriter.WithDefaults()

	lbWriterSink, err := logbroker.NewReplicationSink(
		dstLbWriter,
		solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()}),
		logger.Log,
		"transferID",
	)
	require.NoError(t, err)
	return &LBWriter{lbWriterSink}
}

func WriteToLb(t *testing.T, instance, topic string, port int, modelWithCreds logbroker.LbDestination, lines []string) {
	lbWriterSink := NewLBWriter(t, instance, topic, port, modelWithCreds)
	defer lbWriterSink.Close()

	lbWriterSink.WriteLines(t, topic, lines)
}
