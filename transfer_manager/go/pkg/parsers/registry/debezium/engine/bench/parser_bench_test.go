package main

import (
	_ "embed"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/parsers/registry/debezium/engine"
)

//go:embed parser_test.jsonl
var benchTest []byte

func BenchmarkParsingViaMultithreading(b *testing.B) {
	batchSizes := []int{1, 10, 100, 1000, 10000, 100000}
	b.ResetTimer()
	for _, size := range batchSizes {
		for threads := 1; threads <= runtime.NumCPU()*8; threads *= 2 {
			parser := engine.NewDebeziumImpl(logger.Log, nil, uint64(threads))
			batch := makeBenchBatch(size)
			b.ResetTimer()
			b.Run(fmt.Sprintf("process %d messages in batch in %d threads", size, threads), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					parser.DoBatch(batch)
					runtime.GC()
				}
				// works only for go version 1.20+
				//	NsForMessage := float64(b.Elapsed()/(time.Nanosecond)) / float64(size*b.N)
				//	b.ReportMetric(NsForMessage, "ns/message")
				//	b.ReportMetric(1/NsForMessage*float64(time.Second), "messages/s")
				//	b.ReportMetric(0, "ns/op")
			})
		}
	}
}

func makeBenchBatch(size int) persqueue.MessageBatch {
	messages := make([]persqueue.ReadMessage, 0)
	for len(messages) < size {
		messages = append(messages, persqueue.ReadMessage{
			Offset:      uint64(len(messages)),
			SeqNo:       0,
			SourceID:    []byte("test_source_id"),
			CreateTime:  time.Now(),
			WriteTime:   time.Now(),
			IP:          "192.168.1.1",
			Data:        benchTest,
			ExtraFields: nil,
		})
	}
	return persqueue.MessageBatch{
		Topic:     "topicName",
		Partition: 0,
		Messages:  messages,
	}
}
