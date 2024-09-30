package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/debezium"
	"github.com/doublecloud/transfer/pkg/parsers"
	"github.com/doublecloud/transfer/pkg/parsers/generic"
	"github.com/doublecloud/transfer/pkg/schemaregistry/confluent"
	"github.com/doublecloud/transfer/pkg/util"
	"github.com/doublecloud/transfer/pkg/util/pool"
	"go.ytsaurus.tech/library/go/core/log"
)

type DebeziumImpl struct {
	logger           log.Logger
	debeziumReceiver *debezium.Receiver

	threadsNumber uint64
}

// DoOne message with multiple debezium events inside.
// Contains multiple debezium events only if messages are
// serialized using schema registry and started with magic zero-byte
func (p *DebeziumImpl) DoOne(partition abstract.Partition, buf []byte, offset uint64, writeTime time.Time) ([]byte, abstract.ChangeItem) {
	msgLen := len(buf)
	if len(buf) != 0 {
		if buf[0] == 0 {
			zeroIndex := bytes.Index(buf[5:], []byte{0})
			if zeroIndex != -1 {
				msgLen = 5 + zeroIndex
			}
		}
	}

	changeItem, err := p.debeziumReceiver.Receive(string(buf[0:msgLen]))
	if err != nil {
		var rawData string
		if len(buf) > 5 && buf[0] == 0 {
			// the case when using the schema registry
			rawData = fmt.Sprintf("%d%s", binary.BigEndian.Uint32(buf[1:5]), buf[5:])
		} else {
			rawData = string(buf)
		}
		p.logger.Warn("Unable to receive changeItems", log.Error(err), log.Any("body", util.Sample(string(buf), 1*1024)))
		return nil, generic.NewUnparsed(partition, strings.ReplaceAll(partition.Topic, "/", "_"), rawData, fmt.Sprintf("debezium receiver returned error, err: %s", err), 0, offset, writeTime)
	}
	return buf[msgLen:], *changeItem
}

func (p *DebeziumImpl) DoBuf(partition abstract.Partition, buf []byte, offset uint64, writeTime time.Time) []abstract.ChangeItem {
	result := make([]abstract.ChangeItem, 0, 1)
	leastBuf := buf
	for {
		if len(leastBuf) == 0 {
			break
		}
		var changeItem abstract.ChangeItem
		leastBuf, changeItem = p.DoOne(partition, leastBuf, offset, writeTime)
		result = append(result, changeItem)
	}
	return result
}

func (p *DebeziumImpl) doMultiThread(batch parsers.MessageBatch) []abstract.ChangeItem {
	multiThreadResult := make([][]abstract.ChangeItem, len(batch.Messages))

	currWork := func(in interface{}) {
		multiThreadResult[in.(int)] = p.Do(batch.Messages[in.(int)], abstract.Partition{Cluster: "", Partition: batch.Partition, Topic: batch.Topic})
	}

	threadsNumber := p.threadsNumber
	if p.threadsNumber == 0 {
		threadsNumber = 1
	}
	currPool := pool.NewDefaultPool(currWork, threadsNumber)
	_ = currPool.Run()
	for currTask := range batch.Messages {
		_ = currPool.Add(currTask)
	}
	_ = currPool.Close()

	result := make([]abstract.ChangeItem, 0, len(batch.Messages))
	for i := range multiThreadResult {
		result = append(result, multiThreadResult[i]...)
	}
	return result
}

func (p *DebeziumImpl) Do(msg parsers.Message, partition abstract.Partition) []abstract.ChangeItem {
	return p.DoBuf(partition, msg.Value, msg.Offset, msg.WriteTime)
}

func (p *DebeziumImpl) DoBatch(batch parsers.MessageBatch) []abstract.ChangeItem {
	if p.threadsNumber > 1 {
		return p.doMultiThread(batch)
	}
	result := make([]abstract.ChangeItem, 0, 1000)
	for _, msg := range batch.Messages {
		result = append(result, p.Do(msg, abstract.Partition{Cluster: "", Partition: batch.Partition, Topic: batch.Topic})...)
	}
	return result
}

func NewDebeziumImpl(logger log.Logger, schemaRegistry *confluent.SchemaRegistryClient, threads uint64) *DebeziumImpl {
	return &DebeziumImpl{
		logger:           logger,
		debeziumReceiver: debezium.NewReceiver(nil, schemaRegistry),
		threadsNumber:    threads,
	}
}
