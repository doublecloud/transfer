// Copyright (c) 2019 Yandex LLC. All rights reserved.
// Author: Andrey Khaliullin <avhaliullin@yandex-team.ru>

package model

import (
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/fake/data"
	logbroker "github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/genproto/Ydb_PersQueue_V0"
)

type Batch struct {
	Cookie   int
	Messages []*data.Message
}

func MessagesToProto(msgs []*data.Message) []*logbroker.ReadResponse_BatchedData_PartitionData {
	type sourceKey struct {
		topic  string
		source string
	}
	type partKey struct {
		topic string
		part  uint32
	}

	parts := make(map[partKey]*logbroker.ReadResponse_BatchedData_PartitionData)
	batches := make(map[sourceKey]*logbroker.ReadResponse_BatchedData_Batch)
	var res []*logbroker.ReadResponse_BatchedData_PartitionData
	for _, msg := range msgs {
		sKey := sourceKey{topic: msg.Topic, source: msg.Source}
		batch, ok := batches[sKey]
		if !ok {
			batch = &logbroker.ReadResponse_BatchedData_Batch{SourceId: []byte(msg.Source)}
			pKey := partKey{topic: msg.Topic, part: msg.Partition}
			partData, ok := parts[pKey]
			if !ok {
				partData = &logbroker.ReadResponse_BatchedData_PartitionData{
					Topic:     pKey.topic,
					Partition: pKey.part,
					Batch:     nil,
				}
				parts[pKey] = partData
				res = append(res, partData)
			}
			partData.Batch = append(partData.Batch, batch)
		}
		batch.MessageData = append(batch.MessageData, &logbroker.ReadResponse_BatchedData_MessageData{
			Codec:        logbroker.ECodec_RAW,
			Offset:       uint64(msg.Offset),
			SeqNo:        msg.SeqNo,
			CreateTimeMs: timeMillis(msg.Created),
			Data:         msg.Data,
		})
	}
	return res
}

func timeMillis(t time.Time) uint64 {
	return uint64(t.UnixNano() / 1000000)
}

type PartitionInfo struct {
	Topic      string
	Partition  uint32
	ReadOffset uint64
	EndOffset  uint64
}
