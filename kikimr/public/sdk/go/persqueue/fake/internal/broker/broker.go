// Copyright (c) 2019 Yandex LLC. All rights reserved.
// Author: Andrey Khaliullin <avhaliullin@yandex-team.ru>

package broker

import (
	"context"
	"fmt"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/fake/internal/concurrent"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/fake/internal/topic"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/log"
)

// Broker is concurrent container for all fake logbroker's stateful discoverable entities:
// topics, writers with seqno stored for topic+source, readers with offset stored for
// topic+clientID
type Broker struct {
	ctx    context.Context
	logger log.Logger

	topics  *concurrent.Map
	readers *concurrent.Map
	writers *concurrent.Map
}

func NewBroker(ctx context.Context, logger log.Logger) *Broker {
	return &Broker{
		ctx:     ctx,
		logger:  logger,
		topics:  concurrent.NewMap(),
		readers: concurrent.NewMap(),
		writers: concurrent.NewMap(),
	}
}

// GetTopicWriter returns writer for specified topic and source, writer and topic are created if needed
func (b *Broker) GetTopicWriter(topicName string, source string) (*topic.Writer, error) {
	res, err := b.writers.GetOrCreateWithErr(
		writerKey{source: source, topic: topicName},
		b.createWriter,
	)
	if err != nil {
		return nil, err
	}
	return res.(*topic.Writer), nil
}

func (b *Broker) createWriter(k interface{}) (interface{}, error) {
	key := k.(writerKey)
	t, err := b.GetTopic(key.topic)
	if err != nil {
		return nil, err
	}
	part := t.RotatePartForWrite()
	q := t.GetQueue(part)
	return topic.NewWriter(b.ctx, b.logger, part, q, key.topic, key.source), nil
}

// GetTopicReader returns reader for specified topic and clientID, reader and topic are created if needed
func (b *Broker) GetTopicReader(topicName string, clientID string) (*topic.Reader, error) {
	res, err := b.readers.GetOrCreateWithErr(
		readerKey{topic: topicName, clientID: clientID},
		b.createReader,
	)
	if err != nil {
		return nil, err
	}
	return res.(*topic.Reader), nil
}

func (b *Broker) createReader(k interface{}) (interface{}, error) {
	key := k.(readerKey)
	t, err := b.GetTopic(key.topic)
	if err != nil {
		return nil, err
	}
	return topic.NewReader(b.ctx, b.logger, t), nil
}

func (b *Broker) CreateTopic(topicName string, parts int) error {
	if parts <= 0 {
		return fmt.Errorf("parts count must be positive")
	}
	if !b.topics.PutIfAbsent(topicName, topic.NewTopic(b.logger, topicName, parts)) {
		return fmt.Errorf("topic '%s' already exists", topicName)
	}
	return nil
}

func (b *Broker) GetTopic(topicName string) (*topic.Topic, error) {
	res := b.topics.Get(topicName)
	if res == nil {
		return nil, fmt.Errorf("topic doesn't exist: %s", topicName)
	}
	return res.(*topic.Topic), nil
}

type writerKey struct {
	topic  string
	source string
}
type readerKey struct {
	topic    string
	clientID string
}
