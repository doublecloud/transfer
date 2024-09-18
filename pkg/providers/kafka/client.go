package kafka

import (
	"context"
	"crypto/tls"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
)

// how to generate mock from 'client' and 'writer' interfaces:
// > export GO111MODULE=on && ya tool mockgen -source ./client.go -package kafka -destination ./client_mock.go

type writer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type client interface {
	CreateTopicIfNotExist(broker []string, topic string, mechanism sasl.Mechanism, tlsConfig *tls.Config, entries []TopicConfigEntry) error
	BuildWriter(broker []string, compression kafka.Compression, saslMechanism sasl.Mechanism, tlsConfig *tls.Config) writer
}
