package kafka

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/library/go/slices"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/errors/coded"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/util"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"go.ytsaurus.tech/library/go/core/log"
)

type clientImpl struct {
	cfg *KafkaDestination
	lgr log.Logger
}

func createBrokerConn(broker string, mechanism sasl.Mechanism, tlsConfig *tls.Config) (*kafka.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	dialer := kafka.Dialer{
		TLS:           tlsConfig,
		SASLMechanism: mechanism,
	}
	brokerConn, err := dialer.DialContext(ctx, "tcp", broker)
	if err != nil {
		return nil, coded.Errorf(providers.NetworkUnreachable, "unable to DialContext broker, err: %w", err)
	}
	return brokerConn, nil
}

func createConn(broker string, mechanism sasl.Mechanism, tlsConfig *tls.Config) (*kafka.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	dialer := kafka.Dialer{
		TLS:           tlsConfig,
		SASLMechanism: mechanism,
		Timeout:       requestTimeout,
	}
	brokerConn, err := dialer.DialContext(ctx, "tcp", broker)
	if err != nil {
		return nil, coded.Errorf(providers.NetworkUnreachable, "unable to DialContext broker, err: %w", err)
	}
	defer brokerConn.Close()

	controller, err := brokerConn.Controller()
	if err != nil {
		return nil, xerrors.Errorf("unable get controller address, err: %w", err)
	}

	controllerConn, err := dialer.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return nil, coded.Errorf(providers.NetworkUnreachable, "unable to DialContext controller, err: %w", err)
	}

	return controllerConn, nil
}

func (c *clientImpl) topicExists(broker string, topic string, mechanism sasl.Mechanism, tlsConfig *tls.Config) (bool, error) {
	conn, err := createConn(broker, mechanism, tlsConfig)
	if err != nil {
		return false, xerrors.Errorf("unable to createConn, err: %w", err)
	}
	defer conn.Close()

	_, err = conn.ReadPartitions(topic)
	if err == kafka.UnknownTopicOrPartition {
		return false, nil
	}
	if err != nil {
		return false, xerrors.Errorf("unable to ReadPartitions, err: %w", err)
	}
	return true, nil
}

func listTopics(broker string, mechanism sasl.Mechanism, tlsConfig *tls.Config) ([]string, error) {
	conn, err := createConn(broker, mechanism, tlsConfig)
	if err != nil {
		return nil, xerrors.Errorf("unable to createConn, err: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, xerrors.Errorf("unable to list topics: %w", err)
	}

	return util.NewSet(slices.Map(partitions, func(t kafka.Partition) string {
		return t.Topic
	})...).Slice(), nil
}

func (c *clientImpl) CreateTopicIfNotExist(brokers []string, topic string, mechanism sasl.Mechanism, tlsConfig *tls.Config, entries []TopicConfigEntry) error {
	if len(brokers) == 0 {
		return abstract.NewFatalError(xerrors.New("expected at least one broker url"))
	}
	c.lgr.Infof("check topic exists: %s", topic)
	exists, err := c.topicExists(brokers[0], topic, mechanism, tlsConfig)
	if err != nil {
		return xerrors.Errorf("unable to check if topic exists: %w", err)
	}
	if exists {
		c.lgr.Infof("topic exists: %s", topic)
		return nil
	}

	conn, err := createConn(brokers[0], mechanism, tlsConfig)
	if err != nil {
		return xerrors.Errorf("unable to createConn, err: %w", err)
	}
	defer conn.Close()

	c.lgr.Infof("topic not exists: %s, create", topic)
	return conn.CreateTopics(kafka.TopicConfig{
		Topic:              topic,
		NumPartitions:      -1,
		ReplicationFactor:  -1,
		ReplicaAssignments: nil,
		ConfigEntries: slices.Map(entries, func(t TopicConfigEntry) kafka.ConfigEntry {
			return kafka.ConfigEntry{
				ConfigName:  t.ConfigName,
				ConfigValue: t.ConfigValue,
			}
		}),
	})
}

func (c *clientImpl) BuildWriter(brokers []string, compression kafka.Compression, saslMechanism sasl.Mechanism, tlsConfig *tls.Config) writer {
	return &kafka.Writer{
		Addr:       kafka.TCP(brokers...),
		Balancer:   &kafka.Hash{},
		BatchBytes: c.cfg.BatchBytes,
		Transport: &kafka.Transport{
			TLS:  tlsConfig,
			SASL: saslMechanism,
		},
		Compression: compression,
	}
}
