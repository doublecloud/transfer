package kafka

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/config"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/metering/writer"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/xtls"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/scram"
)

func init() {
	config.RegisterTypeTagged((*config.WriterConfig)(nil), (*Kafka)(nil), (*Kafka)(nil).Type(), nil)
	writer.Register(
		new(Kafka).Type(),
		func(schema, topic, sourceID string, cfg writer.WriterConfig) (writer.Writer, error) {
			return New(cfg.(*Kafka), schema, topic, sourceID, logger.Log)
		},
	)
}

type Kafka struct {
	Brokers       []string       `mapstructure:"brokers"`
	User          string         `mapstructure:"user"`
	Password      config.Secret  `mapstructure:"password"`
	TLSMode       config.TLSMode `mapstructure:"tls_mode"`
	AuthMechanism string         `mapstructure:"auth_mechanism"`
}

func (*Kafka) IsMeteringWriter() {}
func (*Kafka) IsTypeTagged()     {}
func (*Kafka) Type() string      { return "kafka" }

type Writer struct {
	sourceID    string
	kafkaWriter *kafka.Writer
}

func (w *Writer) Close() error {
	return nil
}

func New(cfg *Kafka, schema string, topic string, id string, l log.Logger) (*Writer, error) {
	var tlsFiles []string
	var tlsEnabled bool

	switch tlsMode := cfg.TLSMode.(type) {
	case *config.TLSModeDisabled:
		tlsEnabled = false
	case *config.TLSModeEnabled:
		tlsFiles = tlsMode.CACertificatePaths
		tlsEnabled = true
	}

	var mechanism sasl.Mechanism
	if cfg.User == "" {
		mechanism = nil
	} else {
		var algo scram.Algorithm
		if cfg.AuthMechanism == "SHA-512" {
			algo = scram.SHA512
		} else {
			algo = scram.SHA256
		}

		m, err := scram.Mechanism(algo, cfg.User, string(cfg.Password))
		if err != nil {
			return nil, xerrors.Errorf("failed to init sasl mechanism: %w", err)
		}
		mechanism = m
	}

	var tlsCfg *tls.Config
	var err error
	if len(tlsFiles) > 0 {
		tlsCfg, err = xtls.FromPath(tlsFiles)
		if err != nil {
			return nil, err
		}
	} else if tlsEnabled && len(tlsFiles) == 0 {
		tlsCfg = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	writer := &kafka.Writer{
		Addr:     kafka.TCP(cfg.Brokers...),
		Topic:    topic,
		Balancer: &kafka.Hash{},
		Transport: &kafka.Transport{
			TLS:  tlsCfg,
			SASL: mechanism,
		},
	}

	return &Writer{
		kafkaWriter: writer,
		sourceID:    id,
	}, nil
}

func (w *Writer) Write(data string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if data == "" {
		return nil
	}

	message := kafka.Message{
		Key:   []byte(w.sourceID),
		Value: []byte(data),
	}

	if err := w.kafkaWriter.WriteMessages(ctx, message); err != nil {
		switch t := err.(type) {
		case kafka.MessageTooLargeError:
			return xerrors.Errorf("one message exceeded max message size (client-side limit, can be changed by private option BatchBytes), len(key):%d, len(val):%d, err:%w", len(t.Message.Key), len(t.Message.Value), err)
		default:
			return xerrors.Errorf("unable to write message: %w", err)
		}
	}
	return nil
}
