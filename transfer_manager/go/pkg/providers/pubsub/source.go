package pubsub

import (
	"context"
	"hash/fnv"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsequeue"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers"
	"go.ytsaurus.tech/library/go/core/log"
)

var (
	_ abstract.Source = (*Source)(nil)
)

type Source struct {
	cancel     context.CancelFunc
	client     *pubsub.Client
	ctx        context.Context
	transferID string
	topic      *pubsub.Topic
	lgr        log.Logger
	parser     parsers.Parser
}

func (s *Source) Run(sink abstract.AsyncSink) error {
	sub := s.client.Subscription(s.transferID)
	found, err := sub.Exists(s.ctx)
	if err != nil {
		return xerrors.Errorf("unable to check sub: %w", err)
	}
	if !found {
		return abstract.NewFatalError(xerrors.Errorf("sub: %s not found", s.transferID))
	}
	pq := parsequeue.NewWaitable[*pubsub.Message](s.lgr, 10, sink, func(message *pubsub.Message) []abstract.ChangeItem {
		return s.parser.Do(persqueue.ReadMessage{
			Offset:      0,
			SeqNo:       hash(message.ID),
			SourceID:    []byte(message.OrderingKey),
			CreateTime:  message.PublishTime,
			WriteTime:   message.PublishTime,
			IP:          "",
			Data:        message.Data,
			Codec:       0,
			ExtraFields: message.Attributes,
		}, abstract.Partition{
			Cluster:   "",
			Partition: 0,
			Topic:     s.topic.ID(),
		})
	}, func(data *pubsub.Message, pushSt time.Time, err error) {
		data.Ack()
	})
	defer pq.Close()
	return sub.Receive(s.ctx, func(ctx context.Context, message *pubsub.Message) {
		_ = pq.Add(message)
	})
}

func (s *Source) Stop() {
	s.cancel()
}

func hash(id string) uint64 {
	algorithm := fnv.New64a()
	_, _ = algorithm.Write([]byte(id))
	return algorithm.Sum64()
}

func NewSource() (*Source, error) {
	ctx, cancel := context.WithCancel(context.Background())
	client, err := pubsub.NewClient(ctx, "")
	if err != nil {
		return nil, xerrors.Errorf("unable to creat pubsub: %w", err)
	}
	return &Source{
		cancel: cancel,
		client: client,
		ctx:    ctx,
	}, nil
}
