package kafka

import (
	"context"
	"encoding/gob"

	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/library/go/core/metrics"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/library/go/slices"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	cpclient "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/middlewares"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/logbroker"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util"
)

func init() {
	destinationFactory := func() server.Destination {
		return new(KafkaDestination)
	}
	gob.RegisterName("*server.KafkaSource", new(KafkaSource))
	gob.RegisterName("*server.KafkaDestination", new(KafkaDestination))
	server.RegisterSource(ProviderType, func() server.Source {
		return new(KafkaSource)
	})
	server.RegisterDestination(ProviderType, destinationFactory)
	abstract.RegisterProviderName(ProviderType, "Kafka")

	providers.Register(ProviderType, New)
}

const ProviderType = abstract.ProviderType("kafka")

// To verify providers contract implementation
var (
	_ providers.Replication = (*Provider)(nil)
	_ providers.Sniffer     = (*Provider)(nil)
	_ providers.Sinker      = (*Provider)(nil)

	_ providers.Activator = (*Provider)(nil)
)

var (
	systemTopics = util.NewSet(
		"__consumer_offsets", // is used to store information about committed offsets for each topic:partition per group of consumers (groupID).
		"_schema",            // is not a default kafka topic (at least at kafka 8,9). This is an internal topic used by the Schema Registry which is a distributed storage layer for Avro schemas.
	)
)

type Provider struct {
	logger   log.Logger
	registry metrics.Registry
	cp       cpclient.Coordinator
	transfer *server.Transfer
}

func (p *Provider) Sniffer(ctx context.Context) (abstract.Fetchable, error) {
	src, ok := p.transfer.Src.(*KafkaSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected source type: %T", p.transfer.Src)
	}
	topics := src.GroupTopics
	if len(topics) == 0 && src.Topic != "" {
		topics = append(topics, src.Topic)
	}
	if len(topics) == 0 { // no topics specified, we should sniff all topics
		brokers, err := ResolveBrokers(src.Connection)
		if err != nil {
			return nil, xerrors.Errorf("unable to resolve brokers: %w", err)
		}

		if len(brokers) == 0 {
			return nil, xerrors.New("empty brokers")
		}
		mechanism, err := src.Auth.GetAuthMechanism()
		if err != nil {
			return nil, xerrors.Errorf("unable to define auth mechanism: %w", err)
		}
		tlsCfg, err := src.Connection.TLSConfig()
		if err != nil {
			return nil, xerrors.Errorf("unable to construct tls config: %w", err)
		}
		topics, err = listTopics(brokers[0], mechanism, tlsCfg)
		if err != nil {
			return nil, xerrors.Errorf("unabel to list topics: %w", err)
		}
		topics = slices.Filter(topics, func(s string) bool {
			return !systemTopics.Contains(s) // ignore system topics
		})
	}
	return &sourceMultiTopics{
		src:      src,
		topics:   topics,
		logger:   p.logger,
		registry: p.registry,
	}, nil
}

func (p *Provider) Type() abstract.ProviderType {
	return ProviderType
}

func (p *Provider) Source() (abstract.Source, error) {
	src, ok := p.transfer.Src.(*KafkaSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected source type: %T", p.transfer.Src)
	}
	if !src.IsHomo { // we can enforce homo from outside
		src.IsHomo = p.transfer.DstType() == ProviderType && src.IsDefaultMirror()
	}
	if !src.SynchronizeIsNeeded {
		src.SynchronizeIsNeeded = p.transfer.DstType() == logbroker.ProviderType
	}
	if len(p.transfer.DataObjects.GetIncludeObjects()) > 0 && len(src.GroupTopics) == 0 { // infer topics from transfer
		src.GroupTopics = p.transfer.DataObjects.GetIncludeObjects()
	}
	return NewSource(p.transfer.ID, src, p.logger, p.registry)
}

func (p *Provider) Sink(middlewares.Config) (abstract.Sinker, error) {
	dst, ok := p.transfer.Dst.(*KafkaDestination)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}
	cfgCopy := *dst
	cfgCopy.FormatSettings = InferFormatSettings(p.transfer.Src, cfgCopy.FormatSettings)
	return NewReplicationSink(&cfgCopy, p.registry, p.logger)
}

func (p *Provider) SnapshotSink(middlewares.Config) (abstract.Sinker, error) {
	dst, ok := p.transfer.Dst.(*KafkaDestination)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}
	cfgCopy := *dst
	cfgCopy.FormatSettings = InferFormatSettings(p.transfer.Src, cfgCopy.FormatSettings)
	return NewSnapshotSink(&cfgCopy, p.registry, p.logger)
}

func (p *Provider) Activate(ctx context.Context, task *server.TransferOperation, table abstract.TableMap, callbacks providers.ActivateCallbacks) error {
	if p.transfer.SrcType() == ProviderType && !p.transfer.IncrementOnly() {
		return xerrors.New("Only allowed mode for Kafka source is replication")
	}
	return nil
}

func New(lgr log.Logger, registry metrics.Registry, cp cpclient.Coordinator, transfer *server.Transfer) providers.Provider {
	return &Provider{
		logger:   lgr,
		registry: registry,
		cp:       cp,
		transfer: transfer,
	}
}
