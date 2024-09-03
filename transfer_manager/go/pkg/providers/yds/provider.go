package yds

import (
	"context"
	"encoding/gob"

	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	cpclient "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/middlewares"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry/audittrailsv1"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/elastic"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/opensearch"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/worker/tasks"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	destinationFactory := func() server.Destination {
		return new(YDSDestination)
	}
	gob.RegisterName("*server.YDSSource", new(YDSSource))
	gob.RegisterName("*server.YDSDestination", new(YDSDestination))
	server.RegisterSource(ProviderType, func() server.Source {
		return new(YDSSource)
	})
	server.RegisterDestination(ProviderType, destinationFactory)
	abstract.RegisterProviderName(ProviderType, "YDS")
	providers.Register(ProviderType, New)
}

const ProviderType = abstract.ProviderType("yds")

// To verify providers contract implementation
var (
	_ providers.Replication = (*Provider)(nil)
	_ providers.Sinker      = (*Provider)(nil)

	_ providers.Activator   = (*Provider)(nil)
	_ providers.Deactivator = (*Provider)(nil)
	_ providers.Cleanuper   = (*Provider)(nil)
	_ providers.Tester      = (*Provider)(nil)
)

type Provider struct {
	logger   log.Logger
	registry metrics.Registry
	cp       cpclient.Coordinator
	transfer *server.Transfer
}

func (p *Provider) Type() abstract.ProviderType {
	return ProviderType
}

func (p *Provider) Source() (abstract.Source, error) {
	src, ok := p.transfer.Src.(*YDSSource)
	if !ok {
		return nil, xerrors.Errorf("Unknown source type: %T", p.transfer.Src)
	}
	src.IsLbSink = p.transfer.DstType() == ProviderType
	_, isElasticSearchDst := p.transfer.Dst.(*elastic.ElasticSearchDestination)
	_, isOpenSearchDst := p.transfer.Dst.(*opensearch.OpenSearchDestination)

	if isElasticSearchDst || isOpenSearchDst {
		if parsers.IsThisParserConfig(src.ParserConfig, new(audittrailsv1.ParserConfigAuditTrailsV1Common)) {
			src.ParserConfig, _ = parsers.ParserConfigStructToMap(&audittrailsv1.ParserConfigAuditTrailsV1Common{UseElasticSchema: true})
		}
	}
	return NewSource(p.transfer.ID, src, p.logger, p.registry)
}

func (p *Provider) Sink(middlewares.Config) (abstract.Sinker, error) {
	cfg, ok := p.transfer.Dst.(*YDSDestination)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}
	cfgCopy := *cfg
	cfgCopy.LbDstConfig.FormatSettings = InferFormatSettings(p.transfer.Src, cfgCopy.LbDstConfig.FormatSettings)
	return NewSink(&cfgCopy, p.registry, p.logger, p.transfer.ID)
}

const (
	ReadRuleCheck = abstract.CheckType("read-rule-check")
	ConfiigCheck  = abstract.CheckType("source-config-check")
)

func (p *Provider) Test(ctx context.Context) *abstract.TestResult {
	tr := abstract.NewTestResult(ReadRuleCheck)
	src, ok := p.transfer.Src.(*YDSSource)
	if !ok {
		return nil
	}
	if src.Consumer == "" {
		src.Consumer = "test-endpoint-" + p.transfer.ID
		defer func() {
			err := DropReadRule(src, "")
			if err != nil {
				p.logger.Error("cannot drop read rule", log.Error(err))
			}
		}()
	}
	err := CreateReadRule(src, "")
	if err != nil {
		return tr.NotOk(ReadRuleCheck, xerrors.Errorf("unable to add read rule: %w", err))
	}
	tr.Ok(ReadRuleCheck)
	source, err := p.Source()
	if err != nil {
		return tr.NotOk(ConfiigCheck, xerrors.Errorf("unable to construct reader: %w", err))
	}
	return tasks.SniffReplicationData(ctx, source.(abstract.Fetchable), tr, p.transfer)
}

func (p *Provider) Activate(ctx context.Context, task *server.TransferOperation, table abstract.TableMap, callbacks providers.ActivateCallbacks) error {
	if !p.transfer.IncrementOnly() {
		return xerrors.New("Only allowed mode for YDS source is replication")
	}
	src, ok := p.transfer.Src.(*YDSSource)
	if !ok {
		return xerrors.Errorf("unexpected src type: %T", p.transfer.Src)
	}
	if err := CreateReadRule(src, p.transfer.ID); err != nil {
		return xerrors.Errorf("unable to add read rule: %w", err)
	}
	return nil
}

func (p *Provider) dropNonCustomReadRule() error {
	src, ok := p.transfer.Src.(*YDSSource)
	if !ok {
		return xerrors.Errorf("unexpected src type: %T", p.transfer.Src)
	}
	if src.Consumer != "" {
		p.logger.Infof("skip drop for user defined consumer '%v'", src.Consumer)
		return nil
	}
	return DropReadRule(src, p.transfer.ID)
}

func (p *Provider) Cleanup(ctx context.Context, task *server.TransferOperation) error {
	return p.dropNonCustomReadRule()
}

func (p *Provider) Deactivate(ctx context.Context, task *server.TransferOperation) error {
	return p.dropNonCustomReadRule()
}

func New(lgr log.Logger, registry metrics.Registry, cp cpclient.Coordinator, transfer *server.Transfer) providers.Provider {
	return &Provider{
		logger:   lgr,
		registry: registry,
		cp:       cp,
		transfer: transfer,
	}
}
