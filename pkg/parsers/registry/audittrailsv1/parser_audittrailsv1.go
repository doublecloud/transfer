package audittrailsv1

import (
	"github.com/doublecloud/transfer/pkg/parsers"
	audittrailsv1engine "github.com/doublecloud/transfer/pkg/parsers/registry/audittrailsv1/engine"
	"github.com/doublecloud/transfer/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewParserAuditTrailsV1(inWrapped interface{}, sniff bool, logger log.Logger, registry *stats.SourceStats) (parsers.Parser, error) {
	in := inWrapped.(*ParserConfigAuditTrailsV1Common)

	return audittrailsv1engine.NewAuditTrailsV1ParserImpl(in.UseElasticSchema, sniff, logger, registry), nil
}

func init() {
	parsers.Register(
		NewParserAuditTrailsV1,
		[]parsers.AbstractParserConfig{new(ParserConfigAuditTrailsV1Common)},
	)
}
