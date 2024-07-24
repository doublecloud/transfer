package cloudlogging

import (
	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/parsers"
	cloudloggingengine "github.com/doublecloud/tross/transfer_manager/go/pkg/parsers/registry/cloudlogging/engine"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/stats"
)

func NewParserCloudLogging(inWrapped interface{}, sniff bool, logger log.Logger, registry *stats.SourceStats) (parsers.Parser, error) {
	return cloudloggingengine.NewCloudLoggingImpl(sniff, logger, registry), nil
}

func init() {
	parsers.Register(
		NewParserCloudLogging,
		[]parsers.AbstractParserConfig{new(ParserConfigCloudLoggingCommon)},
	)
}
