package cloudevents

import (
	"github.com/doublecloud/transfer/pkg/parsers"
	cloudeventsengine "github.com/doublecloud/transfer/pkg/parsers/registry/cloudevents/engine"
	"github.com/doublecloud/transfer/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewParserCloudEvents(inWrapped interface{}, _ bool, logger log.Logger, _ *stats.SourceStats) (parsers.Parser, error) {
	switch in := inWrapped.(type) {
	case *ParserConfigCloudEventsCommon:
		return cloudeventsengine.NewCloudEventsImpl(in.TLSFile, in.Username, in.Password, in.PasswordFallback, true, logger, nil), nil
	case *ParserConfigCloudEventsLb:
		return cloudeventsengine.NewCloudEventsImpl(in.TLSFile, in.Username, in.Password, in.PasswordFallback, true, logger, nil), nil
	}
	return nil, nil
}

func init() {
	parsers.Register(
		NewParserCloudEvents,
		[]parsers.AbstractParserConfig{new(ParserConfigCloudEventsCommon), new(ParserConfigCloudEventsLb)},
	)
}
