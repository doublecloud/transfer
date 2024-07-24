package native

import (
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/parsers"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/stats"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util"
)

type ParserNative struct {
	logger log.Logger
}

func (p *ParserNative) Do(msg persqueue.ReadMessage, partition abstract.Partition) []abstract.ChangeItem {
	changeItems, err := abstract.UnmarshalChangeItems(msg.Data)
	if err != nil {
		p.logger.Debug("Unable to convert body to changeItems", log.Error(err), log.Any("body", util.Sample(string(msg.Data), 1*1024)))
	}
	return changeItems
}

func (p *ParserNative) DoBatch(batch persqueue.MessageBatch) []abstract.ChangeItem {
	result := make([]abstract.ChangeItem, 0)
	for _, msg := range batch.Messages {
		changeItems, err := abstract.UnmarshalChangeItems(msg.Data)
		if err != nil {
			p.logger.Debug("Unable to convert body to changeItems", log.Error(err), log.Any("body", util.Sample(string(msg.Data), 1*1024)))
		}
		result = append(result, changeItems...)
	}
	return result
}

func NewParserNative(_ interface{}, _ bool, logger log.Logger, registry *stats.SourceStats) (parsers.Parser, error) {
	parser := &ParserNative{
		logger: logger,
	}
	return parser, nil
}

func init() {
	parsers.Register(
		NewParserNative,
		[]parsers.AbstractParserConfig{new(ParserConfigNativeLb)},
	)
}
