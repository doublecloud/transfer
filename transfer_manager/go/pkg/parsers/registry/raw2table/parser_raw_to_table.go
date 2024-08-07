package raw2table

import (
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/parsers"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/parsers/registry/raw2table/engine"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewParserRawToTable(inWrapped interface{}, sniff bool, logger log.Logger, registry *stats.SourceStats) (parsers.Parser, error) {
	var parser *engine.RawToTableImpl

	switch in := inWrapped.(type) {
	case *ParserConfigRawToTableCommon:
		parser = engine.NewRawToTable(
			logger,
			in.IsAddTimestamp,
			in.IsAddHeaders,
			in.IsAddKey,
			in.IsKeyString,
			in.IsValueString,
			in.IsTopicAsName,
			in.TableName,
		)
	case *ParserConfigRawToTableLb:
		return nil, xerrors.New("not implemented")
	default:
		return nil, xerrors.Errorf("unknown parserConfig type: %T", inWrapped)
	}

	return parser, nil
}

func init() {
	parsers.Register(
		NewParserRawToTable,
		[]parsers.AbstractParserConfig{new(ParserConfigRawToTableLb), new(ParserConfigRawToTableCommon)},
	)
}
