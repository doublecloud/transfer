package confluentschemaregistry

import (
	"github.com/doublecloud/tross/transfer_manager/go/pkg/parsers"
	conflueentschemaregistryengine "github.com/doublecloud/tross/transfer_manager/go/pkg/parsers/registry/confluentschemaregistry/engine"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewParserConfluentSchemaRegistry(inWrapped interface{}, _ bool, logger log.Logger, _ *stats.SourceStats) (parsers.Parser, error) {
	switch in := inWrapped.(type) {
	case *ParserConfigConfluentSchemaRegistryCommon:
		return conflueentschemaregistryengine.NewConfluentSchemaRegistryImpl(in.SchemaRegistryURL, in.TLSFile, in.Username, in.Password, false, logger), nil
	case *ParserConfigConfluentSchemaRegistryLb:
		return conflueentschemaregistryengine.NewConfluentSchemaRegistryImpl(in.SchemaRegistryURL, in.TLSFile, in.Username, in.Password, false, logger), nil
	}
	return nil, nil
}

func init() {
	parsers.Register(
		NewParserConfluentSchemaRegistry,
		[]parsers.AbstractParserConfig{new(ParserConfigConfluentSchemaRegistryCommon), new(ParserConfigConfluentSchemaRegistryLb)},
	)
}
