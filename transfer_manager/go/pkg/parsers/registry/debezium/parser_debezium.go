package debezium

import (
	"runtime"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers"
	debeziumengine "github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry/debezium/engine"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/schemaregistry/confluent"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewParserDebezium(inWrapped interface{}, _ bool, logger log.Logger, _ *stats.SourceStats) (parsers.Parser, error) {
	var client *confluent.SchemaRegistryClient = nil
	var err error
	switch in := inWrapped.(type) {
	case *ParserConfigDebeziumLb:
		if in.SchemaRegistryURL == "" {
			break
		}
		client, err = confluent.NewSchemaRegistryClientWithTransport(in.SchemaRegistryURL, in.TLSFile, logger)
		if err != nil {
			return nil, xerrors.Errorf("Unable to create schema registry client: %w", err)
		}
		client.SetCredentials(in.Username, in.Password)
	case *ParserConfigDebeziumCommon:
		if in.SchemaRegistryURL == "" {
			break
		}
		client, err = confluent.NewSchemaRegistryClientWithTransport(in.SchemaRegistryURL, in.TLSFile, logger)
		if err != nil {
			return nil, xerrors.Errorf("Unable to create schema registry client: %w", err)
		}
		client.SetCredentials(in.Username, in.Password)
	}

	return debeziumengine.NewDebeziumImpl(logger, client, uint64(runtime.NumCPU()*4)), nil
}

func init() {
	parsers.Register(
		NewParserDebezium,
		[]parsers.AbstractParserConfig{new(ParserConfigDebeziumCommon), new(ParserConfigDebeziumLb)},
	)
}
