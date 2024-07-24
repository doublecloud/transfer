package unpacker

import (
	"encoding/binary"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/schemaregistry/confluent"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util"
)

type SchemaRegistry struct {
	schemaRegistryClient *confluent.SchemaRegistryClient
}

func (s *SchemaRegistry) Unpack(message []byte) ([]byte, []byte, error) {
	if len(message) < 5 {
		return nil, nil, xerrors.Errorf("Can't extract schema id form message: message length less then 5 (%v)", len(message))
	}
	if message[0] != 0 {
		return nil, nil, xerrors.Errorf("Unknown magic byte in message (%v) (first byte in message must be 0)", string(message))
	}
	schemaID := binary.BigEndian.Uint32(message[1:5])

	schema, _ := backoff.RetryNotifyWithData(func() (*confluent.Schema, error) {
		return s.schemaRegistryClient.GetSchema(int(schemaID))
	}, backoff.NewConstantBackOff(time.Second), util.BackoffLogger(logger.Log, "getting schema"),
	)

	return []byte(schema.Schema), message[5:], nil
}

func NewSchemaRegistry(srClient *confluent.SchemaRegistryClient) *SchemaRegistry {
	return &SchemaRegistry{
		schemaRegistryClient: srClient,
	}
}
