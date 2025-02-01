package unpacker

import (
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/util"
	"github.com/goccy/go-json"
)

type IncludeSchema struct {
}

func (*IncludeSchema) Unpack(message []byte) ([]byte, []byte, error) {
	var jsonMessage struct {
		Schema  json.RawMessage `json:"schema"`
		Payload json.RawMessage `json:"payload"`
	}
	err := json.Unmarshal(message, &jsonMessage)
	if err != nil {
		return nil, nil, xerrors.Errorf("can't decode message %s: %w", util.Sample(string(message), 1024), err)
	}

	return jsonMessage.Schema, jsonMessage.Payload, nil
}

func NewIncludeSchema() *IncludeSchema {
	return &IncludeSchema{}
}
