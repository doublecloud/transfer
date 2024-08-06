package queue

import (
	"github.com/doublecloud/tross/library/go/core/xerrors"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"go.ytsaurus.tech/library/go/core/log"
)

func New(format server.SerializationFormat, saveTxOrder, dropKeys, isSnapshot bool, logger log.Logger) (Serializer, error) {
	var result Serializer
	var err error
	switch format.Name {
	case server.SerializationFormatDebezium:
		result, err = NewDebeziumSerializer(format.Settings, saveTxOrder, dropKeys, isSnapshot, logger)
	case server.SerializationFormatJSON:
		result, err = NewJSONSerializer(*format.BatchingSettings, saveTxOrder, logger)
	case server.SerializationFormatMirror, server.SerializationFormatLbMirror:
		result, err = NewMirrorSerializer(logger)
	case server.SerializationFormatNative:
		result, err = NewNativeSerializer(*format.BatchingSettings, saveTxOrder)
	case server.SerializationFormatRawColumn:
		result = NewRawColumnSerializer(format.Settings[server.ColumnNameParamName], logger)
	default:
		return nil, xerrors.Errorf("unknown serialization format: %s", format.Name)
	}
	if err != nil {
		return result, xerrors.Errorf("unable to create serializer: %w", err)
	}
	return result, nil
}
