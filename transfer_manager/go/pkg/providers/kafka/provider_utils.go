package kafka

import (
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	debeziumparameters "github.com/doublecloud/transfer/transfer_manager/go/pkg/debezium/parameters"
	debezium_prod_status "github.com/doublecloud/transfer/transfer_manager/go/pkg/debezium/prodstatus"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/airbyte"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/mysql"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres"
)

type Mirrareable interface {
	ForceMirror()
}

func InferFormatSettings(src server.Source, formatSettings server.SerializationFormat) server.SerializationFormat {
	result := formatSettings.Copy()

	if result.Name == server.SerializationFormatAuto {
		if server.IsDefaultMirrorSource(src) {
			result.Name = server.SerializationFormatMirror
			return *result
		}
		if server.IsAppendOnlySource(src) {
			result.Name = server.SerializationFormatJSON
			return *result
		}

		switch src.(type) {
		case Mirrareable:
			result.Name = server.SerializationFormatLbMirror
		case *airbyte.AirbyteSource:
			result.Name = server.SerializationFormatJSON
		default:
			result.Name = server.SerializationFormatDebezium
		}
	}
	if result.Name == server.SerializationFormatDebezium {
		switch s := src.(type) {
		case *postgres.PgSource:
			if _, ok := result.Settings[debeziumparameters.DatabaseDBName]; !ok {
				result.Settings[debeziumparameters.DatabaseDBName] = s.Database
			}
			result.Settings[debeziumparameters.SourceType] = "pg"
		case *mysql.MysqlSource:
			result.Settings[debeziumparameters.SourceType] = "mysql"
		}
	}

	return *result
}

func sourceCompatible(src server.Source, transferType abstract.TransferType, serializationName server.SerializationFormatName) error {
	switch serializationName {
	case server.SerializationFormatAuto:
		return nil
	case server.SerializationFormatDebezium:
		if debezium_prod_status.IsSupportedSource(src.GetProviderType().Name(), transferType) {
			return nil
		}
		return xerrors.Errorf("in debezium serializer not supported source type: %s", src.GetProviderType().Name())
	case server.SerializationFormatJSON:
		if src.GetProviderType().Name() == airbyte.ProviderType.Name() {
			return nil
		}
		if server.IsAppendOnlySource(src) {
			return nil
		}
		return xerrors.New("in JSON serializer supported only next source types: AppendOnly and airbyte")
	case server.SerializationFormatNative:
		return nil
	case server.SerializationFormatMirror:
		if server.IsDefaultMirrorSource(src) {
			return nil
		}
		return xerrors.New("in Mirror serialized supported only default mirror source types")
	case server.SerializationFormatLbMirror:
		if src.GetProviderType().Name() == "lb" { // sorry again
			return nil
		}
		return xerrors.New("in LbMirror serialized supported only lb source type")
	case server.SerializationFormatRawColumn:
		return nil
	default:
		return xerrors.Errorf("unknown serializer name: %s", serializationName)
	}
}
