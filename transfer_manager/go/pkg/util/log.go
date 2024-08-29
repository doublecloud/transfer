package util

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/doublecloud/transfer/cloud/dataplatform/ycloud/protoutil"
	"go.ytsaurus.tech/library/go/core/log"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type requestMethodCtxField struct{}

var RequestMethodKey = requestMethodCtxField{}

func AppendMethodField(ctx context.Context, fields []log.Field) []log.Field {
	if method := ctx.Value(RequestMethodKey); method != nil {
		if stringMethod, ok := method.(string); ok {
			return append(fields, log.String("method", stringMethod))
		}
	}
	return fields
}

type FieldNames struct {
	MainName        string
	StringFieldName string
}

func AppendRequestDataField(
	logFields []log.Field,
	protoMessage protoreflect.ProtoMessage,
	fieldNames FieldNames,
	lgr log.Logger,
) []log.Field {
	logSafeMessage := protoutil.CopyWithoutSensitiveFields(protoMessage)
	logSafeJSON, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(logSafeMessage)
	if err != nil {
		lgr.Warn(fmt.Sprintf("Cannot marshal %s message into JSON", fieldNames.MainName), log.Error(err))
	} else {
		const maxJSONLen = 32 * 1024
		if len(logSafeJSON) > maxJSONLen {
			logFields = append(logFields, log.ByteString(fmt.Sprintf(fieldNames.StringFieldName), SampleBytes(logSafeJSON, maxJSONLen)))
		} else {
			var jsonObject interface{}
			if err := json.Unmarshal(logSafeJSON, &jsonObject); err != nil {
				lgr.Warn("Cannot unmarshal protojson", log.Error(err))
			} else {
				logFields = append(logFields, log.Reflect(fieldNames.MainName, jsonObject))
			}
		}
	}
	return logFields
}

func LogFields(ctx context.Context, req interface{}, fieldNames FieldNames, lgr log.Logger) []log.Field {
	logFields := AppendMethodField(ctx, []log.Field{})
	if req == nil {
		return logFields
	}
	protoMessage, ok := req.(protoreflect.ProtoMessage)
	if ok {
		logFields = AppendRequestDataField(logFields, protoMessage, fieldNames, lgr)
	} else {
		lgr.Warnf("Input message of type %T does not implement protoreflect.ProtoMessage", protoMessage)
	}
	return logFields
}
