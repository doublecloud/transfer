package protoutil

import (
	cloud "github.com/doublecloud/transfer/cloud/bitbucket/private-api/yandex/cloud/priv"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TODO: involves many copies on recursion. May be optimized
func CopyWithoutSensitiveFields(message protoreflect.ProtoMessage) protoreflect.ProtoMessage {
	result := proto.Clone(message)
	messageDescriptor := result.ProtoReflect().Descriptor()
	for i := 0; i < messageDescriptor.Fields().Len(); i++ {
		fieldDescriptor := messageDescriptor.Fields().Get(i)
		isSensitive := proto.GetExtension(fieldDescriptor.Options(), cloud.E_Sensitive).(bool)
		if isSensitive && message.ProtoReflect().Has(fieldDescriptor) {
			result.ProtoReflect().Clear(fieldDescriptor)
			continue
		}

		// Current field is not sensitive, but one of the nested fields might be. Let's recurse
		if fieldDescriptor.Kind() != protoreflect.MessageKind {
			// No messages in the field -> no recursion
			continue
		}
		if fieldDescriptor.Cardinality() == protoreflect.Repeated {
			if fieldDescriptor.IsList() {
				listField := result.ProtoReflect().Get(fieldDescriptor).List()
				if !listField.IsValid() {
					continue
				}
				for i := 0; i < listField.Len(); i++ {
					logSafeListElement := CopyWithoutSensitiveFields(listField.Get(i).Message().Interface())
					listField.Set(i, protoreflect.ValueOfMessage(logSafeListElement.ProtoReflect()))
				}
			} else { // Map
				mapValueDescriptor := fieldDescriptor.MapValue()
				if mapValueDescriptor.Kind() != protoreflect.MessageKind {
					continue
				}
				mapField := result.ProtoReflect().Get(fieldDescriptor).Map()
				if !mapField.IsValid() {
					continue
				}
				mapField.Range(func(key protoreflect.MapKey, value protoreflect.Value) bool {
					logSafeMapValue := CopyWithoutSensitiveFields(value.Message().Interface())
					mapField.Set(key, protoreflect.ValueOfMessage(logSafeMapValue.ProtoReflect()))
					return true
				})
			}
		} else { // Singular field
			nestedMessage := result.ProtoReflect().Get(fieldDescriptor).Message()
			if !nestedMessage.IsValid() {
				continue
			}
			logSafeNestedMessage := CopyWithoutSensitiveFields(nestedMessage.Interface()).ProtoReflect()
			result.ProtoReflect().Set(fieldDescriptor, protoreflect.ValueOfMessage(logSafeNestedMessage))
		}
	}
	return result
}
