package form

import (
	"context"

	cloud "github.com/doublecloud/transfer/cloud/bitbucket/private-api/yandex/cloud/priv"
	protoOptions "github.com/doublecloud/transfer/cloud/dataplatform/api/options"
	"github.com/doublecloud/transfer/cloud/dataplatform/dynamicform"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/runtime/protoimpl"
	"google.golang.org/protobuf/types/descriptorpb"
)

func IsSensitive(field protoreflect.FieldDescriptor) bool {
	isSecret := field.Message() != nil && field.Message().Name() == "Secret"
	isPassword := field.Name() == "password" || field.Name() == "raw_password"
	return isSecret || isPassword || isSensitiveExtension(field) || isViewedAsPassword(field)
}

func isSensitiveExtension(field protoreflect.FieldDescriptor) bool {
	extension := getExtensionFromField(field, cloud.E_Sensitive)
	if sensitive, ok := extension.(bool); ok {
		return sensitive
	}
	return false
}

func isViewedAsPassword(field protoreflect.FieldDescriptor) bool {
	extension := getExtensionFromField(field, protoOptions.E_FieldInfo)
	if fieldInfo, ok := extension.(*protoOptions.FieldInfo); ok {
		if appearance := fieldInfo.GetAppearance(); appearance != nil {
			return appearance.GetViewType() == "password"
		}
	}
	return false
}

func getExtensionFromField(field protoreflect.FieldDescriptor, extension *protoimpl.ExtensionInfo) any {
	options := field.Options()
	if fieldOptions, ok := options.(*descriptorpb.FieldOptions); ok {
		extension := proto.GetExtension(fieldOptions, extension)
		return extension
	}
	return nil
}

func getVisible(
	propertyProvider dynamicform.PropertyProvider,
	protoItem protoreflect.Descriptor,
	fieldInfo *dynamicform.FieldInfo,
) (bool, error) {
	visible := fieldInfo.Visible()
	if propertyProvider != nil {
		var err error
		visible, err = propertyProvider.GetVisible(fieldInfo.Visibility(), context.TODO(), dynamicform.LocaleEN, protoItem, visible)
		if err != nil {
			return false, xerrors.Errorf("unable get visible: %w", err)
		}
	}
	return visible, nil
}

func tryClearFieldFromInfo(
	message protoreflect.Message, field protoreflect.FieldDescriptor, propertyProvider dynamicform.PropertyProvider,
	clearHidden bool, clearImmutable bool, clearNoTest bool,
) error {
	if !clearHidden && !clearImmutable && !clearNoTest {
		return nil
	}

	fieldInfo, err := dynamicform.GetFieldInfo(field)
	if err != nil {
		return xerrors.Errorf("Can't get field info for '%v': %w", field.FullName(), err)
	}

	if clearHidden {
		visible, err := getVisible(propertyProvider, field, fieldInfo)
		if err != nil {
			return xerrors.Errorf("can't get visible for '%v': %w", field.FullName(), err)
		}
		if !visible {
			message.Clear(field)
			return nil
		}

		if field.Kind() == protoreflect.EnumKind && !field.IsList() {
			fieldValue := message.Get(field)
			fieldValueEnum := fieldValue.Enum()
			enum := field.Enum().Values().ByNumber(fieldValueEnum)
			enumInfo, err := dynamicform.GetEnumInfo(enum)
			if err != nil {
				return xerrors.Errorf("Can't get enum info for '%v': %w", field.FullName(), err)
			}

			visible, err := getVisible(propertyProvider, enum, enumInfo)
			if err != nil {
				return xerrors.Errorf("can't get visible for '%v': %w", field.FullName(), err)
			}
			if !visible {
				message.Clear(field)
				return nil
			}
		}
	}

	if clearImmutable && fieldInfo.Immutable() {
		message.Clear(field)
		return nil
	}

	if clearNoTest {
		if fieldInfo.NoTest() {
			message.Clear(field)
			return nil
		}

		if field.Kind() == protoreflect.EnumKind && !field.IsList() {
			fieldValue := message.Get(field)
			fieldValueEnum := fieldValue.Enum()
			enumValueDescriptor := field.Enum().Values().ByNumber(fieldValueEnum)
			enumInfo, err := dynamicform.GetEnumInfo(enumValueDescriptor)
			if err != nil {
				return xerrors.Errorf("Can't get enum info for '%v': %w", field.FullName(), err)
			}
			if enumInfo.NoTest() {
				message.Clear(field)
				return nil
			}
		}
	}

	return nil
}

func ClearFields(message protoreflect.Message, clearHidden bool, clearImmutable bool, clearSecrets bool, clearNoTest bool) error {
	descriptor := message.Descriptor()
	for i := 0; i < descriptor.Fields().Len(); i++ {
		field := descriptor.Fields().Get(i)
		if !message.Has(field) {
			continue
		}

		if clearHidden || clearImmutable || clearNoTest {
			propertyProvider := NewDefaultPropertyProvider()
			if err := tryClearFieldFromInfo(message, field, propertyProvider, clearHidden, clearImmutable, clearNoTest); err != nil {
				return xerrors.Errorf("Can't check field '%v': %w", field.FullName(), err)
			}
		}

		if clearSecrets {
			if IsSensitive(field) {
				message.Clear(field)
			}
		}

		switch {
		case !field.IsList() && !field.IsMap():
			if field.Kind() != protoreflect.MessageKind {
				continue
			}
			fieldValue := message.Get(field)
			fieldValueMessage := fieldValue.Message()
			if err := ClearFields(fieldValueMessage, clearHidden, clearImmutable, clearSecrets, clearNoTest); err != nil {
				return xerrors.Errorf("Can't check field '%v': %w", field.FullName(), err)
			}
		case field.IsList():
			if field.Kind() != protoreflect.MessageKind {
				continue
			}
			fieldValue := message.Get(field)
			fieldValueList := fieldValue.List()
			for j := 0; j < fieldValueList.Len(); j++ {
				listItemValue := fieldValueList.Get(j)
				if err := ClearFields(listItemValue.Message(), clearHidden, clearImmutable, clearSecrets, clearNoTest); err != nil {
					return xerrors.Errorf("Can't check array item #%d '%v': %w", j, field.FullName(), err)
				}
			}
		case field.IsMap():
			if field.MapValue().Kind() != protoreflect.MessageKind {
				continue
			}
			fieldValue := message.Get(field)
			fieldValueMap := fieldValue.Map()
			var mapErr error
			fieldValueMap.Range(func(key protoreflect.MapKey, value protoreflect.Value) bool {
				if err := ClearFields(value.Message(), clearHidden, clearImmutable, clearSecrets, clearNoTest); err != nil {
					mapErr = xerrors.Errorf("can't check map item key='%v' value='%v', field='%v': %w", key, value, field.FullName(), err)
					return false
				}
				return true
			})
			if mapErr != nil {
				return xerrors.Errorf("error with traversing map: %w", mapErr)
			}
		default:
		}
	}

	return nil
}
