package form

import (
	"fmt"
	"math/rand"
	"strconv"

	protoopt "github.com/doublecloud/transfer/cloud/dataplatform/api/options"
	"github.com/doublecloud/transfer/cloud/dataplatform/dynamicform"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type FormDataGenerator struct {
	iterationsCount  int
	iteration        int
	dataCounter      uint64
	immutableCache   map[protoreflect.FullName]protoreflect.Value
	typ              protoreflect.MessageType
	ignoreVisible    bool
	propertyProvider dynamicform.PropertyProvider
}

func NewFormDataGenerator(typ protoreflect.MessageType, ignoreVisible bool) (*FormDataGenerator, error) {
	generator := &FormDataGenerator{
		iterationsCount:  0,
		iteration:        0,
		dataCounter:      0,
		immutableCache:   map[protoreflect.FullName]protoreflect.Value{},
		typ:              typ,
		ignoreVisible:    ignoreVisible,
		propertyProvider: NewDefaultPropertyProvider(),
	}
	return generator, generator.setIterationsCount()
}

func (generator *FormDataGenerator) Iteration() int {
	return generator.iteration
}

func (generator *FormDataGenerator) DataCounter() uint64 {
	return generator.dataCounter
}

func (generator *FormDataGenerator) nextDummy() uint64 {
	generator.dataCounter++
	return generator.dataCounter
}

func (generator *FormDataGenerator) nextDummyBytes(n int) []byte {
	buf := make([]byte, n)

	seed := int64(generator.nextDummy())
	rand.New(rand.NewSource(seed)).Read(buf)

	return buf
}

func (generator *FormDataGenerator) setIterationsCount() error {
	message := generator.typ.New()
	descriptor := message.Descriptor()
	var err error
	generator.iterationsCount, err = generator.getMaxOneOfVariantsCount(descriptor)
	if generator.iterationsCount == 0 {
		generator.iterationsCount = 1
	}
	if err != nil {
		return xerrors.Errorf("Can't get max oneof variants for message '%v': %w", descriptor.FullName(), err)
	}
	return nil
}

func (generator *FormDataGenerator) getMaxOneOfVariantsCount(descriptor protoreflect.MessageDescriptor) (int, error) {
	oneofs := descriptor.Oneofs()
	maxFieldsCount := 0
	for i := 0; i < oneofs.Len(); i++ {
		oneof := oneofs.Get(i)
		visibleFields := []protoreflect.FieldDescriptor{}
		for j := 0; j < oneof.Fields().Len(); j++ {
			field := oneof.Fields().Get(j)
			fieldInfo, err := dynamicform.GetFieldInfo(field)
			if err != nil {
				return 0, xerrors.Errorf("Can't get field info for field: %v", field.FullName())
			}

			visible, err := getVisible(generator.propertyProvider, field, fieldInfo)
			if err != nil {
				return 0, xerrors.Errorf("can't get visible for '%v': %w", field.FullName(), err)
			}
			if (visible || generator.ignoreVisible) && !fieldInfo.NoTest() {
				visibleFields = append(visibleFields, field)
			}
		}

		if maxFieldsCount < len(visibleFields) {
			maxFieldsCount = len(visibleFields)
		}
	}

	for i := 0; i < descriptor.Fields().Len(); i++ {
		field := descriptor.Fields().Get(i)
		fieldInfo, err := dynamicform.GetFieldInfo(field)
		if err != nil {
			return 0, xerrors.Errorf("Can't get field info for field: %v", field.FullName())
		}

		visible, err := getVisible(generator.propertyProvider, field, fieldInfo)
		if err != nil {
			return 0, xerrors.Errorf("can't get visible for '%v': %w", field.FullName(), err)
		}
		if !(visible || generator.ignoreVisible) || fieldInfo.NoTest() {
			continue
		}

		var childDescriptor protoreflect.MessageDescriptor
		if field.Message() != nil {
			childDescriptor = field.Message()
		} else if field.MapValue() != nil && field.MapValue().Message() != nil {
			childDescriptor = field.MapValue().Message()
		}

		if childDescriptor == nil {
			continue
		}

		maxChildFieldsCount, err := generator.getMaxOneOfVariantsCount(childDescriptor)
		if err != nil {
			return 0, xerrors.Errorf("Can't get max oneof variants for message '%v': %w", childDescriptor.FullName(), err)
		}
		if maxFieldsCount < maxChildFieldsCount {
			maxFieldsCount = maxChildFieldsCount
		}
	}

	return maxFieldsCount, nil
}

type protoValueFactory func() (protoreflect.Value, error)

func (generator *FormDataGenerator) createListValue(
	valueFactory protoValueFactory,
	field protoreflect.FieldDescriptor,
) (protoreflect.Value, error) {
	value, err := valueFactory()
	if err != nil {
		//nolint:descriptiveerrors
		return protoreflect.ValueOf(nil), err
	}
	list := value.List()

	// Add one element to list
	itemValueFactory := func() (protoreflect.Value, error) {
		return list.NewElement(), nil
	}
	itemValue, err := generator.createFieldValue(itemValueFactory, field, true)
	if err != nil {
		//nolint:descriptiveerrors
		return protoreflect.ValueOf(nil), err
	}
	list.Append(itemValue)

	return value, nil
}

func (generator *FormDataGenerator) createMapValue(
	valueFactory protoValueFactory,
	field protoreflect.FieldDescriptor,
) (protoreflect.Value, error) {
	value, err := valueFactory()
	if err != nil {
		//nolint:descriptiveerrors
		return protoreflect.ValueOf(nil), err
	}
	dict := value.Map() // dict coz map is reserved word

	// Add one element to map
	mapKeyFactory := func() (protoreflect.Value, error) {
		return protoreflect.ValueOf(nil), xerrors.Errorf("Can't use complex type for map key value")
	}
	mapKey, err := generator.createFieldValue(mapKeyFactory, field.MapKey(), false)
	if err != nil {
		//nolint:descriptiveerrors
		return protoreflect.ValueOf(nil), err
	}
	mapValueFactory := func() (protoreflect.Value, error) {
		return dict.NewValue(), nil
	}
	mapValue, err := generator.createFieldValue(mapValueFactory, field.MapValue(), false)
	if err != nil {
		//nolint:descriptiveerrors
		return protoreflect.ValueOf(nil), err
	}
	dict.Set(mapKey.MapKey(), mapValue)

	return value, nil
}

func (generator *FormDataGenerator) createMessageValue(valueFactory protoValueFactory) (protoreflect.Value, error) {
	value, err := valueFactory()
	if err != nil {
		//nolint:descriptiveerrors
		return protoreflect.ValueOf(nil), err
	}
	//nolint:descriptiveerrors
	return value, generator.fillMessage(value.Message())
}

func (generator *FormDataGenerator) createEnumValue(field protoreflect.FieldDescriptor) (protoreflect.Value, error) {
	enums := field.Enum().Values()
	visibleEnums := []protoreflect.EnumValueDescriptor{}
	for i := 0; i < enums.Len(); i++ {
		enum := enums.Get(i)
		enumInfo, err := dynamicform.GetEnumInfo(enum)
		if err != nil {
			//nolint:descriptiveerrors
			return protoreflect.ValueOf(nil), err
		}

		visible, err := getVisible(generator.propertyProvider, enum, enumInfo)
		if err != nil {
			return protoreflect.ValueOf(nil), xerrors.Errorf("can't get visible for '%v': %w", field.FullName(), err)
		}
		if (!visible && !generator.ignoreVisible) || enumInfo.NoTest() {
			continue
		}

		visibleEnums = append(visibleEnums, enum)
	}

	count := len(visibleEnums)
	if count == 0 {
		return protoreflect.ValueOf(nil), xerrors.Errorf("No visible in test enum values for field '%v', of type '%v'",
			field.FullName(), field.Enum().FullName())
	}

	index := int(generator.nextDummy() % uint64(count))
	if index == 0 && count > 1 { // Usually first value of the enum is 'none' value
		index = 1
	}

	enumValueDesc := visibleEnums[index]
	value := protoreflect.ValueOfEnum(enumValueDesc.Number())

	return value, nil
}

func (generator *FormDataGenerator) createBoolValue() (protoreflect.Value, error) {
	return protoreflect.ValueOfBool(generator.nextDummy()%2 == 0), nil
}

func (generator *FormDataGenerator) createInt64Value() (protoreflect.Value, error) {
	return protoreflect.ValueOfInt64(int64(generator.nextDummy())), nil
}

func (generator *FormDataGenerator) createDoubleValue() (protoreflect.Value, error) {
	return protoreflect.ValueOfFloat64(float64(generator.nextDummy()) / 100), nil
}

func (generator *FormDataGenerator) createStringValue(field protoreflect.FieldDescriptor) (protoreflect.Value, error) {
	info, err := dynamicform.GetFieldInfo(field)
	if err != nil {
		//nolint:descriptiveerrors
		return protoreflect.ValueOf(nil), err
	}
	switch info.TestValueType() {
	case protoopt.ValueType_VALUE_TYPE_UNSPECIFIED:
		str := strconv.FormatUint(generator.nextDummy(), 10)
		if int64(info.Maximum()) != 0 && len(str) > int(info.Maximum()) {
			return protoreflect.ValueOfString(str[:int(info.Maximum())]), nil
		}
		return protoreflect.ValueOfString(str), nil
	case protoopt.ValueType_VALUE_TYPE_INTERVAL:
		return protoreflect.ValueOfString(fmt.Sprintf("%vs", (generator.nextDummy()%59)+1)), nil
	case protoopt.ValueType_VALUE_TYPE_BYTES_SIZE:
		return protoreflect.ValueOfString(fmt.Sprintf("%v B", (generator.nextDummy()%1023)+1)), nil
	case protoopt.ValueType_VALUE_TYPE_JSON:
		return protoreflect.ValueOfString(fmt.Sprintf(`{"key_%v":%v}`, generator.nextDummy(), generator.nextDummy())), nil
	case protoopt.ValueType_VALUE_TYPE_JSON_COLUMNS:
		return protoreflect.ValueOfString(fmt.Sprintf(`[{"name":"column_%v","type":"INT32"}]`, generator.nextDummy())), nil
	case protoopt.ValueType_VALUE_TYPE_TABLE_NAME:
		return protoreflect.ValueOfString(fmt.Sprintf("schema_%v.table_%v", generator.nextDummy(), generator.nextDummy())), nil
	case protoopt.ValueType_VALUE_TYPE_JSON_YT_COLUMNS:
		return protoreflect.ValueOfString(fmt.Sprintf(`[{"name":"column_%v","type":"VT_INT32"}]`, generator.nextDummy())), nil
	case protoopt.ValueType_VALUE_TYPE_YT_CLUSTER:
		return protoreflect.ValueOfString("hahn"), nil
	default:
		return protoreflect.ValueOf(nil), xerrors.Errorf("Unknown value type '%v'", info.TestValueType())
	}
}

func (generator *FormDataGenerator) createBytesValue() (protoreflect.Value, error) {
	buf := generator.nextDummyBytes(42)
	return protoreflect.ValueOfBytes(buf), nil
}

func (generator *FormDataGenerator) createFieldValue(
	complexValueFactory protoValueFactory,
	field protoreflect.FieldDescriptor,
	skipList bool,
) (val protoreflect.Value, err error) {
	switch {
	case !skipList && field.IsList():
		//nolint:descriptiveerrors
		val, err = generator.createListValue(complexValueFactory, field)
	case field.IsMap():
		//nolint:descriptiveerrors
		val, err = generator.createMapValue(complexValueFactory, field)
	case field.Kind() == protoreflect.MessageKind:
		//nolint:descriptiveerrors
		val, err = generator.createMessageValue(complexValueFactory)
	case field.Kind() == protoreflect.EnumKind:
		//nolint:descriptiveerrors
		val, err = generator.createEnumValue(field)
	case field.Kind() == protoreflect.BoolKind:
		//nolint:descriptiveerrors
		val, err = generator.createBoolValue()
	case field.Kind() == protoreflect.Int64Kind:
		//nolint:descriptiveerrors
		val, err = generator.createInt64Value()
	case field.Kind() == protoreflect.DoubleKind:
		//nolint:descriptiveerrors
		val, err = generator.createDoubleValue()
	case field.Kind() == protoreflect.StringKind:
		//nolint:descriptiveerrors
		val, err = generator.createStringValue(field)
	case field.Kind() == protoreflect.BytesKind:
		//nolint:descriptiveerrors
		val, err = generator.createBytesValue()
	default:
		return protoreflect.ValueOf(nil), xerrors.Errorf(
			"Form field '%v' use restricted type, allowed types are: bool, int64, double, string, bytes, arrays, maps, messages, enums",
			field.FullName())
	}

	if err != nil {
		//nolint:descriptiveerrors
		return protoreflect.ValueOf(nil), xerrors.Errorf("Form field '%v': %w", field.FullName(), err)
	}

	return val, nil
}

func (generator *FormDataGenerator) fillField(
	message protoreflect.Message, field protoreflect.FieldDescriptor,
) error {
	info, err := dynamicform.GetFieldInfo(field)
	if err != nil {
		//nolint:descriptiveerrors
		return err
	}

	immutable := info.Immutable() || info.TestImmutable()

	if immutable {
		if value, ok := generator.immutableCache[field.FullName()]; ok {
			message.Set(field, value)
			return nil
		}
	}

	valueFactory := func() (protoreflect.Value, error) {
		return message.NewField(field), nil
	}
	value, err := generator.createFieldValue(valueFactory, field, false)
	if err != nil {
		//nolint:descriptiveerrors
		return err
	}
	if immutable {
		generator.immutableCache[field.FullName()] = value
	}

	message.Set(field, value)
	return nil
}

func (generator *FormDataGenerator) fillMessage(message protoreflect.Message) error {
	descriptor := message.Descriptor()

	oneofs := descriptor.Oneofs()
	for i := 0; i < oneofs.Len(); i++ {
		oneof := oneofs.Get(i)
		visibleFields := []protoreflect.FieldDescriptor{}
		for j := 0; j < oneof.Fields().Len(); j++ {
			field := oneof.Fields().Get(j)
			fieldInfo, err := dynamicform.GetFieldInfo(field)
			if err != nil {
				//nolint:descriptiveerrors
				return err
			}

			visible, err := getVisible(generator.propertyProvider, field, fieldInfo)
			if err != nil {
				return xerrors.Errorf("can't get visible for '%v': %w", field.FullName(), err)
			}
			if (visible || generator.ignoreVisible) && !fieldInfo.NoTest() {
				visibleFields = append(visibleFields, field)
			}
		}

		fieldIndex := generator.iteration % len(visibleFields)
		field := visibleFields[fieldIndex]
		if err := generator.fillField(message, field); err != nil {
			//nolint:descriptiveerrors
			return err
		}
	}

	for i := 0; i < descriptor.Fields().Len(); i++ {
		field := descriptor.Fields().Get(i)
		if field.ContainingOneof() != nil {
			continue
		}
		fieldInfo, err := dynamicform.GetFieldInfo(field)
		if err != nil {
			//nolint:descriptiveerrors
			return err
		}

		visible, err := getVisible(generator.propertyProvider, field, fieldInfo)
		if err != nil {
			return xerrors.Errorf("can't get visible for '%v': %w", field.FullName(), err)
		}
		if (!visible && !generator.ignoreVisible) || fieldInfo.NoTest() {
			continue
		}

		if err := generator.fillField(message, field); err != nil {
			//nolint:descriptiveerrors
			return err
		}
	}

	return nil
}

func (generator *FormDataGenerator) Next() (bool, error) {
	if generator.iteration >= generator.iterationsCount {
		return false, nil
	}

	generator.immutableCache = map[protoreflect.FullName]protoreflect.Value{}
	generator.iteration++
	return true, nil
}

func (generator *FormDataGenerator) Generate() (protoreflect.Message, error) {
	message := generator.typ.New()
	return message, generator.fillMessage(message)
}
