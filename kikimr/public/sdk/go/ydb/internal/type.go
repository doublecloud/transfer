package internal

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

type T interface {
	toYDB() *Ydb.Type
	toString(*bytes.Buffer)
	equal(T) bool
}

func TypeToYDB(t T) *Ydb.Type {
	return t.toYDB()
}

func WriteTypeStringTo(buf *bytes.Buffer, t T) {
	t.toString(buf)
}

func TypesEqual(a, b T) bool {
	return a.equal(b)
}

func TypeFromYDB(x *Ydb.Type) T {
	switch v := x.Type.(type) {
	case *Ydb.Type_TypeId:
		return primitiveTypeFromYDB(v.TypeId)

	case *Ydb.Type_OptionalType:
		return OptionalType{TypeFromYDB(v.OptionalType.Item)}

	case *Ydb.Type_ListType:
		return ListType{TypeFromYDB(v.ListType.Item)}

	case *Ydb.Type_DecimalType:
		d := v.DecimalType
		return DecimalType{d.Precision, d.Scale}

	case *Ydb.Type_TupleType:
		t := v.TupleType
		return TupleType{TypesFromYDB(t.Elements)}

	case *Ydb.Type_StructType:
		s := v.StructType
		return StructType{StructFields(s.Members)}

	case *Ydb.Type_DictType:
		d := v.DictType
		return DictType{
			Key:     TypeFromYDB(d.Key),
			Payload: TypeFromYDB(d.Payload),
		}

	case *Ydb.Type_VariantType:
		t := v.VariantType
		switch x := t.Type.(type) {
		case *Ydb.VariantType_TupleItems:
			return VariantType{
				T: TupleType{TypesFromYDB(x.TupleItems.Elements)},
			}
		case *Ydb.VariantType_StructItems:
			return VariantType{
				S: StructType{StructFields(x.StructItems.Members)},
			}
		default:
			panic(fmt.Sprintf("ydb: unknown variant type '%+v'", x))
		}

	case *Ydb.Type_VoidType:
		return VoidType{}

	case *Ydb.Type_NullType:
		return NullType{}

	default:
		panic(fmt.Sprintf("ydb: unknown type '%+v'", x))
	}
}

func StructFields(ms []*Ydb.StructMember) []StructField {
	fs := make([]StructField, len(ms))
	for i, m := range ms {
		fs[i] = StructField{
			Name: m.Name,
			Type: TypeFromYDB(m.Type),
		}
	}
	return fs
}

func TypesFromYDB(es []*Ydb.Type) []T {
	ts := make([]T, len(es))
	for i, el := range es {
		ts[i] = TypeFromYDB(el)
	}
	return ts
}

func primitiveTypeFromYDB(t Ydb.Type_PrimitiveTypeId) T {
	switch t {
	case Ydb.Type_BOOL:
		return TypeBool
	case Ydb.Type_INT8:
		return TypeInt8
	case Ydb.Type_UINT8:
		return TypeUint8
	case Ydb.Type_INT16:
		return TypeInt16
	case Ydb.Type_UINT16:
		return TypeUint16
	case Ydb.Type_INT32:
		return TypeInt32
	case Ydb.Type_UINT32:
		return TypeUint32
	case Ydb.Type_INT64:
		return TypeInt64
	case Ydb.Type_UINT64:
		return TypeUint64
	case Ydb.Type_FLOAT:
		return TypeFloat
	case Ydb.Type_DOUBLE:
		return TypeDouble
	case Ydb.Type_DATE:
		return TypeDate
	case Ydb.Type_DATETIME:
		return TypeDatetime
	case Ydb.Type_TIMESTAMP:
		return TypeTimestamp
	case Ydb.Type_INTERVAL:
		return TypeInterval
	case Ydb.Type_TZ_DATE:
		return TypeTzDate
	case Ydb.Type_TZ_DATETIME:
		return TypeTzDatetime
	case Ydb.Type_TZ_TIMESTAMP:
		return TypeTzTimestamp
	case Ydb.Type_STRING:
		return TypeString
	case Ydb.Type_UTF8:
		return TypeUTF8
	case Ydb.Type_YSON:
		return TypeYSON
	case Ydb.Type_JSON:
		return TypeJSON
	case Ydb.Type_UUID:
		return TypeUUID
	case Ydb.Type_JSON_DOCUMENT:
		return TypeJSONDocument
	case Ydb.Type_DYNUMBER:
		return TypeDyNumber
	default:
		panic("ydb: unexpected type")
	}
}

// TODO prepare toYDB() calls in constructors as an optimization.

type ListType struct {
	T T
}

func (l ListType) String() string {
	var buf bytes.Buffer
	l.toString(&buf)
	return buf.String()
}

func (l ListType) equal(t T) bool {
	x, ok := t.(ListType)
	if !ok {
		return false
	}
	return l.T.equal(x.T)
}

func (l ListType) toString(buf *bytes.Buffer) {
	buf.WriteString("List<")
	l.T.toString(buf)
	buf.WriteString(">")
}

func (l ListType) toYDB() *Ydb.Type {
	if x, ok := l.T.(PrimitiveType); ok {
		return listPrimitive[x]
	}
	return &Ydb.Type{
		Type: &Ydb.Type_ListType{
			ListType: &Ydb.ListType{
				Item: l.T.toYDB(),
			},
		},
	}
}

type TupleType struct {
	Elems []T
}

func (t TupleType) Empty() bool {
	return len(t.Elems) == 0
}

func (t TupleType) String() string {
	var buf bytes.Buffer
	t.toString(&buf)
	return buf.String()
}

func (t TupleType) equal(x T) bool {
	v, ok := x.(TupleType)
	if !ok {
		return false
	}
	if len(t.Elems) != len(v.Elems) {
		return false
	}
	for i, elem := range t.Elems {
		if !elem.equal(v.Elems[i]) {
			return false
		}
	}
	return true
}

func (t TupleType) toString(buf *bytes.Buffer) {
	buf.WriteString("Tuple<")
	for i, t := range t.Elems {
		if i > 0 {
			buf.WriteByte(',')
		}
		t.toString(buf)
	}
	buf.WriteByte('>')
}

func (t TupleType) toYDB() *Ydb.Type {
	return &Ydb.Type{
		Type: &Ydb.Type_TupleType{
			TupleType: &Ydb.TupleType{
				Elements: t.Elements(),
			},
		},
	}
}

func (t TupleType) Elements() []*Ydb.Type {
	es := make([]*Ydb.Type, len(t.Elems))
	for i, t := range t.Elems {
		es[i] = t.toYDB()
	}
	return es
}

type StructField struct {
	Name string
	Type T
}

type StructType struct {
	Fields []StructField
}

func (s StructType) Empty() bool {
	return len(s.Fields) == 0
}

func (s StructType) String() string {
	var buf bytes.Buffer
	s.toString(&buf)
	return buf.String()
}

func (s StructType) equal(t T) bool {
	v, ok := t.(StructType)
	if !ok {
		return false
	}
	if len(s.Fields) != len(v.Fields) {
		return false
	}
	for i, sf := range s.Fields {
		vf := v.Fields[i]
		if sf.Name != vf.Name {
			return false
		}
		if !sf.Type.equal(vf.Type) {
			return false
		}
	}
	return true
}

func (s StructType) toString(buf *bytes.Buffer) {
	buf.WriteString("Struct<")
	for i, f := range s.Fields {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(f.Name)
		buf.WriteByte(':')
		f.Type.toString(buf)
	}
	buf.WriteByte('>')
}

func (s StructType) toYDB() *Ydb.Type {
	return &Ydb.Type{
		Type: &Ydb.Type_StructType{
			StructType: &Ydb.StructType{
				Members: s.Members(),
			},
		},
	}
}

func (s StructType) Members() []*Ydb.StructMember {
	ms := make([]*Ydb.StructMember, len(s.Fields))
	for i, f := range s.Fields {
		ms[i] = &Ydb.StructMember{
			Name: f.Name,
			Type: f.Type.toYDB(),
		}
	}
	return ms
}

type DictType struct {
	Key     T
	Payload T
}

func Dict(key, payload T) T {
	return DictType{
		Key:     key,
		Payload: payload,
	}
}

func (d DictType) String() string {
	var buf bytes.Buffer
	d.toString(&buf)
	return buf.String()
}

func (d DictType) equal(t T) bool {
	v, ok := t.(DictType)
	if !ok {
		return false
	}
	if !d.Key.equal(v.Key) {
		return false
	}
	if !d.Payload.equal(v.Payload) {
		return false
	}
	return true
}

func (d DictType) toString(buf *bytes.Buffer) {
	buf.WriteString("Dict<")
	d.Key.toString(buf)
	buf.WriteByte(',')
	d.Payload.toString(buf)
	buf.WriteByte('>')
}

func (d DictType) toYDB() *Ydb.Type {
	return &Ydb.Type{
		Type: &Ydb.Type_DictType{
			DictType: &Ydb.DictType{
				Key:     d.Key.toYDB(),
				Payload: d.Payload.toYDB(),
			},
		},
	}
}

type VariantType struct {
	S StructType
	T TupleType
}

func (v VariantType) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (v VariantType) at(i int) (T, bool) {
	if v.S.Empty() {
		if len(v.T.Elems) <= i {
			return nil, false
		}
		return v.T.Elems[i], true
	}
	if len(v.S.Fields) <= i {
		return nil, false
	}
	return v.S.Fields[i].Type, true
}

func (v VariantType) equal(t T) bool {
	x, ok := t.(VariantType)
	if !ok {
		return false
	}
	if v.S.Empty() {
		return v.T.equal(x.T)
	}
	return v.S.equal(x.S)
}

func (v VariantType) toString(buf *bytes.Buffer) {
	buf.WriteString("Variant<")
	if v.S.Empty() {
		v.T.toString(buf)
	} else {
		v.S.toString(buf)
	}
	buf.WriteString(">")
}

func (v VariantType) toYDB() *Ydb.Type {
	vt := new(Ydb.VariantType)
	if v.S.Empty() { // StructItems.
		vt.Type = &Ydb.VariantType_TupleItems{
			TupleItems: &Ydb.TupleType{
				Elements: v.T.Elements(),
			},
		}
	} else { // TupleItems.
		vt.Type = &Ydb.VariantType_StructItems{
			StructItems: &Ydb.StructType{
				Members: v.S.Members(),
			},
		}
	}
	return &Ydb.Type{
		Type: &Ydb.Type_VariantType{
			VariantType: vt,
		},
	}
}

type VoidType struct{}

func (v VoidType) toYDB() *Ydb.Type {
	return void
}

func (v VoidType) String() string {
	return "Void"
}

func (v VoidType) equal(t T) bool {
	_, ok := t.(VoidType)
	return ok
}

func (v VoidType) toString(buf *bytes.Buffer) {
	buf.WriteString(v.String())
}

var (
	void = &Ydb.Type{
		Type: &Ydb.Type_VoidType{},
	}
	optionalVoid = (OptionalType{VoidType{}}).toYDB()
)

type NullType struct{}

func (v NullType) toYDB() *Ydb.Type {
	return null
}

func (v NullType) String() string {
	return "Null"
}

func (v NullType) equal(t T) bool {
	_, ok := t.(NullType)
	return ok
}

func (v NullType) toString(buf *bytes.Buffer) {
	buf.WriteString(v.String())
}

var (
	null = &Ydb.Type{
		Type: &Ydb.Type_NullType{},
	}
	optionalNull = (OptionalType{NullType{}}).toYDB()
)

type OptionalType struct {
	T T
}

func (opt OptionalType) String() string {
	var buf bytes.Buffer
	opt.toString(&buf)
	return buf.String()
}

func (opt OptionalType) equal(t T) bool {
	v, ok := t.(OptionalType)
	if !ok {
		return false
	}
	return opt.T.equal(v.T)
}

func (opt OptionalType) toString(buf *bytes.Buffer) {
	buf.WriteString("Optional<")
	opt.T.toString(buf)
	buf.WriteString(">")
}

func (opt OptionalType) toYDB() *Ydb.Type {
	if x, ok := opt.T.(PrimitiveType); ok {
		return optionalPrimitive[x]
	}
	return &Ydb.Type{
		Type: &Ydb.Type_OptionalType{
			OptionalType: &Ydb.OptionalType{
				Item: opt.T.toYDB(),
			},
		},
	}
}

type DecimalType struct {
	Precision uint32
	Scale     uint32
}

func (d DecimalType) String() string {
	var buf bytes.Buffer
	d.toString(&buf)
	return buf.String()
}

func (d DecimalType) equal(t T) bool {
	v, ok := t.(DecimalType)
	if !ok {
		return false
	}
	return d == v
}

func (d DecimalType) toString(buf *bytes.Buffer) {
	buf.WriteString("Decimal(")
	buf.WriteString(strconv.FormatUint(uint64(d.Precision), 10))
	buf.WriteByte(',')
	buf.WriteString(strconv.FormatUint(uint64(d.Scale), 10))
	buf.WriteString(")")
}

func (d DecimalType) toYDB() *Ydb.Type {
	return &Ydb.Type{
		Type: &Ydb.Type_DecimalType{
			DecimalType: &Ydb.DecimalType{
				Precision: d.Precision,
				Scale:     d.Scale,
			},
		},
	}
}

type PrimitiveType int

func (t PrimitiveType) String() string {
	if int(t) < len(primitiveString) {
		return primitiveString[t]
	}
	return "Unknown"
}

func (t PrimitiveType) equal(x T) bool {
	v, ok := x.(PrimitiveType)
	return ok && v == t
}

func (t PrimitiveType) toYDB() *Ydb.Type {
	i := int(t)
	if 0 < i && i < len(primitive) {
		return primitive[i]
	}
	panic("ydb: unexpected primitive type")
}

func (t PrimitiveType) toString(buf *bytes.Buffer) {
	buf.WriteString(t.String())
}

// Primitive TypesFromYDB known by YDB.
const (
	TypeUnknown PrimitiveType = iota
	TypeBool
	TypeInt8
	TypeUint8
	TypeInt16
	TypeUint16
	TypeInt32
	TypeUint32
	TypeInt64
	TypeUint64
	TypeFloat
	TypeDouble
	TypeDate
	TypeDatetime
	TypeTimestamp
	TypeInterval
	TypeTzDate
	TypeTzDatetime
	TypeTzTimestamp
	TypeString
	TypeUTF8
	TypeYSON
	TypeJSON
	TypeUUID
	TypeJSONDocument
	TypeDyNumber
)

var primitiveString = [...]string{
	TypeUnknown:      "<unknown>",
	TypeBool:         "Bool",
	TypeInt8:         "Int8",
	TypeUint8:        "Uint8",
	TypeInt16:        "Int16",
	TypeUint16:       "Uint16",
	TypeInt32:        "Int32",
	TypeUint32:       "Uint32",
	TypeInt64:        "Int64",
	TypeUint64:       "Uint64",
	TypeFloat:        "Float",
	TypeDouble:       "Double",
	TypeDate:         "Date",
	TypeDatetime:     "Datetime",
	TypeTimestamp:    "Timestamp",
	TypeInterval:     "Interval",
	TypeTzDate:       "TzDate",
	TypeTzDatetime:   "TzDatetime",
	TypeTzTimestamp:  "TzTimestamp",
	TypeString:       "String",
	TypeUTF8:         "Utf8",
	TypeYSON:         "Yson",
	TypeJSON:         "Json",
	TypeUUID:         "Uuid",
	TypeJSONDocument: "JsonDocument",
	TypeDyNumber:     "DyNumber",
}

var primitive = [...]*Ydb.Type{
	TypeBool:         &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_BOOL}},
	TypeInt8:         &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT8}},
	TypeUint8:        &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT8}},
	TypeInt16:        &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT16}},
	TypeUint16:       &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT16}},
	TypeInt32:        &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32}},
	TypeUint32:       &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT32}},
	TypeInt64:        &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT64}},
	TypeUint64:       &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT64}},
	TypeFloat:        &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_FLOAT}},
	TypeDouble:       &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DOUBLE}},
	TypeDate:         &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DATE}},
	TypeDatetime:     &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DATETIME}},
	TypeTimestamp:    &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_TIMESTAMP}},
	TypeInterval:     &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INTERVAL}},
	TypeTzDate:       &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_TZ_DATE}},
	TypeTzDatetime:   &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_TZ_DATETIME}},
	TypeTzTimestamp:  &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_TZ_TIMESTAMP}},
	TypeString:       &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_STRING}},
	TypeUTF8:         &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UTF8}},
	TypeYSON:         &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_YSON}},
	TypeJSON:         &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_JSON}},
	TypeUUID:         &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UUID}},
	TypeJSONDocument: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_JSON_DOCUMENT}},
	TypeDyNumber:     &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DYNUMBER}},
}

var optionalPrimitive = [...]*Ydb.Type{
	TypeBool:         &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: primitive[TypeBool]}}},
	TypeInt8:         &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: primitive[TypeInt8]}}},
	TypeUint8:        &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: primitive[TypeUint8]}}},
	TypeInt16:        &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: primitive[TypeInt16]}}},
	TypeUint16:       &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: primitive[TypeUint16]}}},
	TypeInt32:        &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: primitive[TypeInt32]}}},
	TypeUint32:       &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: primitive[TypeUint32]}}},
	TypeInt64:        &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: primitive[TypeInt64]}}},
	TypeUint64:       &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: primitive[TypeUint64]}}},
	TypeFloat:        &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: primitive[TypeFloat]}}},
	TypeDouble:       &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: primitive[TypeDouble]}}},
	TypeDate:         &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: primitive[TypeDate]}}},
	TypeDatetime:     &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: primitive[TypeDatetime]}}},
	TypeTimestamp:    &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: primitive[TypeTimestamp]}}},
	TypeInterval:     &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: primitive[TypeInterval]}}},
	TypeTzDate:       &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: primitive[TypeTzDate]}}},
	TypeTzDatetime:   &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: primitive[TypeTzDatetime]}}},
	TypeTzTimestamp:  &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: primitive[TypeTzTimestamp]}}},
	TypeString:       &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: primitive[TypeString]}}},
	TypeUTF8:         &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: primitive[TypeUTF8]}}},
	TypeYSON:         &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: primitive[TypeYSON]}}},
	TypeJSON:         &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: primitive[TypeJSON]}}},
	TypeUUID:         &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: primitive[TypeUUID]}}},
	TypeJSONDocument: &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: primitive[TypeJSONDocument]}}},
	TypeDyNumber:     &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: primitive[TypeDyNumber]}}},
}

var listPrimitive = [...]*Ydb.Type{
	TypeBool:         &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: primitive[TypeBool]}}},
	TypeInt8:         &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: primitive[TypeInt8]}}},
	TypeUint8:        &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: primitive[TypeUint8]}}},
	TypeInt16:        &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: primitive[TypeInt16]}}},
	TypeUint16:       &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: primitive[TypeUint16]}}},
	TypeInt32:        &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: primitive[TypeInt32]}}},
	TypeUint32:       &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: primitive[TypeUint32]}}},
	TypeInt64:        &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: primitive[TypeInt64]}}},
	TypeUint64:       &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: primitive[TypeUint64]}}},
	TypeFloat:        &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: primitive[TypeFloat]}}},
	TypeDouble:       &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: primitive[TypeDouble]}}},
	TypeDate:         &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: primitive[TypeDate]}}},
	TypeDatetime:     &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: primitive[TypeDatetime]}}},
	TypeTimestamp:    &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: primitive[TypeTimestamp]}}},
	TypeInterval:     &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: primitive[TypeInterval]}}},
	TypeTzDate:       &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: primitive[TypeTzDate]}}},
	TypeTzDatetime:   &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: primitive[TypeTzDatetime]}}},
	TypeTzTimestamp:  &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: primitive[TypeTzTimestamp]}}},
	TypeString:       &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: primitive[TypeString]}}},
	TypeUTF8:         &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: primitive[TypeUTF8]}}},
	TypeYSON:         &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: primitive[TypeYSON]}}},
	TypeJSON:         &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: primitive[TypeJSON]}}},
	TypeUUID:         &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: primitive[TypeUUID]}}},
	TypeJSONDocument: &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: primitive[TypeJSONDocument]}}},
	TypeDyNumber:     &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: primitive[TypeDyNumber]}}},
}
