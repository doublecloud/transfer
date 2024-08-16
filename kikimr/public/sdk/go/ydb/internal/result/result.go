package result

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"math"
	"reflect"
	"time"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/internal"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

func Reset(s *Scanner, set *Ydb.ResultSet, columnNames ...string) {
	s.reset(set)
	s.setColumnIndexes(columnNames)
}

func Columns(s *Scanner, it func(name string, typ internal.T)) {
	s.columns(it)
}

type Scanner struct {
	set       *Ydb.ResultSet
	row       *Ydb.Value
	converter *rawConverter
	stack     scanStack
	nextRow   int
	nextItem  int

	setColumnIndex map[string]int

	columnIndexes           []int
	defaultValueForOptional bool
	err                     error
}

// Must not be exported.
func (s *Scanner) reset(set *Ydb.ResultSet) {
	s.set = set
	s.row = nil
	s.nextRow = 0
	s.nextItem = 0
	s.setColumnIndex = nil
	s.columnIndexes = nil
	s.defaultValueForOptional = true
	s.stack.reset()
	s.converter = &rawConverter{
		Scanner: s,
	}
}

func (s *Scanner) path() string {
	var buf bytes.Buffer
	_, _ = s.writePathTo(&buf)
	return buf.String()
}

func (s *Scanner) writePathTo(w io.Writer) (n int64, err error) {
	x := s.stack.current()
	st := x.name
	m, err := io.WriteString(w, st)
	if err != nil {
		return n, err
	}
	n += int64(m)
	return n, nil
}

func (s *Scanner) getType() ydb.Type {
	x := s.stack.current()
	if x.isEmpty() {
		return nil
	}
	return internal.TypeFromYDB(x.t)
}

func (s *Scanner) hasItems() bool {
	return s.err == nil && s.set != nil && s.row != nil
}

func (s *Scanner) seekItemByID(id int) {
	if !s.hasItems() || id >= len(s.set.Columns) {
		s.noValueError()
		return
	}
	col := s.set.Columns[id]
	s.stack.scanItem.name = col.Name
	s.stack.scanItem.t = col.Type
	s.stack.scanItem.v = s.row.Items[id]
}

func (s *Scanner) setColumnIndexes(columns []string) {
	if columns == nil {
		s.columnIndexes = nil
		return
	}
	s.indexSetColumns()
	s.columnIndexes = make([]int, 0, len(columns))
	for _, col := range columns {
		colIndex, ok := s.setColumnIndex[col]
		if !ok {
			s.noColumnError(col)
			return
		}
		s.columnIndexes = append(s.columnIndexes, colIndex)
	}
}

// HasNextRow reports whether result row may be advanced.
//
// It may be useful to call HasNextRow() instead of NextRow() to look ahead
// without advancing the result rows.
func (s *Scanner) HasNextRow() bool {
	return s.err == nil && s.set != nil && s.nextRow < len(s.set.Rows)
}

// NextRow selects next row in the current result set.
// It returns false if there are no more rows in the result set.
func (s *Scanner) NextRow() bool {
	if !s.HasNextRow() {
		return false
	}
	s.row = s.set.Rows[s.nextRow]
	s.nextRow++
	s.nextItem = 0
	s.stack.reset()

	return true
}

// ColumnCount returns number of columns in the current result set.
func (s *Scanner) ColumnCount() int {
	if s.set == nil {
		return 0
	}
	return len(s.set.Columns)
}

// RowCount returns number of rows in the result set.
func (s *Scanner) RowCount() int {
	if s.set == nil {
		return 0
	}
	return len(s.set.Rows)
}

// ItemCount returns number of items in the current row.
func (s *Scanner) ItemCount() int {
	if s.row == nil {
		return 0
	}
	return len(s.row.Items)
}

// columns allows to iterate over all columns of the current result set.
// Must not be exported.
func (s *Scanner) columns(it func(name string, typ internal.T)) {
	if s.set == nil {
		return
	}
	for _, m := range s.set.Columns {
		it(m.Name, internal.TypeFromYDB(m.Type))
	}
}

// ResultSetTruncated returns true if current result set has been truncated by server
func (s *Scanner) ResultSetTruncated() bool {
	if s.set == nil {
		s.errorf("there are no sets in the scanner")
		return false
	}
	return s.set.Truncated
}

// Err returns error caused Scanner to be broken.
func (s *Scanner) Err() error {
	return s.err
}

// Any returns any primitive or optional value.
// Currently, it may return one of these types:
//
//	bool
//	int8
//	uint8
//	int16
//	uint16
//	int32
//	uint32
//	int64
//	uint64
//	float32
//	float64
//	[]byte
//	string
//	[16]byte
func (s *Scanner) any() interface{} {
	x := s.stack.current()
	if s.err != nil || x.isEmpty() {
		return nil
	}

	if s.isNull() {
		return nil
	}

	if s.isCurrentTypeOptional() {
		s.unwrap()
		x = s.stack.current()
	}

	t := internal.TypeFromYDB(x.t)
	p, primitive := t.(internal.PrimitiveType)
	if !primitive {
		return nil
	}

	switch p {
	case internal.TypeBool:
		return s.bool()
	case internal.TypeInt8:
		return s.int8()
	case internal.TypeUint8:
		return s.uint8()
	case internal.TypeInt16:
		return s.int16()
	case internal.TypeUint16:
		return s.uint16()
	case internal.TypeInt32:
		return s.int32()
	case internal.TypeFloat:
		return s.float()
	case internal.TypeDouble:
		return s.double()
	case internal.TypeString:
		return string(s.bytes())
	case internal.TypeUUID:
		return s.uint128()
	case internal.TypeUint32:
		return s.uint32()
	case internal.TypeDate:
		return internal.UnmarshalDate(s.uint32())
	case internal.TypeDatetime:
		return internal.UnmarshalDatetime(s.uint32())
	case internal.TypeUint64:
		return s.uint64()
	case internal.TypeTimestamp:
		return internal.UnmarshalTimestamp(s.uint64())
	case internal.TypeInt64:
		return s.int64()
	case internal.TypeInterval:
		return internal.DurationFromMicroseconds(s.int64())
	case internal.TypeTzDate:
		src, err := internal.UnmarshalTzDate(s.text())
		if err != nil {
			s.errorf("scan row failed: %w", err)
		}
		return src
	case internal.TypeTzDatetime:
		src, err := internal.UnmarshalTzDatetime(s.text())
		if err != nil {
			s.errorf("scan row failed: %w", err)
		}
		return src
	case internal.TypeTzTimestamp:
		src, err := internal.UnmarshalTzTimestamp(s.text())
		if err != nil {
			s.errorf("scan row failed: %w", err)
		}
		return src
	case internal.TypeUTF8, internal.TypeDyNumber:
		return s.text()
	case
		internal.TypeYSON,
		internal.TypeJSON,
		internal.TypeJSONDocument:
		return []byte(s.text())
	default:
		s.errorf("ydb/table: unknown primitive type")
		return nil
	}
}

// Value returns current item under scan as ydb.Value type.
func (s *Scanner) value() ydb.Value {
	if s.err != nil {
		return nil
	}
	x := s.stack.current()
	return internal.ValueFromYDB(x.t, x.v)
}

// s.set must be initialized.
func (s *Scanner) indexSetColumns() {
	if s.set == nil {
		s.setColumnIndex = nil
		return
	}
	s.setColumnIndex = make(map[string]int, len(s.set.Columns))
	for i, m := range s.set.Columns {
		s.setColumnIndex[m.Name] = i
	}
}

func (s *Scanner) assertTypeOptional(typ *Ydb.Type) (t *Ydb.Type_OptionalType) {
	x := typ.Type
	if t, _ = x.(*Ydb.Type_OptionalType); t == nil {
		s.typeError(x, t)
	}
	return
}

func (s *Scanner) assertCurrentTypePrimitive(id Ydb.Type_PrimitiveTypeId) bool {
	p := s.assertTypePrimitive(s.stack.current().t)
	if p == nil {
		return false
	}
	if p.TypeId != id {
		s.primitiveTypeError(p.TypeId, id)
		return false
	}
	return true
}

func (s *Scanner) assertCurrentTypeOptionalPrimitive(id Ydb.Type_PrimitiveTypeId) bool {
	typ := s.stack.current().t
	if t, _ := typ.Type.(*Ydb.Type_OptionalType); t != nil {
		typ = t.OptionalType.Item
	}
	if typ == nil {
		return false
	}
	p := s.assertTypePrimitive(typ)
	if p == nil {
		return false
	}
	if p.TypeId != id {
		s.primitiveTypeError(p.TypeId, id)
		return false
	}
	return true
}

func (s *Scanner) isCurrentTypeOptional() bool {
	c := s.stack.current()
	return isOptional(c.t)
}

func (s *Scanner) errorf(f string, args ...interface{}) {
	if s.err != nil {
		return
	}
	s.err = fmt.Errorf(f, args...)
}

func (s *Scanner) typeError(act, exp interface{}) {
	s.errorf(
		"unexpected type during scan at %q %s: %s; want %s",
		s.Path(), s.Type(), nameIface(act), nameIface(exp),
	)
}

func (s *Scanner) valueTypeError(act, exp interface{}) {
	s.errorf(
		"unexpected value during scan at %q %s: %s; want %s",
		s.Path(), s.Type(), nameIface(act), nameIface(exp),
	)
}

func (s *Scanner) noValueError() {
	s.errorf(
		"no value at %q",
		s.path(),
	)
}

func (s *Scanner) noColumnError(name string) {
	s.errorf(
		"no column %q", name,
	)
}

func (s *Scanner) overflowError(i, n interface{}) {
	s.errorf("overflow error: %d overflows capacity of %t", i, n)
}

func (s *Scanner) primitiveTypeError(act, exp Ydb.Type_PrimitiveTypeId) {
	s.errorf(
		"unexpected type id at %q %s: %s; want %s",
		s.path(), s.getType(), act, exp,
	)
}

func (s *Scanner) isNull() bool {
	_, yes := s.stack.currentValue().(*Ydb.Value_NullFlagValue)
	return yes
}

// unwrap unwraps current item under scan interpreting it as Optional<T> type.
func (s *Scanner) unwrap() {
	if s.err != nil {
		return
	}

	t, _ := s.stack.currentType().(*Ydb.Type_OptionalType)
	if t == nil {
		return
	}

	if isOptional(t.OptionalType.Item) {
		s.stack.scanItem.v = s.unwrapValue()
	}
	s.stack.scanItem.t = t.OptionalType.Item
}

func (s *Scanner) unwrapValue() (v *Ydb.Value) {
	x, _ := s.stack.currentValue().(*Ydb.Value_NestedValue)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.NestedValue
}

func (s *Scanner) bool() (v bool) {
	x, _ := s.stack.currentValue().(*Ydb.Value_BoolValue)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.BoolValue
}
func (s *Scanner) int8() (v int8) {
	d := s.int32()
	if d < math.MinInt8 || math.MaxInt8 < d {
		s.overflowError(d, v)
		return
	}
	return int8(d)
}
func (s *Scanner) uint8() (v uint8) {
	d := s.uint32()
	if d > math.MaxUint8 {
		s.overflowError(d, v)
		return
	}
	return uint8(d)
}
func (s *Scanner) int16() (v int16) {
	d := s.int32()
	if d < math.MinInt16 || math.MaxInt16 < d {
		s.overflowError(d, v)
		return
	}
	return int16(d)
}
func (s *Scanner) uint16() (v uint16) {
	d := s.uint32()
	if d > math.MaxUint16 {
		s.overflowError(d, v)
		return
	}
	return uint16(d)
}
func (s *Scanner) int32() (v int32) {
	x, _ := s.stack.currentValue().(*Ydb.Value_Int32Value)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.Int32Value
}
func (s *Scanner) uint32() (v uint32) {
	x, _ := s.stack.currentValue().(*Ydb.Value_Uint32Value)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.Uint32Value
}
func (s *Scanner) int64() (v int64) {
	x, _ := s.stack.currentValue().(*Ydb.Value_Int64Value)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.Int64Value
}
func (s *Scanner) uint64() (v uint64) {
	x, _ := s.stack.currentValue().(*Ydb.Value_Uint64Value)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.Uint64Value
}
func (s *Scanner) float() (v float32) {
	x, _ := s.stack.currentValue().(*Ydb.Value_FloatValue)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.FloatValue
}
func (s *Scanner) double() (v float64) {
	x, _ := s.stack.currentValue().(*Ydb.Value_DoubleValue)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.DoubleValue
}
func (s *Scanner) bytes() (v []byte) {
	x, _ := s.stack.currentValue().(*Ydb.Value_BytesValue)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.BytesValue
}
func (s *Scanner) text() (v string) {
	x, _ := s.stack.currentValue().(*Ydb.Value_TextValue)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.TextValue
}
func (s *Scanner) low128() (v uint64) {
	x, _ := s.stack.currentValue().(*Ydb.Value_Low_128)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.Low_128
}
func (s *Scanner) uint128() (v [16]byte) {
	c := s.stack.current()
	if c.isEmpty() {
		s.errorf("TODO")
		return
	}
	lo := s.low128()
	hi := c.v.High_128
	return internal.BigEndianUint128(hi, lo)
}

func (s *Scanner) setTime(dst *time.Time) {
	switch t := s.stack.current().t.GetTypeId(); t {
	case Ydb.Type_DATE:
		*dst = internal.UnmarshalDate(s.uint32())
	case Ydb.Type_DATETIME:
		*dst = internal.UnmarshalDatetime(s.uint32())
	case Ydb.Type_TIMESTAMP:
		*dst = internal.UnmarshalTimestamp(s.uint64())
	case Ydb.Type_TZ_DATE:
		src, err := internal.UnmarshalTzDate(s.text())
		if err != nil {
			s.errorf("scan row failed: %w", err)
		}
		*dst = src
	case Ydb.Type_TZ_DATETIME:
		src, err := internal.UnmarshalTzDatetime(s.text())
		if err != nil {
			s.errorf("scan row failed: %w", err)
		}
		*dst = src
	case Ydb.Type_TZ_TIMESTAMP:
		src, err := internal.UnmarshalTzTimestamp(s.text())
		if err != nil {
			s.errorf("scan row failed: %w", err)
		}
		*dst = src
	default:
		s.errorf("scan row failed: incorrect source type %s", t)
	}
}

func (s *Scanner) setString(dst *string) {
	switch t := s.stack.current().t.GetTypeId(); t {
	case Ydb.Type_STRING:
		*dst = string(s.bytes())
	case Ydb.Type_UTF8:
		*dst = s.text()
	case Ydb.Type_DYNUMBER:
		*dst = s.text()
	default:
		s.errorf("scan row failed: incorrect source type %s", t)
	}
}

func (s *Scanner) setByte(dst *[]byte) {
	switch t := s.stack.current().t.GetTypeId(); t {
	case Ydb.Type_UUID:
		src := s.uint128()
		*dst = src[:]
	case Ydb.Type_YSON, Ydb.Type_JSON, Ydb.Type_JSON_DOCUMENT:
		*dst = []byte(s.text())
	case Ydb.Type_STRING:
		*dst = s.bytes()
	default:
		s.errorf("scan row failed: incorrect source type %s", t)
	}
}

func (s *Scanner) trySetByteArray(v interface{}, optional bool, def bool) bool {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	if rv.Kind() == reflect.Ptr {
		if !optional {
			return false
		}
		if s.isNull() {
			rv.Set(reflect.Zero(rv.Type()))
			return true
		}
		if rv.IsZero() {
			nv := reflect.New(rv.Type().Elem())
			rv.Set(nv)
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Array {
		return false
	}
	if rv.Type().Elem().Kind() != reflect.Uint8 {
		return false
	}
	if def {
		rv.Set(reflect.Zero(rv.Type()))
		return true
	}
	var dst []byte
	s.setByte(&dst)
	if rv.Len() != len(dst) {
		return false
	}
	reflect.Copy(rv, reflect.ValueOf(dst))
	return true
}

func (s *Scanner) scanRequired(value interface{}) {
	switch v := value.(type) {
	case *bool:
		*v = s.bool()
	case *int8:
		*v = s.int8()
	case *int16:
		*v = s.int16()
	case *int:
		*v = int(s.int32())
	case *int32:
		*v = s.int32()
	case *int64:
		*v = s.int64()
	case *uint8:
		*v = s.uint8()
	case *uint16:
		*v = s.uint16()
	case *uint32:
		*v = s.uint32()
	case *uint:
		*v = uint(s.uint32())
	case *uint64:
		*v = s.uint64()
	case *float32:
		*v = s.float()
	case *float64:
		*v = s.double()
	case *time.Time:
		s.setTime(v)
	case *time.Duration:
		*v = internal.DurationFromMicroseconds(s.int64())
	case *string:
		s.setString(v)
	case *[]byte:
		s.setByte(v)
	case *[16]byte:
		*v = s.uint128()
	case *interface{}:
		*v = s.any()
	case sql.Scanner:
		err := v.Scan(s.any())
		if err != nil {
			s.errorf("sql.Scanner error: %w", err)
		}
	case ydb.Scanner:
		err := v.UnmarshalYDB(s.converter)
		if err != nil {
			s.errorf("ydb.Scanner error: %w", err)
		}
	case *ydb.Value:
		*v = s.value()
	default:
		ok := s.trySetByteArray(v, false, false)
		if !ok {
			s.errorf("scan row failed: type %T is unknown", v)
		}
	}
}

func (s *Scanner) scanOptional(value interface{}) {
	if s.defaultValueForOptional {
		if s.isNull() {
			s.setDefaultValue(value)
		} else {
			s.unwrap()
			s.scanRequired(value)
		}
		return
	}
	switch v := value.(type) {
	case **bool:
		if s.isNull() {
			*v = nil
		} else {
			src := s.bool()
			*v = &src
		}
	case **int8:
		if s.isNull() {
			*v = nil
		} else {
			src := s.int8()
			*v = &src
		}
	case **int16:
		if s.isNull() {
			*v = nil
		} else {
			src := s.int16()
			*v = &src
		}
	case **int32:
		if s.isNull() {
			*v = nil
		} else {
			src := s.int32()
			*v = &src
		}
	case **int:
		if s.isNull() {
			*v = nil
		} else {
			src := int(s.int32())
			*v = &src
		}
	case **int64:
		if s.isNull() {
			*v = nil
		} else {
			src := s.int64()
			*v = &src
		}
	case **uint8:
		if s.isNull() {
			*v = nil
		} else {
			src := s.uint8()
			*v = &src
		}
	case **uint16:
		if s.isNull() {
			*v = nil
		} else {
			src := s.uint16()
			*v = &src
		}
	case **uint32:
		if s.isNull() {
			*v = nil
		} else {
			src := s.uint32()
			*v = &src
		}
	case **uint:
		if s.isNull() {
			*v = nil
		} else {
			src := uint(s.uint32())
			*v = &src
		}
	case **uint64:
		if s.isNull() {
			*v = nil
		} else {
			src := s.uint64()
			*v = &src
		}
	case **float32:
		if s.isNull() {
			*v = nil
		} else {
			src := s.float()
			*v = &src
		}
	case **float64:
		if s.isNull() {
			*v = nil
		} else {
			src := s.double()
			*v = &src
		}
	case **time.Time:
		if s.isNull() {
			*v = nil
		} else {
			s.unwrap()
			var src time.Time
			s.setTime(&src)
			*v = &src
		}
	case **time.Duration:
		if s.isNull() {
			*v = nil
		} else {
			src := internal.DurationFromMicroseconds(s.int64())
			*v = &src
		}
	case **string:
		if s.isNull() {
			*v = nil
		} else {
			s.unwrap()
			var src string
			s.setString(&src)
			*v = &src
		}
	case **[]byte:
		if s.isNull() {
			*v = nil
		} else {
			s.unwrap()
			var src []byte
			s.setByte(&src)
			*v = &src
		}
	case **[16]byte:
		if s.isNull() {
			*v = nil
		} else {
			src := s.uint128()
			*v = &src
		}
	case **interface{}:
		if s.isNull() {
			*v = nil
		} else {
			s.unwrap()
			src := s.any()
			*v = &src
		}
	case sql.Scanner:
		s.unwrap()
		err := v.Scan(s.any())
		if err != nil {
			s.errorf("sql.Scanner error: %w", err)
		}
	case ydb.Scanner:
		err := v.UnmarshalYDB(s.converter)
		if err != nil {
			s.errorf("ydb.Scanner error: %w", err)
		}
	default:
		s.unwrap()
		ok := s.trySetByteArray(v, true, false)
		if !ok {
			rv := reflect.TypeOf(v)
			if rv.Kind() == reflect.Ptr && rv.Elem().Kind() == reflect.Ptr {
				s.errorf("scan row failed: type %T is unknown", v)
			} else {
				s.errorf("scan row failed: type %T is not optional! use double pointer or sql.Scanner.", v)
			}
		}
	}
}

func (s *Scanner) setDefaultValue(dst interface{}) {
	switch v := dst.(type) {
	case *bool:
		*v = false
	case *int8:
		*v = 0
	case *int16:
		*v = 0
	case *int32:
		*v = 0
	case *int64:
		*v = 0
	case *uint8:
		*v = 0
	case *uint16:
		*v = 0
	case *uint32:
		*v = 0
	case *uint64:
		*v = 0
	case *float32:
		*v = 0
	case *float64:
		*v = 0
	case *time.Time:
		*v = time.Time{}
	case *time.Duration:
		*v = 0
	case *string:
		*v = ""
	case *[]byte:
		*v = nil
	case *[16]byte:
		*v = [16]byte{}
	case *interface{}:
		*v = nil
	case sql.Scanner:
		err := v.Scan(nil)
		if err != nil {
			s.errorf("sql.Scanner error: %w", err)
		}
	case ydb.Scanner:
		err := v.UnmarshalYDB(s.converter)
		if err != nil {
			s.errorf("ydb.Scanner error: %w", err)
		}
	case *ydb.Value:
		*v = s.value()
	default:
		ok := s.trySetByteArray(v, false, true)
		if !ok {
			s.errorf("scan row failed: type %T is unknown", v)
		}
	}
}

// Deprecated: Use Scan and implement ydb.Scanner
func (s *Scanner) ScanRaw(row ydb.CustomScanner) error {
	return row.UnmarshalYDB(s.converter)
}

// ScanWithDefaults scan with default type values.
// Nil values applied as default value type
// Input params - pointers to types.
func (s *Scanner) ScanWithDefaults(values ...interface{}) error {
	s.defaultValueForOptional = true
	return s.scan(values)
}

// Scan values.
// Input params - pointers to types:
//
//	bool
//	int8
//	uint8
//	int16
//	uint16
//	int32
//	uint32
//	int64
//	uint64
//	float32
//	float64
//	[]byte
//	[16]byte
//	string
//	time.Time
//	time.Duration
//	ydb.Value
//
// For custom types implement sql.Scanner interface.
// For optional type use double pointer construction.
// For unknown types use interface type.
// Supported scanning byte arrays of various length.
// For complex yql types: Dict, List, Tuple and own specific scanning logic implement ydb.Scanner with UnmarshalYDB method
// See examples for more detailed information.
// Output param - Scanner error
func (s *Scanner) Scan(values ...interface{}) error {
	s.defaultValueForOptional = false
	return s.scan(values)
}

func (s *Scanner) scan(values []interface{}) error {
	if s.err != nil {
		return s.err
	}
	if s.columnIndexes != nil {
		if len(s.columnIndexes) != len(values) {
			s.errorf("scan row failed: count of values and column are different")
			return s.err
		}
	}
	if s.ColumnCount() < len(values) {
		s.errorf("scan row failed: count of columns less then values")
		return s.err
	}
	if s.nextItem != 0 {
		panic("scan row failed: double scan per row")
	}
	for i, value := range values {
		if s.columnIndexes == nil {
			s.seekItemByID(i)
		} else {
			s.seekItemByID(s.columnIndexes[i])
		}
		if s.err != nil {
			return s.err
		}
		if s.isCurrentTypeOptional() {
			s.scanOptional(value)
		} else {
			s.scanRequired(value)
		}
	}
	s.nextItem += len(values)
	return s.err
}

const tinyStack = 8

var emptyItem item

type item struct {
	name string
	i    int // Index in listing types.
	t    *Ydb.Type
	v    *Ydb.Value
}

func (x item) isEmpty() bool {
	return x.v == nil
}

type scanStack struct {
	v        []item
	p        int8
	scanItem item
}

func (s *scanStack) size() int {
	if !s.scanItem.isEmpty() {
		s.set(s.scanItem)
	}
	return int(s.p) + 1
}

func (s *scanStack) get(i int) item {
	return s.v[i]
}

func (s *scanStack) reset() {
	s.scanItem = emptyItem
	s.p = 0
}

func (s *scanStack) enter() {
	// support compatibility
	if !s.scanItem.isEmpty() {
		s.set(s.scanItem)
	}
	s.scanItem = emptyItem
	s.p++
}

func (s *scanStack) leave() {
	s.set(emptyItem)
	if s.p > 0 {
		s.p--
	}
}

func (s *scanStack) set(v item) {
	if int(s.p) == len(s.v) {
		s.v = append(s.v, v)
	} else {
		s.v[s.p] = v
	}
}

func (s *scanStack) parent() item {
	if s.p == 0 {
		return emptyItem
	}
	return s.v[s.p-1]
}

func (s *scanStack) current() item {
	if !s.scanItem.isEmpty() {
		return s.scanItem
	}
	if s.v == nil {
		return emptyItem
	}
	return s.v[s.p]
}

func (s *scanStack) currentValue() interface{} {
	if v := s.current().v; v != nil {
		return v.Value
	}
	return nil
}

func (s *scanStack) currentType() interface{} {
	if t := s.current().t; t != nil {
		return t.Type
	}
	return nil
}

func isOptional(typ *Ydb.Type) bool {
	if typ == nil {
		return false
	}
	_, yes := typ.Type.(*Ydb.Type_OptionalType)
	return yes
}
