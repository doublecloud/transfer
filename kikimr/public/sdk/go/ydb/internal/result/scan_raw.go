package result

import (
	"bytes"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/internal"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

type rawConverter struct {
	*Scanner
}

func (s *rawConverter) Date() (v time.Time) {
	s.unwrap()
	return internal.UnmarshalDate(s.uint32())
}
func (s *rawConverter) Datetime() (v time.Time) {
	s.unwrap()
	return internal.UnmarshalDatetime(s.uint32())
}
func (s *rawConverter) Timestamp() (v time.Time) {
	s.unwrap()
	return internal.UnmarshalTimestamp(s.uint64())
}
func (s *rawConverter) Interval() (v time.Duration) {
	s.unwrap()
	return internal.DurationFromMicroseconds(s.int64())
}
func (s *rawConverter) TzDate() (v time.Time) {
	s.unwrap()
	if s.isNull() {
		return
	}
	src, err := internal.UnmarshalTzDate(s.text())
	if err != nil {
		s.errorf("scan raw failed: %w", err)
	}
	return src
}
func (s *rawConverter) TzDatetime() (v time.Time) {
	s.unwrap()
	if s.isNull() {
		return
	}
	src, err := internal.UnmarshalTzDatetime(s.text())
	if err != nil {
		s.errorf("scan raw failed: %w", err)
	}
	return src
}
func (s *rawConverter) TzTimestamp() (v time.Time) {
	s.unwrap()
	if s.isNull() {
		return
	}
	src, err := internal.UnmarshalTzTimestamp(s.text())
	if err != nil {
		s.errorf("scan raw failed: %w", err)
	}
	return src
}
func (s *rawConverter) String() (v string) {
	s.unwrap()
	return string(s.bytes())
}
func (s *rawConverter) YSON() (v []byte) {
	s.unwrap()
	return []byte(s.text())
}
func (s *rawConverter) JSON() (v []byte) {
	s.unwrap()
	return []byte(s.text())
}
func (s *rawConverter) JSONDocument() (v []byte) {
	s.unwrap()
	return []byte(s.text())
}

func (s *rawConverter) Any() interface{} {
	return s.any()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) HasItems() bool {
	return s.err == nil && s.set != nil && s.row != nil
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) HasNextItem() bool {
	return s.HasItems() && s.nextItem < len(s.row.Items)
}

// NextItem selects next item to parse in the current row.
// It returns false if there are no more items in the row.
//
// Note that NextItem() differs from NextRow() and NextSet() – if it return
// false it fails the Result such that no further operations may be processed.
// That is, res.Err() becomes non-nil.
// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) NextItem() (ok bool) {
	if !s.HasNextItem() {
		s.noValueError()
		return false
	}

	i := s.nextItem
	s.nextItem = i + 1

	s.stack.reset()
	col := s.set.Columns[i]
	s.stack.set(item{
		name: col.Name,
		i:    i,
		t:    col.Type,
		v:    s.row.Items[i],
	})

	return true
}

// SeekItem finds the column with given name in the result set and selects
// appropriate item to parse in the current row.
// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) SeekItem(name string) bool {
	if !s.HasItems() {
		s.noValueError()
		return false
	}
	if s.setColumnIndex == nil {
		s.indexSetColumns()
	}
	i, ok := s.setColumnIndex[name]
	if !ok {
		s.noColumnError(name)
		return false
	}
	s.nextItem = i + 1

	s.stack.reset()
	col := s.set.Columns[i]
	s.stack.set(item{
		name: col.Name,
		i:    i,
		t:    col.Type,
		v:    s.row.Items[i],
	})

	return true
}

// Deprecated: remove at next major release
func (s *Scanner) Path() string {
	var buf bytes.Buffer
	_, _ = s.WritePathTo(&buf)
	return buf.String()
}

// Deprecated: remove at next major release
func (s *Scanner) WritePathTo(w io.Writer) (n int64, err error) {
	for sp := 0; sp < s.stack.size(); sp++ {
		if sp > 0 {
			m, err := io.WriteString(w, ".")
			if err != nil {
				return n, err
			}
			n += int64(m)
		}
		x := s.stack.get(sp)
		s := x.name
		if s == "" {
			s = strconv.Itoa(x.i)
		}
		m, err := io.WriteString(w, s)
		if err != nil {
			return n, err
		}
		n += int64(m)
	}
	return n, nil
}

// Deprecated: remove at next major release
func (s *Scanner) Type() ydb.Type {
	x := s.stack.current()
	if x.isEmpty() {
		return nil
	}
	return internal.TypeFromYDB(x.t)
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) Bool() (v bool) {
	if s.err != nil {
		return
	}
	s.unwrap()
	return s.bool()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) Int8() (v int8) {
	if s.err != nil {
		return
	}
	s.unwrap()
	return s.int8()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) Uint8() (v uint8) {
	if s.err != nil {
		return
	}
	s.unwrap()
	return s.uint8()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) Int16() (v int16) {
	if s.err != nil {
		return
	}
	s.unwrap()
	return s.int16()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) Uint16() (v uint16) {
	if s.err != nil {
		return
	}
	s.unwrap()
	return s.uint16()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) Int32() (v int32) {
	if s.err != nil {
		return
	}
	s.unwrap()
	return s.int32()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) Uint32() (v uint32) {
	if s.err != nil {
		return
	}
	s.unwrap()
	return s.uint32()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) Int64() (v int64) {
	if s.err != nil {
		return
	}
	s.unwrap()
	return s.int64()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) Uint64() (v uint64) {
	if s.err != nil {
		return
	}
	s.unwrap()
	return s.uint64()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) Float() (v float32) {
	if s.err != nil {
		return
	}
	s.unwrap()
	return s.float()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) Double() (v float64) {
	if s.err != nil {
		return
	}
	s.unwrap()
	return s.double()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) Date() (v uint32) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_DATE) {
		return
	}
	return s.uint32()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) Datetime() (v uint32) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_DATETIME) {
		return
	}
	return s.uint32()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) Timestamp() (v uint64) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_TIMESTAMP) {
		return
	}
	return s.uint64()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) Interval() (v int64) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_INTERVAL) {
		return
	}
	return s.int64()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) TzDate() (v string) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_TZ_DATE) {
		return
	}
	return s.text()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) TzDatetime() (v string) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_TZ_DATETIME) {
		return
	}
	return s.text()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) TzTimestamp() (v string) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_TZ_TIMESTAMP) {
		return
	}
	return s.text()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) String() (v []byte) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_STRING) {
		return
	}
	return s.bytes()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) UTF8() (v string) {
	if s.err != nil {
		return
	}
	s.unwrap()
	return s.text()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) YSON() (v []byte) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_YSON) {
		return
	}
	return s.bytes()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) JSON() (v string) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_JSON) {
		return
	}
	return s.text()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) UUID() (v [16]byte) {
	if s.err != nil {
		return
	}
	s.unwrap()
	return s.uint128()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) JSONDocument() (v string) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_JSON_DOCUMENT) {
		return
	}
	return s.text()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) DyNumber() (v string) {
	if s.err != nil {
		return
	}
	s.unwrap()
	return s.text()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) OBool() (v bool) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_BOOL) {
		return
	}
	if s.isNull() {
		return
	}
	return s.bool()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) OInt8() (v int8) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_INT8) {
		return
	}
	if s.isNull() {
		return
	}
	return s.int8()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) OUint8() (v uint8) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_UINT8) {
		return
	}
	if s.isNull() {
		return
	}
	return s.uint8()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) OInt16() (v int16) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_INT16) {
		return
	}
	if s.isNull() {
		return
	}
	return s.int16()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) OUint16() (v uint16) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_UINT16) {
		return
	}
	if s.isNull() {
		return
	}
	return s.uint16()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) OInt32() (v int32) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_INT32) {
		return
	}
	if s.isNull() {
		return
	}
	return s.int32()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) OUint32() (v uint32) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_UINT32) {
		return
	}
	if s.isNull() {
		return
	}
	return s.uint32()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) OInt64() (v int64) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_INT64) {
		return
	}
	if s.isNull() {
		return
	}
	return s.int64()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) OUint64() (v uint64) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_UINT64) {
		return
	}
	if s.isNull() {
		return
	}
	return s.uint64()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) OFloat() (v float32) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_FLOAT) {
		return
	}
	if s.isNull() {
		return
	}
	return s.float()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) ODouble() (v float64) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_DOUBLE) {
		return
	}
	if s.isNull() {
		return
	}
	return s.double()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) ODate() (v uint32) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_DATE) {
		return
	}
	if s.isNull() {
		return
	}
	return s.uint32()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) ODatetime() (v uint32) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_DATETIME) {
		return
	}
	if s.isNull() {
		return
	}
	return s.uint32()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) OTimestamp() (v uint64) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_TIMESTAMP) {
		return
	}
	if s.isNull() {
		return
	}
	return s.uint64()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) OInterval() (v int64) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_INTERVAL) {
		return
	}
	if s.isNull() {
		return
	}
	return s.int64()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) OTzDate() (v string) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_TZ_DATE) {
		return
	}
	if s.isNull() {
		return
	}
	return s.text()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) OTzDatetime() (v string) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_TZ_DATETIME) {
		return
	}
	if s.isNull() {
		return
	}
	return s.text()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) OTzTimestamp() (v string) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_TZ_TIMESTAMP) {
		return
	}
	if s.isNull() {
		return
	}
	return s.text()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) OString() (v []byte) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_STRING) {
		return
	}
	if s.isNull() {
		return
	}
	return s.bytes()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) OUTF8() (v string) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_UTF8) {
		return
	}
	if s.isNull() {
		return
	}
	return s.text()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) OYSON() (v string) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_YSON) {
		return
	}
	if s.isNull() {
		return
	}
	return s.text()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) OJSON() (v string) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_JSON) {
		return
	}
	if s.isNull() {
		return
	}
	return s.text()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) OUUID() (v [16]byte) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_UUID) {
		return
	}
	if s.isNull() {
		return
	}
	return s.uint128()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) OJSONDocument() (v string) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_JSON_DOCUMENT) {
		return
	}
	if s.isNull() {
		return
	}
	return s.text()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) ODyNumber() (v string) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_DYNUMBER) {
		return
	}
	if s.isNull() {
		return
	}
	return s.text()
}

// Value returns current item under scan as ydb.Value type.
// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) Value() ydb.Value {
	if s.err != nil {
		return nil
	}
	s.unwrap()
	x := s.stack.current()
	return internal.ValueFromYDB(x.t, x.v)
}

// Deprecated: Use Scan and implement ydb.Scanner
func (s *Scanner) Any() interface{} {
	x := s.stack.current()
	if s.err != nil || x.isEmpty() {
		return nil
	}

	if s.IsNull() {
		return nil
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
	case
		internal.TypeYSON,
		internal.TypeString:
		return s.bytes()
	case internal.TypeUUID:
		return s.uint128()
	case
		internal.TypeUint32,
		internal.TypeDate,
		internal.TypeDatetime:
		return s.uint32()
	case
		internal.TypeUint64,
		internal.TypeTimestamp:
		return s.uint64()
	case
		internal.TypeInt64,
		internal.TypeInterval:
		return s.int64()
	case
		internal.TypeTzDate,
		internal.TypeTzDatetime,
		internal.TypeTzTimestamp,
		internal.TypeUTF8,
		internal.TypeJSON,
		internal.TypeJSONDocument,
		internal.TypeDyNumber:
		return s.text()
	default:
		panic("ydb/table: unknown primitive type")
	}
}

// Deprecated: Use Scan and implement ydb.Scanner
func (s *Scanner) ListIn() (size int) {
	if s.err != nil {
		return 0
	}
	x := s.stack.current()
	if s.assertTypeList(x.t) != nil {
		return s.itemsIn()
	}
	return 0
}

// Deprecated: Use Scan and implement ydb.Scanner
func (s *Scanner) ListItem(i int) {
	if s.err != nil {
		return
	}
	p := s.stack.parent()
	if !s.itemsBoundsCheck(p.v.Items, i) {
		return
	}
	if t := s.assertTypeList(p.t); t != nil {
		s.stack.set(item{
			i: i,
			t: t.ListType.Item,
			v: p.v.Items[i],
		})
	}
}

// Deprecated: Use Scan and implement ydb.Scanner
func (s *Scanner) ListOut() {
	if s.err != nil {
		return
	}
	p := s.stack.parent()
	if t := s.assertTypeList(p.t); t != nil {
		s.itemsOut()
	}
}

// Deprecated: Use Scan and implement ydb.Scanner
func (s *Scanner) TupleIn() (size int) {
	if s.err != nil {
		return 0
	}
	x := s.stack.current()
	if s.assertTypeTuple(x.t) != nil {
		return s.itemsIn()
	}
	return 0
}

// Deprecated: Use Scan and implement ydb.Scanner
func (s *Scanner) TupleItem(i int) {
	if s.err != nil {
		return
	}
	p := s.stack.parent()
	if !s.itemsBoundsCheck(p.v.Items, i) {
		return
	}
	if t := s.assertTypeTuple(p.t); t != nil {
		s.stack.set(item{
			i: i,
			t: t.TupleType.Elements[i],
			v: p.v.Items[i],
		})
	}
}

// Deprecated: Use Scan and implement ydb.Scanner
func (s *Scanner) TupleOut() {
	if s.err != nil {
		return
	}
	p := s.stack.parent()
	if t := s.assertTypeTuple(p.t); t != nil {
		s.itemsOut()
	}
}

// Deprecated: Use Scan and implement ydb.Scanner
func (s *Scanner) StructIn() (size int) {
	if s.err != nil {
		return 0
	}
	x := s.stack.current()
	if s.assertTypeStruct(x.t) != nil {
		return s.itemsIn()
	}
	return 0
}

// Deprecated: Use Scan and implement ydb.Scanner
func (s *Scanner) StructField(i int) (name string) {
	if s.err != nil {
		return
	}
	p := s.stack.parent()
	if !s.itemsBoundsCheck(p.v.Items, i) {
		return
	}
	if t := s.assertTypeStruct(p.t); t != nil {
		m := t.StructType.Members[i]
		name = m.Name
		s.stack.set(item{
			name: m.Name,
			i:    i,
			t:    m.Type,
			v:    p.v.Items[i],
		})
	}
	return
}

// Deprecated: Use Scan and implement ydb.Scanner
func (s *Scanner) StructOut() {
	if s.err != nil {
		return
	}
	p := s.stack.parent()
	if t := s.assertTypeStruct(p.t); t != nil {
		s.itemsOut()
	}
}

// Deprecated: Use Scan and implement ydb.Scanner
func (s *Scanner) DictIn() (size int) {
	if s.err != nil {
		return 0
	}
	x := s.stack.current()
	if s.assertTypeDict(x.t) != nil {
		return s.pairsIn()
	}
	return 0
}

// Deprecated: Use Scan and implement ydb.Scanner
func (s *Scanner) DictKey(i int) {
	if s.err != nil {
		return
	}
	p := s.stack.parent()
	if !s.pairsBoundsCheck(p.v.Pairs, i) {
		return
	}
	if t := s.assertTypeDict(p.t); t != nil {
		s.stack.set(item{
			i: i,
			t: t.DictType.Key,
			v: p.v.Pairs[i].Key,
		})
	}
}

// Deprecated: Use Scan and implement ydb.Scanner
func (s *Scanner) DictPayload(i int) {
	if s.err != nil {
		return
	}
	p := s.stack.parent()
	if !s.pairsBoundsCheck(p.v.Pairs, i) {
		return
	}
	if t := s.assertTypeDict(p.t); t != nil {
		s.stack.set(item{
			i: i,
			t: t.DictType.Payload,
			v: p.v.Pairs[i].Payload,
		})
	}
}

// Deprecated: Use Scan and implement ydb.Scanner
func (s *Scanner) DictOut() {
	if s.err != nil {
		return
	}
	p := s.stack.parent()
	if t := s.assertTypeDict(p.t); t != nil {
		s.pairsOut()
	}
}

// Deprecated: Use Scan and implement ydb.Scanner
func (s *Scanner) Variant() (name string, index uint32) {
	if s.err != nil {
		return
	}
	x := s.stack.current()
	t := s.assertTypeVariant(x.t)
	if t == nil {
		return
	}
	v, index := s.variant()
	if v == nil {
		return
	}
	name, typ := s.unwrapVariantType(t, index)
	s.stack.scanItem.v = nil
	s.stack.set(item{
		name: name,
		i:    int(index),
		t:    typ,
		v:    v,
	})
	return name, index
}

// Deprecated: Use Scan and implement ydb.Scanner
func (s *Scanner) Unwrap() {
	if s.err != nil {
		return
	}
	x := s.stack.current()
	t := s.assertTypeOptional(x.t)
	if t == nil {
		return
	}
	v := x.v
	if isOptional(t.OptionalType.Item) {
		v = s.unwrapValue()
	}
	s.stack.enter()
	s.stack.set(item{
		name: "*",
		t:    t.OptionalType.Item,
		v:    v,
	})
}

// Deprecated: Use Scan and implement ydb.Scanner
func (s *Scanner) Decimal(t ydb.Type) (v [16]byte) {
	if s.err != nil || !s.assertCurrentTypeDecimal(t) {
		return
	}
	return s.uint128()
}

// Deprecated: Use Scan and implement ydb.Scanner
func (s *Scanner) UnwrapDecimal() (v [16]byte, precision, scale uint32) {
	d := s.assertTypeDecimal(s.stack.current().t)
	if d == nil {
		return
	}
	return s.uint128(), d.DecimalType.Precision, d.DecimalType.Scale
}

// Deprecated: Use Scan and implement ydb.Scanner
func (s *Scanner) ODecimal(t ydb.Type) (v [16]byte) {
	if s.err != nil || !s.assertCurrentTypeOptionalDecimal(t) {
		return
	}
	if s.isNull() {
		return
	}
	return s.uint128()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) AssertType(t ydb.Type) bool {
	return s.assertCurrentTypeIs(t)
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) Null() {
	if s.err != nil || !s.assertCurrentTypeNullable() {
		return
	}
	s.null()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) IsNull() bool {
	if s.err != nil {
		return false
	}
	return s.isNull()
}

// Deprecated: Use Scan instead
// Method will be available only for RawValue in the next major release
func (s *Scanner) IsOptional() bool {
	if s.err != nil {
		return false
	}
	return s.isCurrentTypeOptional()
}

// Deprecated: Use Scan and implement ydb.Scanner
func (s *Scanner) IsDecimal() bool {
	if s.err != nil {
		return false
	}
	return s.isCurrentTypeDecimal()
}

func (s *Scanner) isCurrentTypeDecimal() bool {
	c := s.stack.current()
	_, ok := c.t.Type.(*Ydb.Type_DecimalType)
	return ok
}

func (s *Scanner) unwrapVariantType(typ *Ydb.Type_VariantType, index uint32) (name string, t *Ydb.Type) {
	i := int(index)
	switch x := typ.VariantType.Type.(type) {
	case *Ydb.VariantType_TupleItems:
		if i >= len(x.TupleItems.Elements) {
			s.errorf("TODO")
			return
		}
		return "", x.TupleItems.Elements[i]

	case *Ydb.VariantType_StructItems:
		if i >= len(x.StructItems.Members) {
			s.errorf("TODO")
			return
		}
		m := x.StructItems.Members[i]
		return m.Name, m.Type

	default:
		panic("ydb/table: unexpected variant items type")
	}
}

func (s *Scanner) boundsError(n, i int) {
	s.errorf(
		"index out of range: %d; have %d",
		i, n,
	)
}

func (s *Scanner) decimalTypeError(t ydb.Type) {
	s.errorf(
		"unexpected decimal type at %q %s: want %s",
		s.Path(), s.Type(), t,
	)
}

func (s *Scanner) null() {
	x, _ := s.stack.currentValue().(*Ydb.Value_NullFlagValue)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
	}
}

func (s *Scanner) variant() (v *Ydb.Value, index uint32) {
	v = s.unwrapValue()
	if v == nil {
		return
	}
	x := s.stack.current() // Is not nil if unwrapValue succeeded.
	index = x.v.VariantIndex
	return
}

func (s *Scanner) assertCurrentTypeNullable() bool {
	c := s.stack.current()
	if isOptional(c.t) {
		return true
	}
	p := s.stack.parent()
	if isOptional(p.t) {
		return true
	}
	s.errorf("not nullable type at %q: %s (%d %s %s)", s.Path(), s.Type(), s.stack.size(), c.t, p.t)
	return false
}
func (s *Scanner) assertCurrentTypeIs(t ydb.Type) bool {
	c := s.stack.current()
	act := internal.TypeFromYDB(c.t)
	if !internal.TypesEqual(act, t) {
		s.errorf(
			"unexpected type at %q %s: %s; want %s",
			s.Path(), s.Type(), act, t,
		)
		return false
	}
	return true
}
func (s *Scanner) assertCurrentTypeDecimal(t ydb.Type) bool {
	d := s.assertTypeDecimal(s.stack.current().t)
	if d == nil {
		return false
	}
	if !isEqualDecimal(d.DecimalType, t) {
		s.decimalTypeError(t)
		return false
	}
	return true
}
func (s *Scanner) assertCurrentTypeOptionalDecimal(t ydb.Type) bool {
	typ := s.stack.current().t
	if t, _ := typ.Type.(*Ydb.Type_OptionalType); t != nil {
		typ = t.OptionalType.Item
	}
	if typ == nil {
		return false
	}
	d := s.assertTypeDecimal(typ)
	if d == nil {
		return false
	}
	if !isEqualDecimal(d.DecimalType, t) {
		s.decimalTypeError(t)
		return false
	}
	return true
}

func (s *Scanner) itemsIn() int {
	x := s.stack.current()
	if x.isEmpty() {
		return -1
	}
	s.stack.enter()
	return len(x.v.Items)
}

func (s *Scanner) itemsOut() {
	s.stack.leave()
}
func (s *Scanner) itemsBoundsCheck(xs []*Ydb.Value, i int) bool {
	return s.boundsCheck(len(xs), i)
}

func (s *Scanner) pairsIn() int {
	x := s.stack.current()
	if x.isEmpty() {
		return -1
	}
	s.stack.enter()
	return len(x.v.Pairs)
}
func (s *Scanner) pairsOut() {
	s.stack.leave()
}
func (s *Scanner) pairsBoundsCheck(xs []*Ydb.ValuePair, i int) bool {
	return s.boundsCheck(len(xs), i)
}

func (s *Scanner) boundsCheck(n, i int) bool {
	if i < 0 || n <= i {
		s.boundsError(n, i)
		return false
	}
	return true
}

func (s *Scanner) assertTypeList(typ *Ydb.Type) (t *Ydb.Type_ListType) {
	x := typ.Type
	if t, _ = x.(*Ydb.Type_ListType); t == nil {
		s.typeError(x, t)
	}
	return
}
func (s *Scanner) assertTypeTuple(typ *Ydb.Type) (t *Ydb.Type_TupleType) {
	x := typ.Type
	if t, _ = x.(*Ydb.Type_TupleType); t == nil {
		s.typeError(x, t)
	}
	return
}
func (s *Scanner) assertTypeStruct(typ *Ydb.Type) (t *Ydb.Type_StructType) {
	x := typ.Type
	if t, _ = x.(*Ydb.Type_StructType); t == nil {
		s.typeError(x, t)
	}
	return
}
func (s *Scanner) assertTypeDict(typ *Ydb.Type) (t *Ydb.Type_DictType) {
	x := typ.Type
	if t, _ = x.(*Ydb.Type_DictType); t == nil {
		s.typeError(x, t)
	}
	return
}
func (s *Scanner) assertTypeDecimal(typ *Ydb.Type) (t *Ydb.Type_DecimalType) {
	x := typ.Type
	if t, _ = x.(*Ydb.Type_DecimalType); t == nil {
		s.typeError(x, t)
	}
	return
}
func (s *Scanner) assertTypePrimitive(typ *Ydb.Type) (t *Ydb.Type_TypeId) {
	x := typ.Type
	if t, _ = x.(*Ydb.Type_TypeId); t == nil {
		s.typeError(x, t)
	}
	return
}

func (s *Scanner) assertTypeVariant(typ *Ydb.Type) (t *Ydb.Type_VariantType) {
	x := typ.Type
	if t, _ = x.(*Ydb.Type_VariantType); t == nil {
		s.typeError(x, t)
	}
	return
}

func isEqualDecimal(d *Ydb.DecimalType, t ydb.Type) bool {
	w := t.(internal.DecimalType)
	return d.Precision == w.Precision && d.Scale == w.Scale
}

func nameIface(v interface{}) string {
	if v == nil {
		return "nil"
	}
	t := reflect.TypeOf(v)
	s := t.String()
	s = strings.TrimPrefix(s, "*Ydb.Value_")
	s = strings.TrimSuffix(s, "Value")
	s = strings.TrimPrefix(s, "*Ydb.Type_")
	s = strings.TrimSuffix(s, "Type")
	return s
}
