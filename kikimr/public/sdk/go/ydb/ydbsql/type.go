package ydbsql

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/internal"
)

type valuer interface {
	Value() ydb.Value
}

type nullable struct {
	s sql.Scanner
}

func Nullable(s sql.Scanner) sql.Scanner {
	return nullable{s: s}
}

func (n nullable) Scan(x interface{}) error {
	if x == nil {
		return nil
	}
	return n.s.Scan(x)
}

type Date time.Time

func (d *Date) Scan(x interface{}) error {
	v, ok := x.(uint32)
	if !ok {
		return convertError(v, x)
	}
	*d = Date(internal.UnmarshalDate(v))
	return nil
}

func (d Date) Value() ydb.Value {
	return ydb.DateValueFromTime(time.Time(d))
}

type Datetime time.Time

func (d *Datetime) Scan(x interface{}) error {
	v, ok := x.(uint32)
	if !ok {
		return convertError(v, x)
	}
	*d = Datetime(internal.UnmarshalDatetime(v))
	return nil
}

func (d Datetime) Value() ydb.Value {
	return ydb.DatetimeValueFromTime(time.Time(d))
}

type Timestamp time.Time

func (d *Timestamp) Scan(x interface{}) error {
	v, ok := x.(uint64)
	if !ok {
		return convertError(v, x)
	}
	*d = Timestamp(internal.UnmarshalTimestamp(v))
	return nil
}

func (d Timestamp) Value() ydb.Value {
	return ydb.TimestampValueFromTime(time.Time(d))
}

type Interval time.Duration

func (d *Interval) Scan(x interface{}) error {
	v, ok := x.(int64)
	if !ok {
		return convertError(v, x)
	}
	*d = Interval(internal.DurationFromMicroseconds(v))
	return nil
}

func (d Interval) Value() ydb.Value {
	return ydb.IntervalValueFromDuration(time.Duration(d))
}

type TzDate time.Time

func (d *TzDate) Scan(x interface{}) error {
	v, ok := x.(string)
	if !ok {
		return convertError(v, x)
	}
	t, err := internal.UnmarshalTzDate(v)
	if err != nil {
		return err
	}
	*d = TzDate(t)
	return nil
}

func (d TzDate) Value() ydb.Value {
	return ydb.TzDateValueFromTime(time.Time(d))
}

type TzDatetime time.Time

func (d *TzDatetime) Scan(x interface{}) error {
	v, ok := x.(string)
	if !ok {
		return convertError(v, x)
	}
	t, err := internal.UnmarshalTzDatetime(v)
	if err != nil {
		return err
	}
	*d = TzDatetime(t)
	return nil
}

func (d TzDatetime) Value() ydb.Value {
	return ydb.TzDatetimeValueFromTime(time.Time(d))
}

type TzTimestamp time.Time

func (d *TzTimestamp) Scan(x interface{}) error {
	v, ok := x.(string)
	if !ok {
		return convertError(v, x)
	}
	t, err := internal.UnmarshalTzTimestamp(v)
	if err != nil {
		return err
	}
	*d = TzTimestamp(t)
	return nil
}

func (d TzTimestamp) Value() ydb.Value {
	return ydb.TzTimestampValueFromTime(time.Time(d))
}

type Decimal struct {
	Bytes     [16]byte
	Precision uint32
	Scale     uint32
}

func (d *Decimal) Scan(x interface{}) error {
	v, ok := x.(Decimal)
	if !ok {
		return convertError(v, x)
	}
	*d = v
	return nil
}

func convertError(dst, src interface{}) error {
	return fmt.Errorf(
		"ydbsql: can not convert value type %[1]T (%[1]v) to a %[2]T",
		src, dst,
	)
}
