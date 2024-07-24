package ydb

import (
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/internal"
)

// Deprecated: Use standard time.Time instead
// This type will be removed at next major release
type Time time.Time

//	func (t Time) String() string {
//		return time.Time(t).String()
//	}
func (t Time) Date() uint32 {
	return internal.MarshalDate(time.Time(t))
}
func (t Time) Datetime() uint32 {
	return internal.MarshalDatetime(time.Time(t))
}
func (t Time) Timestamp() uint64 {
	return internal.MarshalTimestamp(time.Time(t))
}
func (t Time) TzDate() string {
	return internal.MarshalTzDate(time.Time(t))
}
func (t Time) TzDatetime() string {
	return internal.MarshalTzDatetime(time.Time(t))
}
func (t Time) TzTimestamp() string {
	return internal.MarshalTzTimestamp(time.Time(t))
}

func (t *Time) FromDate(x uint32) error {
	*t = Time(internal.UnmarshalDate(x))
	return nil
}
func (t *Time) FromDatetime(x uint32) error {
	*t = Time(internal.UnmarshalDatetime(x))
	return nil
}
func (t *Time) FromTimestamp(x uint64) error {
	*t = Time(internal.UnmarshalTimestamp(x))
	return nil
}
func (t *Time) FromTzDate(x string) error {
	v, err := internal.UnmarshalTzDate(x)
	if err == nil {
		*t = Time(v)
	}
	return err
}
func (t *Time) FromTzDatetime(x string) error {
	v, err := internal.UnmarshalTzDatetime(x)
	if err == nil {
		*t = Time(v)
	}
	return err
}
func (t *Time) FromTzTimestamp(x string) error {
	v, err := internal.UnmarshalTzTimestamp(x)
	if err == nil {
		*t = Time(v)
	}
	return err
}

// Deprecated: Use standard time.Time instead
// This type will be removed at next major release
type Duration time.Duration

func (d Duration) Interval() int64 {
	return internal.DurationToMicroseconds(time.Duration(d))
}
