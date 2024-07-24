package internal

import "time"

const (
	secondsPerMinute int64 = 60
	secondsPerHour         = 60 * secondsPerMinute
	secondsPerDay          = 24 * secondsPerHour
)

// Date format layouts described in time.Format and time.ANSIC docs.
const (
	tzLayoutDate      = "2006-01-02,MST"
	tzLayoutDatetime  = "2006-01-02T15:04:05,MST"
	tzLayoutTimestamp = "2006-01-02T15:04:05.000000,MST"
)

var (
	unix = time.Unix(0, 0)
)

func DurationFromMicroseconds(n int64) time.Duration {
	return time.Duration(n) * time.Microsecond
}

func DurationToMicroseconds(d time.Duration) int64 {
	return int64(d / time.Microsecond)
}

// Up to 11761191-01-20 00:00:00 +0000 UTC.
func UnmarshalDate(n uint32) time.Time {
	return time.Unix(int64(n)*secondsPerDay, 0)
}

func MarshalDate(t time.Time) uint32 {
	d := t.Sub(unix)
	d /= (time.Hour * 24)
	return uint32(d)
}

// Up to 2106-02-07 06:28:15 +0000 UTC.
func UnmarshalDatetime(n uint32) time.Time {
	return time.Unix(int64(n), 0)
}

func MarshalDatetime(t time.Time) uint32 {
	d := t.Sub(unix)
	d /= time.Second
	return uint32(d)
}

// Up to 586524-01-19 08:01:49.000551615 +0000 UTC.
func UnmarshalTimestamp(n uint64) time.Time {
	sec := n / 1e6
	nsec := (n - (sec * 1e6)) * 1000
	return time.Unix(int64(sec), int64(nsec))
}

func MarshalTimestamp(t time.Time) uint64 {
	d := t.Sub(unix)
	d /= time.Microsecond
	return uint64(d)
}

func UnmarshalTzDate(s string) (time.Time, error) {
	return time.Parse(tzLayoutDate, s)
}

func MarshalTzDate(t time.Time) string {
	return t.Format(tzLayoutDate)
}

func UnmarshalTzDatetime(s string) (time.Time, error) {
	return time.Parse(tzLayoutDatetime, s)
}

func MarshalTzDatetime(t time.Time) string {
	return t.Format(tzLayoutDatetime)
}

func UnmarshalTzTimestamp(s string) (time.Time, error) {
	return time.Parse(tzLayoutTimestamp, s)
}

func MarshalTzTimestamp(t time.Time) string {
	return t.Format(tzLayoutTimestamp)
}
