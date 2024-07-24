package ydb

type ConnState int8

const (
	ConnOffline ConnState = iota - 2
	ConnBanned
	ConnStateUnknown
	ConnOnline
)

func (s ConnState) String() string {
	switch s {
	case ConnOnline:
		return "online"
	case ConnOffline:
		return "offline"
	case ConnBanned:
		return "banned"
	default:
		return "unknown"
	}
}
