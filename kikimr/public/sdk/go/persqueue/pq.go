package persqueue

import (
	"fmt"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/genproto/Ydb_PersQueue_V0"
	"google.golang.org/grpc/codes"
)

type Codec int32

const (
	Raw Codec = iota
	Gzip
	Lzop // XXX: Not implemented.
	Zstd
)

const Version = "Persqueue Go SDK v0.0.1"

type TopicInfo struct {
	Topic           string
	PartitionGroups []uint32
}

// ToTimestamp converts time to representation used by logbroker.
func ToTimestamp(t time.Time) uint64 {
	return uint64(t.UnixNano() / int64(time.Millisecond))
}

// FromTimestamp converts time from representation used by logbroker.
func FromTimestamp(t uint64) time.Time {
	return time.Unix(0, int64(t*uint64(time.Millisecond)))
}

type temparable interface {
	Temporary() bool
}

type Error struct {
	Code        int
	Description string
}

func (e *Error) Error() string {
	return fmt.Sprintf("persqueue: %s (code %d)", e.Description, e.Code)
}

func (e *Error) Temporary() bool {
	return e.Code != int(Ydb_PersQueue_V0.EErrorCode_UNKNOWN_TOPIC) &&
		e.Code != int(Ydb_PersQueue_V0.EErrorCode_ACCESS_DENIED) &&
		e.Code != int(Ydb_PersQueue_V0.EErrorCode_BAD_REQUEST)
}

var (
	_ temparable = (*Error)(nil)
	_ error      = (*Error)(nil)
)

type GRPCError struct {
	Description string
	Status      codes.Code
}

func (e *GRPCError) Error() string {
	return fmt.Sprintf("grpc: %s (status: %v)", e.Description, e.Status)
}

func (e *GRPCError) Temporary() bool {
	return e.Status == codes.Unavailable ||
		e.Status == codes.Aborted
}

var (
	_ temparable = (*GRPCError)(nil)
	_ error      = (*GRPCError)(nil)
)
