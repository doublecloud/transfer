package experimental

import (
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/internal/pb/protos/Ydb_Experimental"
)

type StreamQueryProfile uint

const (
	StreamQueryProfileUnknown StreamQueryProfile = iota
	StreamQueryProfileNone
	StreamQueryProfileBasic
	StreamQueryProfileFull
)

func (s StreamQueryProfile) String() string {
	switch s {
	case StreamQueryProfileNone:
		return "none"
	case StreamQueryProfileBasic:
		return "basic"
	case StreamQueryProfileFull:
		return "full"
	default:
		return "unknown"
	}
}

func (s StreamQueryProfile) toYDB() Ydb_Experimental.ExecuteStreamQueryRequest_ProfileMode {
	switch s {
	case StreamQueryProfileNone:
		return Ydb_Experimental.ExecuteStreamQueryRequest_NONE
	case StreamQueryProfileBasic:
		return Ydb_Experimental.ExecuteStreamQueryRequest_BASIC
	case StreamQueryProfileFull:
		return Ydb_Experimental.ExecuteStreamQueryRequest_FULL
	default:
		return Ydb_Experimental.ExecuteStreamQueryRequest_PROFILE_MODE_UNSPECIFIED
	}
}

func streamQueryProfile(s Ydb_Experimental.ExecuteStreamQueryRequest_ProfileMode) StreamQueryProfile {
	switch s {
	case Ydb_Experimental.ExecuteStreamQueryRequest_NONE:
		return StreamQueryProfileNone
	case Ydb_Experimental.ExecuteStreamQueryRequest_BASIC:
		return StreamQueryProfileBasic
	case Ydb_Experimental.ExecuteStreamQueryRequest_FULL:
		return StreamQueryProfileFull
	default:
		return StreamQueryProfileUnknown
	}
}

type (
	streamQueryDesc   Ydb_Experimental.ExecuteStreamQueryRequest
	StreamQueryOption func(s *streamQueryDesc)
)

func WithStreamQueryProfile(profile StreamQueryProfile) StreamQueryOption {
	return func(s *streamQueryDesc) {
		s.ProfileMode = profile.toYDB()
	}
}
