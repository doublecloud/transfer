package experimental

import (
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/internal/pb/protos/Ydb_Experimental"
)

type StreamQueryResult struct {
	ResultType StreamQueryResultType
	Value      interface{}
}

func (s *StreamQueryResult) ResultSet() *StreamQueryResultResultSet {
	if r, ok := s.Value.(*StreamQueryResultResultSet); ok {
		return r
	}
	return nil
}

func (s *StreamQueryResult) Profile() *StreamQueryResultProfile {
	if r, ok := s.Value.(*StreamQueryResultProfile); ok {
		return r
	}
	return nil
}

func (s *StreamQueryResult) Progress() *StreamQueryResultProgress {
	if r, ok := s.Value.(*StreamQueryResultProgress); ok {
		return r
	}
	return nil
}

// StreamQueryResultResultSet TODO: support return real result set
type StreamQueryResultResultSet struct{}

type StreamQueryResultProfile string

type StreamQueryResultProgress struct{}

func streamQueryResult(s *Ydb_Experimental.ExecuteStreamQueryResult) *StreamQueryResult {
	if v := s.GetResultSet(); v != nil {
		return &StreamQueryResult{
			ResultType: StreamQueryResultTypeResultSet,
			Value:      StreamQueryResultResultSet{},
		}
	}

	if v := s.GetProfile(); v != "" {
		return &StreamQueryResult{
			ResultType: StreamQueryResultTypeProfile,
			Value:      &v,
		}
	}

	if v := s.GetProgress(); v != nil {
		return &StreamQueryResult{
			ResultType: StreamQueryResultTypeProgress,
			Value:      StreamQueryResultProgress{},
		}
	}

	return &StreamQueryResult{
		ResultType: StreamQueryResultTypeResultUnknown,
	}
}

type StreamQueryResultType uint

const (
	StreamQueryResultTypeResultUnknown StreamQueryResultType = iota
	StreamQueryResultTypeResultSet
	StreamQueryResultTypeProfile
	StreamQueryResultTypeProgress
)
