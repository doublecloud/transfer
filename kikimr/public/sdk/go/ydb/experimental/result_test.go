package experimental

import (
	"testing"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/internal/pb/protos/Ydb_Experimental"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

func TestResultAny(t *testing.T) {
	var ydbResult Ydb_Experimental.ExecuteStreamQueryResult

	ydbResult = Ydb_Experimental.ExecuteStreamQueryResult{
		Result: &Ydb_Experimental.ExecuteStreamQueryResult_ResultSet{
			ResultSet: &Ydb.ResultSet{},
		},
	}
	r := streamQueryResult(&ydbResult)
	if r.ResultType != StreamQueryResultTypeResultSet && r.ResultSet() == nil {
		t.Error("Unexpected execute query result")
	}

	ydbResult = Ydb_Experimental.ExecuteStreamQueryResult{
		Result: &Ydb_Experimental.ExecuteStreamQueryResult_Profile{
			Profile: "myProfile",
		},
	}
	r = streamQueryResult(&ydbResult)
	if r.ResultType != StreamQueryResultTypeProfile && r.Profile() == nil && *r.Profile() != "myProfile" {
		t.Error("Unexpected execute query result")
	}

	ydbResult = Ydb_Experimental.ExecuteStreamQueryResult{
		Result: &Ydb_Experimental.ExecuteStreamQueryResult_Progress{
			Progress: &Ydb_Experimental.StreamQueryProgress{},
		},
	}
	r = streamQueryResult(&ydbResult)
	if r.ResultType != StreamQueryResultTypeProgress && r.Progress() == nil {
		t.Error("Unexpected execute query result")
	}
}
