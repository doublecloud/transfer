package experimental

import (
	"bytes"
	"context"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/internal"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/internal/pb/protos/Ydb_Experimental"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

type Client struct {
	Driver ydb.Driver
}

// ExecuteStreamQuery not fully supported yet
func (c *Client) ExecuteStreamQuery(ctx context.Context, query string, params *QueryParameters, opts ...StreamQueryOption) (*StreamQueryResult, error) {
	var res Ydb_Experimental.ExecuteStreamQueryResult
	req := Ydb_Experimental.ExecuteStreamQueryRequest{
		YqlText:    query,
		Parameters: params.params(),
	}
	for _, opt := range opts {
		opt((*streamQueryDesc)(&req))
	}
	_, err := c.Driver.Call(ctx, ydb.Wrap("/Ydb.Experimental.V1.ExperimentalService/ExecuteStreamQuery", &req, &res))
	if err != nil {
		return nil, err
	}
	return streamQueryResult(&res), nil
}

// QueryParameters TODO: unite with table query parameters
type QueryParameters struct {
	m queryParams
}

func (q *QueryParameters) params() queryParams {
	if q == nil {
		return nil
	}
	return q.m
}

func (q *QueryParameters) Each(it func(name string, value ydb.Value)) {
	if q == nil {
		return
	}
	for key, value := range q.m {
		it(key, internal.ValueFromYDB(
			value.Type,
			value.Value,
		))
	}
}

func (q *QueryParameters) String() string {
	var buf bytes.Buffer
	buf.WriteByte('(')
	q.Each(func(name string, value ydb.Value) {
		buf.WriteString("((")
		buf.WriteString(name)
		buf.WriteString(")(")
		internal.WriteValueStringTo(&buf, value)
		buf.WriteString("))")
	})
	buf.WriteByte(')')
	return buf.String()
}

type queryParams map[string]*Ydb.TypedValue

type ParameterOption func(queryParams)

func NewQueryParameters(opts ...ParameterOption) *QueryParameters {
	q := &QueryParameters{
		m: make(queryParams, len(opts)),
	}
	q.Add(opts...)
	return q
}

func (q *QueryParameters) Add(opts ...ParameterOption) {
	for _, opt := range opts {
		opt(q.m)
	}
}

func ValueParam(name string, v ydb.Value) ParameterOption {
	return func(q queryParams) {
		q[name] = internal.ValueToYDB(v)
	}
}
