package internal

import (
	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
)

type Operation struct {
	method string
	req    proto.Message
	res    proto.Message
	resp   Response
}

func WrapWithResponse(method string, req proto.Message, resp Response) Operation {
	return Operation{
		method: method,
		req:    req,
		resp:   resp,
	}
}

func Wrap(method string, req, res proto.Message) Operation {
	return Operation{
		method: method,
		req:    req,
		res:    res,
	}
}

func Unwrap(op Operation) (method string, req, res proto.Message, resp Response) {
	return op.method, op.req, op.res, op.resp
}

// StreamOperationResponse is an interface that provides access to the
// API-specific response fields.
//
// NOTE: YDB API currently does not provide generic response wrapper as it does
// with RPC API. Thus wee need to generalize it by the hand using this interface.
//
// This generalization is needed for checking status codes and issues in one place.
type StreamOperationResponse interface {
	GetStatus() Ydb.StatusIds_StatusCode
	GetIssues() []*Ydb_Issue.IssueMessage
}

type StreamOperation struct {
	method    string
	req       proto.Message
	resp      StreamOperationResponse
	processor func(error)
}

func WrapStreamOperation(
	method string, req proto.Message,
	resp StreamOperationResponse,
	p func(error),
) StreamOperation {
	return StreamOperation{
		method:    method,
		req:       req,
		resp:      resp,
		processor: p,
	}
}

func UnwrapStreamOperation(op StreamOperation) (
	method string, req proto.Message,
	resp StreamOperationResponse,
	processor func(error),
) {
	return op.method, op.req, op.resp, op.processor
}
