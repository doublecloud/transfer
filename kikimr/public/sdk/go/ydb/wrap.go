package ydb

import (
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/internal"
	"github.com/golang/protobuf/proto"
)

type Operation = internal.Operation

func WrapWithResponse(method string, req proto.Message, resp Response) Operation {
	return internal.WrapWithResponse(method, req, resp)
}

func Wrap(method string, req, res proto.Message) Operation {
	return internal.Wrap(method, req, res)
}

func Unwrap(op Operation) (method string, req, res proto.Message) {
	method, req, res, _ = internal.Unwrap(op)
	return method, req, res
}

type StreamOperationResponse = internal.StreamOperationResponse

type StreamOperation = internal.StreamOperation

func WrapStreamOperation(
	method string, req proto.Message,
	resp StreamOperationResponse,
	p func(error),
) StreamOperation {
	return internal.WrapStreamOperation(method, req, resp, p)
}

func UnwrapStreamOperation(op StreamOperation) (
	method string, req proto.Message,
	resp StreamOperationResponse,
	processor func(error),
) {
	return internal.UnwrapStreamOperation(op)
}
