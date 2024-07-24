package ydb

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc/status"
)

func isTimeoutError(err error) bool {
	var te *TransportError

	switch {
	case
		IsOpError(err, StatusTimeout),
		IsOpError(err, StatusCancelled),
		errors.As(err, &te),
		errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, context.Canceled):
		return true
	default:
		return false
	}
}

func errIf(cond bool, err error) error {
	if cond {
		return err
	}
	return nil
}

func mapGRPCError(err error) error {
	if err == nil {
		return nil
	}
	if te, ok := err.(*TransportError); ok {
		return te
	}
	s, ok := status.FromError(err)
	if !ok {
		return err
	}
	return &TransportError{
		Reason:  transportErrorCode(s.Code()),
		message: s.Message(),
		err:     err,
	}
}

func hideEOF(err error) error {
	if err == io.EOF {
		return nil
	}
	return err
}

// IsTransportError reports whether err is TransportError with given code as
// the Reason.
func IsTransportError(err error, code TransportErrorCode) bool {
	var t *TransportError
	if !errors.As(err, &t) {
		return false
	}
	return t.Reason == code
}

// IsOpError reports whether err is OpError with given code as the Reason.
func IsOpError(err error, code StatusCode) bool {
	var op *OpError
	if !errors.As(err, &op) {
		return false
	}
	return op.Reason == code
}
