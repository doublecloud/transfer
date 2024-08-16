//go:build go1.18
// +build go1.18

package ydbsql

import (
	"database/sql/driver"
	"errors"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb"
)

type badConnError struct {
	err error
}

func (e badConnError) Error() string {
	return "ydbsql: " + e.err.Error()
}

func (e badConnError) Is(err error) bool {
	//nolint:errorlint
	if err == driver.ErrBadConn {
		return true
	}
	return errors.Is(e.err, err)
}

func (e badConnError) As(target interface{}) bool {
	return errors.As(e.err, target)
}

func mapBadSessionError(err error) error {
	if err == nil {
		return nil
	}
	m := (&ydb.RetryChecker{}).Check(err)
	switch {
	case errors.Is(err, driver.ErrBadConn):
		return err
	case
		m.MustDeleteSession(),
		ydb.IsOpError(err, ydb.StatusOverloaded),
		ydb.IsOpError(err, ydb.StatusUnavailable),
		ydb.IsTransportError(err, ydb.TransportErrorResourceExhausted):
		return badConnError{err: err}
	default:
		return err
	}
}

func unwrapErrBadConn(err error) error {
	var e *badConnError
	if errors.As(err, &e) {
		return e.err
	}
	return err
}
