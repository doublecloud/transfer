package db

import (
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
)

type Streamer interface {
	Append(row abstract.ChangeItem) error
	Commit() error
	Close() error
}

type MarshallingError interface {
	IsMarshallingError()
	error
}

func IsMarshallingError(err error) bool {
	var target MarshallingError
	return xerrors.As(err, &target)
}

type ChangeItemMarshaller func(item abstract.ChangeItem) ([]any, error)

type StreamInserter interface {
	StreamInsert(query string, marshaller ChangeItemMarshaller) (Streamer, error)
}
