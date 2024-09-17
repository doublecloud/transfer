package mysql

import (
	"testing"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
)

func TestNotMasterErrorWrapping(t *testing.T) {
	abstract.CheckOpaqueErrorWrapping(t, "struct", func(err error) bool {
		return xerrors.Is(err, *new(NotMasterError))
	}, func(err error) error {
		return *new(NotMasterError)
	})
	abstract.CheckOpaqueErrorWrapping(t, "pointer", func(err error) bool {
		return xerrors.Is(err, *new(NotMasterError))
	}, func(err error) error {
		return new(NotMasterError)
	})
}
