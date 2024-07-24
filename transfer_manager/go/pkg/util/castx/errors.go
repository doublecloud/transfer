package castx

import "github.com/doublecloud/tross/library/go/core/xerrors"

type CastError struct{ error }

func NewCastError(err error) *CastError {
	return &CastError{
		error: xerrors.Errorf("cast failed: %w", err),
	}
}

func (e CastError) Error() string {
	return e.error.Error()
}

func (e CastError) Is(err error) bool {
	_, ok := err.(*CastError)
	return ok
}

func (e CastError) Unwrap() error {
	return e.error
}
