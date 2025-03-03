package gpfdistbin

import (
	"testing"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/stretchr/testify/require"
)

func TestErrorInterface(t *testing.T) {
	err := newCancelFailedError(xerrors.New("error"))
	require.True(t, xerrors.As(err, new(CancelFailedError)))

	var cancelErr1 CancelFailedError
	require.True(t, xerrors.As(err, &cancelErr1))
	require.Equal(t, err, cancelErr1)

	wrappedErr := xerrors.Errorf("unable to fail: %w", err)
	require.True(t, xerrors.As(wrappedErr, new(CancelFailedError)))

	var cancelErr2 CancelFailedError
	require.True(t, xerrors.As(wrappedErr, &cancelErr2))
	require.Equal(t, err, cancelErr2)
}
