package mysql

import (
	"testing"
	"time"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/stretchr/testify/require"
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

func TestTimezoneOffset(t *testing.T) {
	loc, err := time.LoadLocation("UTC")
	require.NoError(t, err)
	require.Equal(t, "+00:00", timezoneOffset(loc))

	loc, err = time.LoadLocation("")
	require.NoError(t, err)
	require.Equal(t, "+00:00", timezoneOffset(loc))

	loc, err = time.LoadLocation("Europe/Moscow")
	require.NoError(t, err)
	require.Equal(t, "+03:00", timezoneOffset(loc))

	loc, err = time.LoadLocation("America/Los_Angeles")
	require.NoError(t, err)
	require.Equal(t, "-07:00", timezoneOffset(loc))

	loc, err = time.LoadLocation("Pacific/Marquesas")
	require.NoError(t, err)
	require.Equal(t, "-09:30", timezoneOffset(loc))

	loc, err = time.LoadLocation("Pacific/Kiritimati")
	require.NoError(t, err)
	require.Equal(t, "+14:00", timezoneOffset(loc))
}
