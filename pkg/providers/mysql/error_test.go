package mysql

import (
	"testing"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestIsErrorCode(t *testing.T) {
	correctErr := &mysql.MySQLError{Number: 1}
	require.False(t, IsErrorCode(xerrors.New("irrelevant"), 0), "irrelevant errors")
	require.False(t, IsErrorCode(&mysql.MySQLError{Number: 0}, 1), "different code errors")
	require.True(t, IsErrorCode(correctErr, 1), "equal code errors")
	require.True(t, IsErrorCode(xerrors.Errorf("oh: %w", correctErr), 1), "wrapped equal code errors")
}
