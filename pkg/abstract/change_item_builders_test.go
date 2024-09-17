package abstract

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTxDone(t *testing.T) {
	changeItem := MakeTxDone(1, 2, time.Now(), "a", "b")
	require.True(t, changeItem.IsTxDone())
}
