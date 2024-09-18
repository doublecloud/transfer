package stringutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTruncateUTF8(t *testing.T) {
	testTruncateUTF8(t, "", "¡", 1)
	testTruncateUTF8(t, "", "♂", 1)
	testTruncateUTF8(t, "", "🍆", 1)
	testTruncateUTF8(t, "!", "!¡", 2)
	testTruncateUTF8(t, "!", "!♂", 2)
	testTruncateUTF8(t, "!", "!🍆", 2)
}

func testTruncateUTF8(t *testing.T, truncated, s string, limit int) {
	require.Equal(t, truncated, string(TruncateUTF8([]byte(s), limit)))
	require.Equal(t, truncated, TruncateUTF8(s, limit))
}
