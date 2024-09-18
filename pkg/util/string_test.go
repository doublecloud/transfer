package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSampleB(t *testing.T) {
	require.Equal(t, []byte("Hello, world"), SampleBytes([]byte("Hello, world"), 20))
	require.Equal(t, []byte("Hello, world"), SampleBytes([]byte("Hello, world"), 12))
	require.Equal(t, []byte("Hello, worl (1 characters more)"), SampleBytes([]byte("Hello, world"), 11))
	require.Equal(t, []byte("Hello (7 characters more)"), SampleBytes([]byte("Hello, world"), 5))
}

func TestPrefix(t *testing.T) {
	require.Equal(t, "", Prefix("", 5))
	require.Equal(t, "very", Prefix("very long string", 4))
	require.Equal(t, "🍆", Prefix("🍆БАКЛАЖАН🍆", 1), "should slice runes, not bytes in string")
}

func TestEnsureNoStringValsOnTheEdges(t *testing.T) {
	err := EnsureNoStringValsOnTheEdges(`{"a":"bcd"}`)
	require.NoError(t, err)

	err = EnsureNoStringValsOnTheEdges(`{"a":" bcd"}`)
	require.Error(t, err)

	err = EnsureNoStringValsOnTheEdges(`{"a":"bcd "}`)
	require.Error(t, err)

	err = EnsureNoStringValsOnTheEdges(`{"a":" bcd "}`)
	require.Error(t, err)

	err = EnsureNoStringValsOnTheEdges(`{"a":" "}`)
	require.NoError(t, err)

	err = EnsureNoStringValsOnTheEdges(`{"a":{"c" = "bcd"}}`)
	require.NoError(t, err)

	err = EnsureNoStringValsOnTheEdges(`{"a":{"c" = " bcd "}}`)
	require.NoError(t, err)
}
