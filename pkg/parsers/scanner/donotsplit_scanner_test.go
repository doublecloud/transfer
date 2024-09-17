package scanner

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var testDataDoNotSplit = []byte("\n\n\n\n\noSDU;LFHIJC]psuf-9hweafpnodsbg  dfgsdfg sdfg")

func TestDoNotSplit(t *testing.T) {
	sc := NewDoNotSplitScanner(testDataDoNotSplit)

	_, err := sc.Event()
	require.Error(t, err)

	require.True(t, sc.Scan())
	evt, err := sc.Event()
	require.NoError(t, err)
	require.Equal(t, testDataDoNotSplit, evt)

	require.False(t, sc.Scan())
	require.NoError(t, sc.Err())
}
