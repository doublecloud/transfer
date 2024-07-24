package persqueue

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInflight(t *testing.T) {
	var b inflight

	msg0 := &WriteMessage{Data: []byte("foo")}
	msg1 := &WriteMessage{Data: []byte("bar")}

	b.Push(msg0)
	b.Push(msg1)

	require.Equal(t, b.Size(), 6)

	require.Len(t, b.PeekAll(), 2)

	require.Equal(t, msg0, b.Peek())
	require.Equal(t, msg0, b.Pop())

	require.Equal(t, msg1, b.Peek())
	require.Len(t, b.PeekAll(), 1)

	require.Equal(t, b.Size(), 3)
}
