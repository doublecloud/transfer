package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewIfNil(t *testing.T) {
	var foo struct {
		Bar *struct {
			Value string
		}
	}
	NewIfNil(&foo.Bar).Value = "123"
	require.NotNil(t, foo.Bar)
	require.Equal(t, "123", foo.Bar.Value)
}
