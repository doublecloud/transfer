package test

import (
	"testing"

	"github.com/doublecloud/tross/library/go/core/resource"
	"github.com/stretchr/testify/assert"
)

func TestResource(t *testing.T) {
	assert.Equal(t, []byte("bar"), resource.Get("foo"))
	assert.Equal(t, []byte("baz"), resource.Get("bar"))
}
