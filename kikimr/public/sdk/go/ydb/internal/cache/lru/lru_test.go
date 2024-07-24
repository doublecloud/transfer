package lru

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCacheAddGet(t *testing.T) {
	maxSize := 100
	c := New(maxSize)
	rands := randStr()
	pairs := make([]entry, maxSize*100)
	for i := range pairs {
		var (
			key   = rands()
			value = rands()
		)
		c.Add(key, value)
		pairs[i] = entry{key, value}
	}
	bound := len(pairs) - maxSize
	for i, p := range pairs {
		x, ok := c.Get(p.key)
		switch {
		case i >= bound && !ok:
			t.Errorf("unexpected miss for #%dth entry", i)
		case i >= bound && x != p.value:
			t.Errorf("unexpected #%dth value: %v; want %v", i, x, p.value)
		case i < bound && ok:
			t.Errorf("unexpected hit for #%dth entry", i)
		}
	}
}

func TestCacheAddRemove(t *testing.T) {
	var (
		rands = randStr()
		key   = rands()
		val   = rands()
	)
	c := New(1)
	c.Add(key, val)
	{
		act, ok := c.Remove(key)
		if !ok || act != val {
			t.Errorf(
				"unexpected Remove(): %q %t; want %q %t",
				act, ok, val, true,
			)
		}
	}
	{
		act, ok := c.Remove(rands())
		if ok {
			t.Errorf("unexpected Remove(): %q %v", act, ok)
		}
	}
}

func randStr() func() string {
	used := make(map[string]struct{})
	return func() string {
		for {
			s := strconv.FormatUint(rand.Uint64(), 32)
			if _, seen := used[s]; !seen {
				return s
			}
		}
	}
}

func TestCacheZeroSize(t *testing.T) {
	c := New(0)
	for i := 0; i < 10; i++ {
		c.Add(fmt.Sprintf("test%d", i), i)
	}
	for i := 0; i < 10; i++ {
		v, has := c.Get(fmt.Sprintf("test%d", i))
		require.False(t, has)
		require.Nil(t, v)
	}
	for i := 0; i < 10; i++ {
		v, has := c.Remove(fmt.Sprintf("test%d", i))
		require.False(t, has)
		require.Nil(t, v)
	}
}
