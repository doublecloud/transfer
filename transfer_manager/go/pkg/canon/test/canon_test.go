package test

import (
	"testing"

	"github.com/doublecloud/transfer/transfer_manager/go/pkg/canon"
)

type nested struct {
	Test string
}
type test struct {
	Name string
	New  interface{}
}

func TestCanonWithNesting(t *testing.T) {
	t.Parallel()
	canon.SaveJSON(t, test{Name: "Something", New: nested{Test: "abc"}})
}

func TestFileNested(t *testing.T) {
	t.Parallel()
	canon.SaveJSON(t, test{Name: "Something", New: `{ "Test": "abc" }`})
}

func TestIgnoreExternal(t *testing.T) {
	t.Parallel()
	t.Run("a", func(t *testing.T) {
		canon.SaveJSON(t, test{Name: "Something"})
	})
	t.Run("b", func(t *testing.T) {
		canon.SaveJSON(t, test{Name: "Something else"})
	})
	t.Run("c", func(t *testing.T) {
		canon.SaveJSON(t, "this test is skipped in general since externally saved result")
	})
}

func TestWithFile(t *testing.T) {
	t.Parallel()
	canon.SaveJSON(t, "abc")
}
