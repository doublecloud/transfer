package table

import "testing"

func TestQueryHasher(t *testing.T) {
	var h queryHasher

	const q = "SELECT 42"
	k1 := h.hash(q)
	k2 := h.hash(q)
	if k1 != k2 {
		t.Errorf("inequal hashes for equal queries: %v vs %v", k1, k2)
	}
}
