package persqueue

import (
	"testing"
)

func TestBuffer_Push_Limit(t *testing.T) {
	size10kb := 10 * 1024
	size5kb := 5 * 1024

	b := newCommitBuffer(size10kb)
	b.AddCookie(PartitionCookie{PartitionCookie: 1}, size5kb)
	b.AddCookie(PartitionCookie{PartitionCookie: 2}, size5kb)
	b.AddCookie(PartitionCookie{PartitionCookie: 3}, size5kb)
	if b.InLimits() {
		t.Error("Must be in limits")
	}
}

func TestBuffer_Enqueue_Limit(t *testing.T) {
	size10kb := 10 * 1024
	size5kb := 5 * 1024

	b := newCommitBuffer(size10kb)
	b.AddCookie(PartitionCookie{PartitionCookie: 1}, size5kb)
	b.AddCookie(PartitionCookie{PartitionCookie: 2}, size5kb)
	b.AddCookie(PartitionCookie{PartitionCookie: 3}, size5kb)
	if b.InLimits() {
		t.Error("Must be off limits")
	}

	b.EnqueueCommit(PartitionCookie{PartitionCookie: 3}, size5kb)
	if !b.InLimits() {
		t.Error("Must be in limits")
	}
}
