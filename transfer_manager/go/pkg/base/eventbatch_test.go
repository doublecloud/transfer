package base

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIteratorOverBatched_Event(t *testing.T) {
	type dummyEvent struct {
		iter int
	}
	sampleEvs := make([]Event, 10)
	for i := 0; i < 10; i++ {
		sampleEvs[i] = &dummyEvent{iter: i}
	}
	batch := NewBatchFromBatches([]EventBatch{
		NewEventBatch([]Event{sampleEvs[0], sampleEvs[1]}),
		NewEventBatch([]Event{sampleEvs[2], sampleEvs[3], sampleEvs[4]}),
		NewBatchFromBatches([]EventBatch{
			NewEventBatch([]Event{sampleEvs[5], sampleEvs[6]}),
			NewEventBatch([]Event{sampleEvs[7], sampleEvs[8], sampleEvs[9]}),
		}),
	})
	cntr := 0
	for batch.Next() {
		ev, err := batch.Event()
		require.NoError(t, err)
		require.Equal(t, sampleEvs[cntr], ev)
		cntr++
	}
	require.Equal(t, 10, cntr)
}
