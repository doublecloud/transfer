package sequencer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartitionToOffsets(t *testing.T) {
	p := partitionToOffsets{
		topic:     "a",
		partition: 0,
		offsets:   make([]int64, 0),
	}

	require.NoError(t, p.appendOffset(2))
	require.NoError(t, p.appendOffset(3))
	require.NoError(t, p.appendOffset(7))

	require.Equal(t, int64(1), p.committedTo())

	require.NoError(t, p.removeOffsets(map[int64]bool{3: true}))
	require.Equal(t, int64(1), p.committedTo())

	require.NoError(t, p.removeOffsets(map[int64]bool{7: true}))
	require.Equal(t, int64(1), p.committedTo())

	require.NoError(t, p.removeOffsets(map[int64]bool{2: true}))
	require.Equal(t, int64(7), p.committedTo())
}

func TestSequences(t *testing.T) {
	sequences := NewSequencer()
	require.NoError(t, sequences.StartProcessing([]QueueMessage{
		{Topic: "a", Partition: 0, Offset: 1},
	}))
	require.NoError(t, sequences.StartProcessing([]QueueMessage{
		{Topic: "a", Partition: 0, Offset: 2},
	}))
	require.NoError(t, sequences.StartProcessing([]QueueMessage{
		{Topic: "a", Partition: 0, Offset: 3},
	}))

	msgs1, err := sequences.Pushed([]QueueMessage{
		{Topic: "a", Partition: 0, Offset: 1},
	})
	require.NoError(t, err)
	require.Len(t, msgs1, 1)
	require.Equal(t, msgs1[0].Topic, "a")
	require.Equal(t, msgs1[0].Partition, 0)
	require.Equal(t, msgs1[0].Offset, int64(1))

	msgs2, err := sequences.Pushed([]QueueMessage{
		{Topic: "a", Partition: 0, Offset: 3},
	})
	require.NoError(t, err)
	require.Len(t, msgs2, 0)

	msgs3, err := sequences.Pushed([]QueueMessage{
		{Topic: "a", Partition: 0, Offset: 2},
	})
	require.NoError(t, err)
	require.Len(t, msgs3, 1)
	require.Equal(t, msgs3[0].Topic, "a")
	require.Equal(t, msgs3[0].Partition, 0)
	require.Equal(t, msgs3[0].Offset, int64(3))
}

func makeArr(in []int) []QueueMessage {
	result := make([]QueueMessage, 0, len(in))
	for _, el := range in {
		result = append(result, QueueMessage{Topic: "a", Partition: 0, Offset: int64(el)})
	}
	return result
}

func compare(t *testing.T, s string, in []int) {
	sequences := NewSequencer()
	require.NoError(t, sequences.StartProcessing(makeArr(in)))
	require.Equal(t, s, sequences.ToStringRanges())
}

func TestSequencesToString(t *testing.T) {
	compare(t, "0:1", []int{1})
	compare(t, "0:2", []int{2})

	compare(t, "0:1,3", []int{1, 3})
	compare(t, "0:2-3", []int{2, 3})

	compare(t, "0:1,3-4", []int{1, 3, 4})
	compare(t, "0:1-2,5", []int{1, 2, 5})

	compare(t, "0:1-2,4-5", []int{1, 2, 4, 5})
}
