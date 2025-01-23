package sequencer

import (
	"fmt"
)

// BuildMapPartitionToOffsetsRange - is used only in logging.
func BuildMapPartitionToOffsetsRange(messages []QueueMessage) string {
	sequencer := NewSequencer()
	_ = sequencer.StartProcessing(messages)
	return sequencer.ToStringRanges()
}

// BuildPartitionOffsetLogLine - is used only in logging.
func BuildPartitionOffsetLogLine(messages []QueueMessage) string {
	if len(messages) == 0 {
		return ""
	}
	result := ""
	for _, message := range messages {
		result += fmt.Sprintf("%d:%d,", message.Partition, message.Offset)
	}
	return result[0 : len(result)-1]
}

// BuildMapTopicPartitionToOffsetsRange - is used only in logging.
func BuildMapTopicPartitionToOffsetsRange(messages []QueueMessage) string {
	sequencer := NewSequencer()
	_ = sequencer.StartProcessing(messages)
	return sequencer.ToStringRangesWithTopic()
}
