package sequencer

import (
	"fmt"
	"strconv"

	"github.com/doublecloud/transfer/pkg/abstract"
)

// BuildMapPartitionToOffsetsRange - is used only in logging
func BuildMapPartitionToOffsetsRange(messages []QueueMessage) string {
	sequencer := NewSequencer()
	_ = sequencer.StartProcessing(messages)
	return sequencer.ToStringRanges()
}

// BuildPartitionOffsetLogLine - is used only in logging
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

// BuildLogMapPartitionToOffsetsRange - is used only in logging
func BuildLogMapPartitionToOffsetsRange(changeItems [][]abstract.ChangeItem) string {
	sumSize := 0
	for _, currArr := range changeItems {
		sumSize += len(currArr)
	}
	messages := make([]QueueMessage, 0, sumSize)
	for _, currArr := range changeItems {
		for _, changeItem := range currArr {
			partitionID, _ := strconv.Atoi(changeItem.PartID)
			messages = append(messages, QueueMessage{Topic: changeItem.Table, Partition: partitionID, Offset: int64(changeItem.LSN)})
		}
	}

	sequencer := NewSequencer()
	_ = sequencer.StartProcessing(messages)
	return sequencer.ToStringRangesWithTopic()
}
