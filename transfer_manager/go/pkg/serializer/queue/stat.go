package queue

import (
	"time"

	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	util "github.com/doublecloud/tross/transfer_manager/go/pkg/util/queues"
)

func LogBatchingStat(logger log.Logger, input []abstract.ChangeItem, in map[abstract.TablePartID][]SerializedMessage, startTime time.Time) {
	sumMessages := 0
	statByMessages := make(map[string]int)
	sumSize := uint64(0)
	statBySize := make(map[string]uint64)
	for k, v := range in {
		fqtn := util.UnquotedFQTNWithPartID(k)
		sumMessages += len(v)
		statByMessages[fqtn] = len(v)
		currTableSize := uint64(0)
		for _, el := range v {
			currTableSize += uint64(len(el.Key)) + uint64(len(el.Value))
		}
		sumSize += currTableSize
		statBySize[fqtn] = currTableSize
	}
	logger.Info(
		"Serialized",
		log.Int("#change_items", len(input)),
		log.Int("#all_messages", sumMessages),
		log.Any("stat_by_messages", statByMessages),
		log.UInt64("#bytes", sumSize),
		log.Any("stat_by_size", statBySize),
		log.String("duration", time.Since(startTime).String()),
	)
}
