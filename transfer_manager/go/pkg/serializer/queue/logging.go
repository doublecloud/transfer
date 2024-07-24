package queue

import (
	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util"
)

func logErrorWithChangeItem(logger log.Logger, msg string, err error, changeItem *abstract.ChangeItem) {
	changeItemJSON := changeItem.ToJSONString()
	logger.Error(msg, log.Error(err), log.Any("change_item", util.DefaultSample(changeItemJSON)))
}
