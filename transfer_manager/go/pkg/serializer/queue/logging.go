package queue

import (
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

func logErrorWithChangeItem(logger log.Logger, msg string, err error, changeItem *abstract.ChangeItem) {
	changeItemJSON := changeItem.ToJSONString()
	logger.Error(msg, log.Error(err), log.Any("change_item", util.DefaultSample(changeItemJSON)))
}
