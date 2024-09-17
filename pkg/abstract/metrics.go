package abstract

import (
	"fmt"
	"strings"

	"github.com/doublecloud/transfer/library/go/core/metrics"
)

func Rows(metrics metrics.Registry, table string, rows int) {
	if !strings.Contains(table, TableConsumerKeeper) {
		metrics.Counter(fmt.Sprintf("sink.table.%v.rows", table)).Add(int64(rows))
	}
}
