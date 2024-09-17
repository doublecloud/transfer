package changeitem

import (
	"sync"

	"github.com/doublecloud/transfer/pkg/util"
)

var (
	systemTables   = util.NewSet[string]()
	systemTablesMu = &sync.RWMutex{}
)

func SystemTables() []string {
	systemTablesMu.RLock()
	defer systemTablesMu.RUnlock()
	return systemTables.Slice()
}

func RegisterSystemTables(tableNames ...string) {
	systemTablesMu.Lock()
	defer systemTablesMu.Unlock()
	systemTables.AddRange(tableNames...)
}

func IsSystemTable(in string) bool {
	systemTablesMu.RLock()
	defer systemTablesMu.RUnlock()
	return systemTables.Contains(in)
}
