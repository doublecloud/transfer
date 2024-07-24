package stats

import (
	"math"
	"sync"
)

type syncStat struct{ target, source uint64 }

var (
	rw   = sync.Mutex{}
	data = map[string]syncStat{}
)

func sourceRows(table string, rows uint64) float64 {
	rw.Lock()
	defer rw.Unlock()
	data[table] = syncStat{
		source: rows,
		target: data[table].target,
	}
	if data[table].target > 0 && data[table].source > 0 {
		return math.Abs(float64(data[table].target-data[table].source)) / float64(data[table].source)
	}
	return 0
}

func targetRows(table string, rows uint64) {
	rw.Lock()
	defer rw.Unlock()
	data[table] = syncStat{
		target: rows,
		source: data[table].source,
	}
}
