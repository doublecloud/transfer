package table

import (
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"
)

// QueryStats holds query execution statistics.
type QueryStats struct {
	stats          *Ydb_TableStats.QueryStats
	processCPUTime time.Duration
	pos            int
}

func (s *QueryStats) init(stats *Ydb_TableStats.QueryStats) {
	if s == nil {
		return
	}
	s.stats = stats
	s.processCPUTime = time.Microsecond * time.Duration(stats.GetProcessCpuTimeUs())
	s.pos = 0
}

func (s *QueryStats) ProcessCPUTime() time.Duration {
	if s == nil {
		return 0
	}
	return s.processCPUTime
}

func (s *QueryStats) Compilation() (c *CompilationStats) {
	if s == nil || s.stats == nil || s.stats.Compilation == nil {
		return nil
	}
	res := new(CompilationStats)
	res.init(s.stats.Compilation)
	return res
}

// CompilationStats holds query compilation statistics.
type CompilationStats struct {
	FromCache bool
	Duration  time.Duration
	CPUTime   time.Duration
}

func (c *CompilationStats) init(x *Ydb_TableStats.CompilationStats) {
	if x == nil {
		return
	}
	c.FromCache = x.FromCache
	c.Duration = time.Microsecond * time.Duration(x.DurationUs)
	c.CPUTime = time.Microsecond * time.Duration(x.CpuTimeUs)
}

// NextPhase returns next execution phase within query.
// If ok flag is false, then there are no more phases and p is invalid.
func (s *QueryStats) NextPhase() (p QueryPhase, ok bool) {
	if s.stats == nil || s.pos >= len(s.stats.QueryPhases) {
		return
	}
	p.init(s.stats.QueryPhases[s.pos])
	s.pos++
	return p, true
}

// QueryPhase holds query execution phase statistics.
type QueryPhase struct {
	Duration       time.Duration
	CPUTime        time.Duration
	AffectedShards uint64

	tables []*Ydb_TableStats.TableAccessStats
	pos    int
}

func (q *QueryPhase) init(x *Ydb_TableStats.QueryPhaseStats) {
	if x == nil {
		return
	}
	q.Duration = time.Microsecond * time.Duration(x.DurationUs)
	q.CPUTime = time.Microsecond * time.Duration(x.CpuTimeUs)
	q.AffectedShards = x.AffectedShards
	q.tables = x.TableAccess
	q.pos = 0
}

// NextTableAccess returns next accessed table within query execution phase.
//
// If ok flag is false, then there are no more accessed tables and t is
// invalid.
func (q *QueryPhase) NextTableAccess() (t TableAccess, ok bool) {
	if q.pos >= len(q.tables) {
		return
	}
	t.init(q.tables[q.pos])
	q.pos++
	return t, true
}

// TableAccess contains query execution phase's table access statistics.
type TableAccess struct {
	Name    string
	Reads   OperationStats
	Updates OperationStats
	Deletes OperationStats
}

func (t *TableAccess) init(x *Ydb_TableStats.TableAccessStats) {
	if x == nil {
		return
	}
	t.Name = x.Name
	t.Reads.init(x.Reads)
	t.Updates.init(x.Updates)
	t.Deletes.init(x.Deletes)
}

type OperationStats struct {
	Rows  uint64
	Bytes uint64
}

func (s *OperationStats) init(x *Ydb_TableStats.OperationStats) {
	if x == nil {
		return
	}
	*s = OperationStats{
		Rows:  x.Rows,
		Bytes: x.Bytes,
	}
}
