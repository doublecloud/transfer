package abstract

import (
	"encoding/gob"
	"encoding/json"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
)

var knownRuntimes = map[RuntimeType]func(spec string) (Runtime, error){}

func init() {
	gob.RegisterName("*abstract.LocalRuntime", new(LocalRuntime))
}

type YtCluster string

type RuntimeType string

const (
	LocalRuntimeType = RuntimeType("local")
)

type Runtime interface {
	NeedRestart(runtime Runtime) bool
	WithDefaults()
	Validate() error
	Type() RuntimeType
	SetVersion(runtimeSpecificVersion string, versionProperties *string) error
}

func NewRuntime(runtime RuntimeType, runtimeSpec string) (Runtime, error) {
	switch runtime {
	case LocalRuntimeType:
		var res LocalRuntime
		if err := json.Unmarshal([]byte(runtimeSpec), &res); err != nil {
			return nil, xerrors.Errorf("local: %w", err)
		}
		return &res, nil
	default:
		f, ok := knownRuntimes[runtime]
		if !ok {
			return nil, xerrors.Errorf("unable to parse runtime of type: %v", runtime)
		}
		return f(runtimeSpec)
	}
}

func RegisterRuntime(r RuntimeType, f func(spec string) (Runtime, error)) {
	knownRuntimes[r] = f
}

// Parallelism params
type ShardUploadParams struct {
	JobCount     int //Workers count
	ProcessCount int //Threads count
}

func NewShardUploadParams(jobCount int, processCount int) *ShardUploadParams {
	return &ShardUploadParams{
		JobCount:     jobCount,
		ProcessCount: processCount,
	}
}

func DefaultShardUploadParams() *ShardUploadParams {
	return NewShardUploadParams(1, 4)
}

type ShardingTaskRuntime interface {
	WorkersNum() int
	ThreadsNumPerWorker() int
	CurrentJobIndex() int
	IsMain() bool
}

type ScheduledTask interface {
	Stop()
	Runtime() Runtime
}

type LimitedResourceRuntime interface {
	RAMGuarantee() uint64
	GCPercentage() int
	ResourceLimiterEnabled() bool
}
