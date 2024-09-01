package maxprocs

import (
	"os"
	"runtime"
	"strings"
)

const (
	SafeProc = 4
	MinProc  = 2
	MaxProc  = 8

	GoMaxProcEnvName      = "GOMAXPROCS"
	QloudCPUEnvName       = "QLOUD_CPU_GUARANTEE"
	InstancectlCPUEnvName = "CPU_GUARANTEE"
)

// Adjust adjust the maximum number of CPUs that can be executing.
// Takes a minimum between n and CPU counts and returns the previous setting
func Adjust(n int) int {
	if n < MinProc {
		n = MinProc
	}

	nCPU := runtime.NumCPU()
	if n < nCPU {
		return runtime.GOMAXPROCS(n)
	}

	return runtime.GOMAXPROCS(nCPU)
}

// AdjustAuto automatically adjust the maximum number of CPUs that can be executing to safe value
// and returns the previous setting
func AdjustAuto() int {
	if val, ok := getEnv(GoMaxProcEnvName); ok {
		return applyIntStringLimit(val)
	}

	if isCgroupsExists() {
		return AdjustCgroup()
	}

	if val, ok := getEnv(InstancectlCPUEnvName); ok {
		return applyFloatStringLimit(strings.TrimRight(val, "c"))
	}

	if val, ok := getEnv(QloudCPUEnvName); ok {
		return applyFloatStringLimit(val)
	}

	return Adjust(SafeProc)
}

func getEnv(envName string) (string, bool) {
	val, ok := os.LookupEnv(envName)
	return val, ok && val != ""
}

// AdjustCgroup automatically adjust the maximum number of CPUs based on the CFS quota
// and returns the previous setting.
func AdjustCgroup() int {
	if val, ok := getEnv(GoMaxProcEnvName); ok {
		return applyIntStringLimit(val)
	}

	quota, err := currentCFSQuota()
	if err != nil {
		return Adjust(SafeProc)
	}

	return applyFloatLimit(quota)
}
