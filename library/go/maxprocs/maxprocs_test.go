package maxprocs_test

import (
	"os"
	"runtime"
	"testing"

	"github.com/doublecloud/transfer/library/go/maxprocs"
	"github.com/stretchr/testify/assert"
)

var (
	originalMaxProcs = runtime.GOMAXPROCS(0)
)

func TestAdjustQloud(t *testing.T) {
	defer reset()

	err := os.Setenv("GOMAXPROCS", "3")
	if !assert.NoError(t, err, "failed to set GOMAXPROCS") {
		return
	}

	maxprocs.AdjustQloud()
	assert.Equal(t, 3, runtime.GOMAXPROCS(0))

	err = os.Unsetenv("GOMAXPROCS")
	if !assert.NoError(t, err, "failed to unset GOMAXPROCS") {
		return
	}

	err = os.Setenv("QLOUD_CPU_GUARANTEE", "2")
	if !assert.NoError(t, err, "failed to set QLOUD_CPU_GUARANTEE") {
		return
	}

	maxprocs.AdjustQloud()
	assert.Equal(t, 2, runtime.GOMAXPROCS(0))
}

func TestAdjustInstancectl(t *testing.T) {
	defer reset()

	err := os.Setenv("GOMAXPROCS", "3")
	if !assert.NoError(t, err, "failed to set GOMAXPROCS") {
		return
	}

	maxprocs.AdjustInstancectl()
	assert.Equal(t, 3, runtime.GOMAXPROCS(0))

	err = os.Unsetenv("GOMAXPROCS")
	if !assert.NoError(t, err, "failed to unset GOMAXPROCS") {
		return
	}

	err = os.Setenv("CPU_GUARANTEE", "2.000c")
	if !assert.NoError(t, err, "failed to set CPU_GUARANTEE") {
		return
	}

	maxprocs.AdjustInstancectl()
	assert.Equal(t, 2, runtime.GOMAXPROCS(0))
}

func TestAdjustAuto(t *testing.T) {
	defer reset()

	err := os.Setenv("GOMAXPROCS", "3")
	if !assert.NoError(t, err, "failed to set GOMAXPROCS") {
		return
	}

	maxprocs.AdjustInstancectl()
	assert.Equal(t, 3, runtime.GOMAXPROCS(0))

	err = os.Unsetenv("GOMAXPROCS")
	if !assert.NoError(t, err, "failed to unset GOMAXPROCS") {
		return
	}

	err = os.Setenv("CPU_GUARANTEE", "2.500c")
	if !assert.NoError(t, err, "failed to set CPU_GUARANTEE") {
		return
	}

	maxprocs.AdjustInstancectl()
	assert.Equal(t, 2, runtime.GOMAXPROCS(0))

	err = os.Unsetenv("CPU_GUARANTEE")
	if !assert.NoError(t, err, "failed to unset CPU_GUARANTEE") {
		return
	}

	err = os.Setenv("QLOUD_CPU_GUARANTEE", "3")
	if !assert.NoError(t, err, "failed to set QLOUD_CPU_GUARANTEE") {
		return
	}

	maxprocs.AdjustQloud()
	assert.Equal(t, 3, runtime.GOMAXPROCS(0))
}

func TestAdjust(t *testing.T) {
	defer reset()

	maxprocs.Adjust(1)
	assert.Equal(t, maxprocs.MinProc, runtime.GOMAXPROCS(0))

	maxprocs.Adjust(10000)
	assert.Equal(t, runtime.NumCPU(), runtime.GOMAXPROCS(0))
}

func reset() {
	_ = os.Unsetenv(maxprocs.GoMaxProcEnvName)
	_ = os.Unsetenv(maxprocs.InstancectlCPUEnvName)
	_ = os.Unsetenv(maxprocs.QloudCPUEnvName)
	runtime.GOMAXPROCS(originalMaxProcs)
}
