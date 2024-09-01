package maxprocs

import (
	"bytes"
	"math"
	"os"
	"strconv"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
)

func applyIntStringLimit(val string) int {
	maxProc, err := strconv.Atoi(val)
	if err == nil {
		return Adjust(maxProc)
	}

	return Adjust(SafeProc)
}

func applyFloatStringLimit(val string) int {
	maxProc, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return Adjust(SafeProc)
	}

	return applyFloatLimit(maxProc)
}

func applyFloatLimit(val float64) int {
	maxProc := int(math.Floor(val))
	return Adjust(maxProc)
}

func readFileInt(filename string) (int, error) {
	raw, err := os.ReadFile(filename)
	if err != nil {
		return 0, xerrors.Errorf("unable to read: %s: %w", filename, err)
	}

	return strconv.Atoi(string(bytes.TrimSpace(raw)))
}
