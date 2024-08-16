//go:build cgo && oracle
// +build cgo,oracle

package logminer

import (
	"strings"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/godror/godror"
)

func IsContainsError(err error, skipCodes []string) bool {
	oraErr := new(godror.OraErr)
	if xerrors.As(err, &oraErr) {
		for _, errorCode := range skipErrorCodes {
			if strings.Contains(oraErr.Message(), errorCode) {
				return true
			}
		}
	}
	return false
}
