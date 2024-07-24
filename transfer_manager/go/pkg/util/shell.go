package util

import (
	"strings"
)

// Simplified version of https://github.com/alessio/shellescape/blob/master/shellescape.go
func ShellQuote(s string) string {
	return `'` + strings.ReplaceAll(s, `'`, `'"'"'`) + `'`
}
