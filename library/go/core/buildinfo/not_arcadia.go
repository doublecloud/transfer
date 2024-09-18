//go:build !arcadia
// +build !arcadia

package buildinfo

// buildinfo is empty stub used when library is not built by ya make.
var buildinfo map[string]string

func init() {
	InitBuildInfo(buildinfo)
}
