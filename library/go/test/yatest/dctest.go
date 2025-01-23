//go:build !arcadia
// +build !arcadia

package yatest

func doInit() {
	context.Initialized = true
	context.Runtime.SourceRoot = ""
}
