//go:build !arcadia
// +build !arcadia

package yatest

func doInit() {
	isRunningUnderGoTest = true
	context.Initialized = true
	context.Runtime.SourceRoot = ""
}
