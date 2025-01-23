//go:build !arcadia
// +build !arcadia

package canon

func init() {
	isRunningUnderGoTest = true
}
