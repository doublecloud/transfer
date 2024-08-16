//go:build arcadia
// +build arcadia

package gotoolchain

import (
	"fmt"
	"os"
	"runtime"

	"github.com/doublecloud/transfer/library/go/test/yatest"
)

func Setup(setEnv func(k, v string) error) error {
	var osSdk string
	if runtime.GOOS == "linux" {
		osSdk = yatest.GlobalResourcePath("OS_SDK_ROOT_RESOURCE_GLOBAL")
	} else if runtime.GOOS == "darwin" {
		osSdk = yatest.GlobalResourcePath("MACOS_SDK_RESOURCE_GLOBAL") + "/MacOSX10.11.sdk"
	}
	goTools := yatest.GlobalResourcePath("GO_TOOLS_RESOURCE_GLOBAL")

	if err := setEnv("PATH", osSdk+"/usr/bin:"+goTools+"/bin:"+os.Getenv("PATH")); err != nil {
		return err
	}

	lldRoot, ok := yatest.RelaxedGlobalResourcePath("LLD_ROOT_RESOURCE_GLOBAL")
	var ldArg string
	if ok {
		ldArg = fmt.Sprintf("-fuse-ld=%s/ld", lldRoot)
	}

	args := fmt.Sprintf("--sysroot=%s %s -Wl,--no-rosegment -Wno-unused-command-line-argument", osSdk, ldArg)
	if err := setEnv("CC", yatest.CCompilerPath()+" "+args); err != nil {
		return err
	}
	if err := setEnv("CXX", yatest.CxxCompilerPath()+" "+args); err != nil {
		return err
	}
	if err := setEnv("GOROOT", goTools); err != nil {
		return err
	}

	home, err := os.MkdirTemp(yatest.WorkPath(""), "home")
	if err != nil {
		return err
	}

	return setEnv("HOME", home)
}
