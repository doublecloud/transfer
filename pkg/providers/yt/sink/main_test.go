package sink

import (
	"os"
	"testing"

	"github.com/doublecloud/transfer/pkg/config/env"
	ytcommon "github.com/doublecloud/transfer/pkg/providers/yt"
)

func TestMain(m *testing.M) {
	if env.IsTest() {
		ytcommon.InitExe()
	}
	os.Exit(m.Run())
}
