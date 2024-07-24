package sink

import (
	"os"
	"testing"

	"github.com/doublecloud/tross/transfer_manager/go/pkg/config/env"
	ytcommon "github.com/doublecloud/tross/transfer_manager/go/pkg/providers/yt"
)

func TestMain(m *testing.M) {
	if env.IsTest() {
		ytcommon.InitExe()
	}
	os.Exit(m.Run())
}
