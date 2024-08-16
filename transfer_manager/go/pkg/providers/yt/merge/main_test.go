package merge

import (
	"os"
	"testing"

	ytcommon "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yt"
)

func TestMain(m *testing.M) {
	ytcommon.InitExe()
	os.Exit(m.Run())
}
