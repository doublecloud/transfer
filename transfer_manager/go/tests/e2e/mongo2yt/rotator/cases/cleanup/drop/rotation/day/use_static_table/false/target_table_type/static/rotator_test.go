package case1

import (
	"os"
	"testing"
	"time"

	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	ytcommon "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yt"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/e2e/mongo2yt/rotator"
	"go.ytsaurus.tech/yt/go/ypath"
)

func TestMain(m *testing.M) {
	ytcommon.InitExe()
	os.Exit(m.Run())
}

func TestCases(t *testing.T) {

	// fix time with modern but certain point
	// Note that rotator may delete tables if date is too far away, so 'now' value is strongly recommended
	ts := time.Now()

	table := abstract.TableID{Namespace: "db", Name: "test"}
	dayRotationExpectedTable := rotator.DayRotation.AnnotateWithTime("db_test", ts)

	t.Run("cleanup=drop;rotation=day;use_static_table=false;table_type=static", func(t *testing.T) {
		source, target := rotator.PrefilledSourceAndTarget()
		target.Cleanup = server.Drop
		target.Rotation = rotator.DayRotation
		target.UseStaticTableOnSnapshot = false
		target.Static = true
		expectedPath := ypath.Path(target.Path).Child(dayRotationExpectedTable)
		rotator.ScenarioCheckActivation(t, source, target, table, ts, expectedPath)
	})
}
