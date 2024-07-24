package ratelimiter

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/coordination"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/internal/traceutil"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/table"
)

const (
	testCoordinationNodePath = "/local/test"
	testResource             = "test_res"
)

func openDB(ctx context.Context) (ydb.Driver, error) {
	var (
		dtrace ydb.DriverTrace
		ctrace table.ClientTrace
		strace table.SessionPoolTrace
	)
	traceutil.Stub(&dtrace, func(name string, args ...interface{}) {
		log.Printf("[driver] %s: %+v", name, traceutil.ClearContext(args))
	})
	traceutil.Stub(&ctrace, func(name string, args ...interface{}) {
		log.Printf("[client] %s: %+v", name, traceutil.ClearContext(args))
	})
	traceutil.Stub(&strace, func(name string, args ...interface{}) {
		log.Printf("[session] %s: %+v", name, traceutil.ClearContext(args))
	})

	dialer := &ydb.Dialer{
		DriverConfig: &ydb.DriverConfig{
			Database: "/local",
			Credentials: ydb.AuthTokenCredentials{
				AuthToken: os.Getenv("YDB_TOKEN"),
			},
			Trace:                dtrace,
			RequestTimeout:       time.Second * 2,
			StreamTimeout:        time.Second * 2,
			OperationTimeout:     time.Second * 2,
			OperationCancelAfter: time.Second * 2,
			DiscoveryInterval:    0,
			BalancingMethod:      0,
			BalancingConfig:      nil,
			PreferLocalEndpoints: false,
		},
		TLSConfig: nil,
		Timeout:   time.Second * 2,
	}
	driver, err := dialer.Dial(ctx, "localhost:2135")
	if err != nil {
		return nil, fmt.Errorf("dial error: %w", err)
	}

	return driver, nil
}

func TestRateLimiter(t *testing.T) {
	t.Skip("need to be tested with docker")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	driver, err := openDB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = driver.Close()
	}()

	client := Client{Driver: driver}
	coordClient := coordination.Client{Driver: driver}

	err = coordClient.DropNode(ctx, testCoordinationNodePath)
	err = coordClient.CreateNode(ctx, testCoordinationNodePath, coordination.Config{
		Path:                     "",
		SelfCheckPeriodMillis:    1000,
		SessionGracePeriodMillis: 1000,
		ReadConsistencyMode:      coordination.ConsistencyModeRelaxed,
		AttachConsistencyMode:    coordination.ConsistencyModeRelaxed,
		RateLimiterCountersMode:  coordination.RateLimiterCountersModeDetailed,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = coordClient.DropNode(ctx, testCoordinationNodePath)
		if err != nil {
			t.Fatal(err)
		}
	}()

	err = client.CreateResource(ctx, testCoordinationNodePath, Resource{
		ResourcePath: testResource,
		HierarchicalDrr: HierarchicalDrrSettings{
			MaxUnitsPerSecond:       1,
			MaxBurstSizeCoefficient: 2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = client.DropResource(ctx, testCoordinationNodePath, testResource)
		if err != nil {
			t.Fatal("Cannot drop resource")
		}
	}()

	descibed, err := client.DescribeResource(ctx, testCoordinationNodePath, testResource)
	if err != nil {
		t.Fatal(err)
	}
	if descibed == nil ||
		descibed.ResourcePath != testResource ||
		descibed.HierarchicalDrr.MaxUnitsPerSecond != 1.0 ||
		descibed.HierarchicalDrr.MaxBurstSizeCoefficient != 2.0 {
		t.Fatal("Resource invalid")
	}

	err = client.AlterResource(ctx, testCoordinationNodePath, Resource{
		ResourcePath: testResource,
		HierarchicalDrr: HierarchicalDrrSettings{
			MaxUnitsPerSecond:       3,
			MaxBurstSizeCoefficient: 4,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	descibed, err = client.DescribeResource(ctx, testCoordinationNodePath, testResource)
	if err != nil {
		t.Fatal(err)
	}
	if descibed == nil ||
		descibed.ResourcePath != testResource ||
		descibed.HierarchicalDrr.MaxUnitsPerSecond != 3.0 ||
		descibed.HierarchicalDrr.MaxBurstSizeCoefficient != 4.0 {
		t.Fatal("Resource invalid")
	}

	list, err := client.ListResource(ctx, testCoordinationNodePath, testResource, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0] != testResource {
		t.Fatal("ListResource error")
	}

	err = client.AcquireResource(ctx, testCoordinationNodePath, testResource, 1, false)
	if err != nil {
		t.Fatal(err)
	}
	err = client.AcquireResource(ctx, testCoordinationNodePath, testResource, 10000, true)
	if err != nil {
		t.Fatal(err)
	}
	err = client.AcquireResource(ctx, testCoordinationNodePath, testResource, 10000, false)
	if err == nil {
		t.Fatal("Resource must not be acquired")
	}
}
