package coordination

import (
	"context"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Coordination"
)

type ConsistencyMode uint

const (
	ConsistencyModeUnset ConsistencyMode = iota
	ConsistencyModeStrict
	ConsistencyModeRelaxed
)

func (t ConsistencyMode) String() string {
	switch t {
	default:
		return "Unknown"
	case ConsistencyModeUnset:
		return "Unset"
	case ConsistencyModeStrict:
		return "Strict"
	case ConsistencyModeRelaxed:
		return "Relaxed"
	}
}

type RateLimiterCountersMode uint

const (
	RateLimiterCountersModeUnset RateLimiterCountersMode = iota
	RateLimiterCountersModeAggregated
	RateLimiterCountersModeDetailed
)

func (t RateLimiterCountersMode) String() string {
	switch t {
	default:
		return "Unknown"
	case RateLimiterCountersModeUnset:
		return "Unset"
	case RateLimiterCountersModeAggregated:
		return "Aggregated"
	case RateLimiterCountersModeDetailed:
		return "Detailed"
	}
}

type Config struct {
	Path                     string
	SelfCheckPeriodMillis    uint32
	SessionGracePeriodMillis uint32
	ReadConsistencyMode      ConsistencyMode
	AttachConsistencyMode    ConsistencyMode
	RateLimiterCountersMode  RateLimiterCountersMode
}

type Client struct {
	Driver ydb.Driver
}

func (c *Client) CreateNode(ctx context.Context, path string, config Config) (err error) {
	req := Ydb_Coordination.CreateNodeRequest{
		Path: path,
		Config: &Ydb_Coordination.Config{
			Path:                     config.Path,
			SelfCheckPeriodMillis:    config.SelfCheckPeriodMillis,
			SessionGracePeriodMillis: config.SessionGracePeriodMillis,
			ReadConsistencyMode:      config.ReadConsistencyMode.to(),
			AttachConsistencyMode:    config.AttachConsistencyMode.to(),
			RateLimiterCountersMode:  config.RateLimiterCountersMode.to(),
		},
	}
	_, err = c.Driver.Call(ctx, ydb.Wrap(
		"/Ydb.Coordination.V1.CoordinationService/CreateNode", &req, nil,
	))
	return
}

func (c *Client) AlterNode(ctx context.Context, path string, config Config) (err error) {
	req := Ydb_Coordination.AlterNodeRequest{
		Path: path,
		Config: &Ydb_Coordination.Config{
			Path:                     config.Path,
			SelfCheckPeriodMillis:    config.SelfCheckPeriodMillis,
			SessionGracePeriodMillis: config.SessionGracePeriodMillis,
			ReadConsistencyMode:      config.ReadConsistencyMode.to(),
			AttachConsistencyMode:    config.AttachConsistencyMode.to(),
			RateLimiterCountersMode:  config.RateLimiterCountersMode.to(),
		},
	}
	_, err = c.Driver.Call(ctx, ydb.Wrap(
		"/Ydb.Coordination.V1.CoordinationService/AlterNode", &req, nil,
	))
	return
}

func (c *Client) DropNode(ctx context.Context, path string) (err error) {
	req := Ydb_Coordination.DropNodeRequest{
		Path: path,
	}
	_, err = c.Driver.Call(ctx, ydb.Wrap(
		"/Ydb.Coordination.V1.CoordinationService/DropNode", &req, nil,
	))
	return
}

// Describes a coordination node
func (c *Client) DescribeNode(ctx context.Context, path string) (*scheme.Entry, *Config, error) {
	var res Ydb_Coordination.DescribeNodeResult
	req := Ydb_Coordination.DescribeNodeRequest{
		Path: path,
	}
	_, err := c.Driver.Call(ctx, ydb.Wrap(
		"/Ydb.Coordination.V1.CoordinationService/DescribeNode", &req, &res,
	))
	if err != nil {
		return nil, nil, err
	}
	return scheme.InnerConvertEntry(res.Self), &Config{
		Path:                     res.Config.Path,
		SelfCheckPeriodMillis:    res.Config.SelfCheckPeriodMillis,
		SessionGracePeriodMillis: res.Config.SessionGracePeriodMillis,
		ReadConsistencyMode:      consistencyMode(res.Config.ReadConsistencyMode),
		AttachConsistencyMode:    consistencyMode(res.Config.AttachConsistencyMode),
		RateLimiterCountersMode:  rateLimiterCountersMode(res.Config.RateLimiterCountersMode),
	}, nil

}

func (t ConsistencyMode) to() Ydb_Coordination.ConsistencyMode {
	switch t {
	case ConsistencyModeStrict:
		return Ydb_Coordination.ConsistencyMode_CONSISTENCY_MODE_STRICT
	case ConsistencyModeRelaxed:
		return Ydb_Coordination.ConsistencyMode_CONSISTENCY_MODE_RELAXED
	default:
		return Ydb_Coordination.ConsistencyMode_CONSISTENCY_MODE_UNSET
	}
}

func consistencyMode(t Ydb_Coordination.ConsistencyMode) ConsistencyMode {
	switch t {
	case Ydb_Coordination.ConsistencyMode_CONSISTENCY_MODE_STRICT:
		return ConsistencyModeStrict
	case Ydb_Coordination.ConsistencyMode_CONSISTENCY_MODE_RELAXED:
		return ConsistencyModeRelaxed
	default:
		return ConsistencyModeUnset
	}
}

func (t RateLimiterCountersMode) to() Ydb_Coordination.RateLimiterCountersMode {
	switch t {
	case RateLimiterCountersModeAggregated:
		return Ydb_Coordination.RateLimiterCountersMode_RATE_LIMITER_COUNTERS_MODE_AGGREGATED
	case RateLimiterCountersModeDetailed:
		return Ydb_Coordination.RateLimiterCountersMode_RATE_LIMITER_COUNTERS_MODE_DETAILED
	default:
		return Ydb_Coordination.RateLimiterCountersMode_RATE_LIMITER_COUNTERS_MODE_UNSET
	}
}

func rateLimiterCountersMode(t Ydb_Coordination.RateLimiterCountersMode) RateLimiterCountersMode {
	switch t {
	case Ydb_Coordination.RateLimiterCountersMode_RATE_LIMITER_COUNTERS_MODE_AGGREGATED:
		return RateLimiterCountersModeAggregated
	case Ydb_Coordination.RateLimiterCountersMode_RATE_LIMITER_COUNTERS_MODE_DETAILED:
		return RateLimiterCountersModeDetailed
	default:
		return RateLimiterCountersModeUnset
	}
}
