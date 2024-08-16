package ratelimiter

import (
	"context"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_RateLimiter"
)

type HierarchicalDrrSettings struct {
	MaxUnitsPerSecond       float64
	MaxBurstSizeCoefficient float64
	PrefetchCoefficient     float64
	PrefetchWatermark       float64
}

type Resource struct {
	ResourcePath    string
	HierarchicalDrr HierarchicalDrrSettings
}

type Client struct {
	Driver ydb.Driver
}

func (c *Client) CreateResource(ctx context.Context, coordinationNodePath string, resource Resource) (err error) {
	req := Ydb_RateLimiter.CreateResourceRequest{
		CoordinationNodePath: coordinationNodePath,
		Resource: &Ydb_RateLimiter.Resource{
			ResourcePath: resource.ResourcePath,
			Type: &Ydb_RateLimiter.Resource_HierarchicalDrr{HierarchicalDrr: &Ydb_RateLimiter.HierarchicalDrrSettings{
				MaxUnitsPerSecond:       resource.HierarchicalDrr.MaxUnitsPerSecond,
				MaxBurstSizeCoefficient: resource.HierarchicalDrr.MaxBurstSizeCoefficient,
				PrefetchCoefficient:     resource.HierarchicalDrr.PrefetchCoefficient,
				PrefetchWatermark:       resource.HierarchicalDrr.PrefetchWatermark,
			}},
		},
	}
	_, err = c.Driver.Call(ctx, ydb.Wrap(
		"/Ydb.RateLimiter.V1.RateLimiterService/CreateResource", &req, nil,
	))
	return
}

func (c *Client) AlterResource(ctx context.Context, coordinationNodePath string, resource Resource) (err error) {
	req := Ydb_RateLimiter.AlterResourceRequest{
		CoordinationNodePath: coordinationNodePath,
		Resource: &Ydb_RateLimiter.Resource{
			ResourcePath: resource.ResourcePath,
			Type: &Ydb_RateLimiter.Resource_HierarchicalDrr{HierarchicalDrr: &Ydb_RateLimiter.HierarchicalDrrSettings{
				MaxUnitsPerSecond:       resource.HierarchicalDrr.MaxUnitsPerSecond,
				MaxBurstSizeCoefficient: resource.HierarchicalDrr.MaxBurstSizeCoefficient,
				PrefetchCoefficient:     resource.HierarchicalDrr.PrefetchCoefficient,
				PrefetchWatermark:       resource.HierarchicalDrr.PrefetchWatermark,
			}},
		},
	}
	_, err = c.Driver.Call(ctx, ydb.Wrap(
		"/Ydb.RateLimiter.V1.RateLimiterService/AlterResource", &req, nil,
	))
	return
}

func (c *Client) DropResource(ctx context.Context, coordinationNodePath string, resourcePath string) (err error) {
	req := Ydb_RateLimiter.DropResourceRequest{
		OperationParams:      nil,
		CoordinationNodePath: coordinationNodePath,
		ResourcePath:         resourcePath,
	}
	_, err = c.Driver.Call(ctx, ydb.Wrap(
		"/Ydb.RateLimiter.V1.RateLimiterService/DropResource", &req, nil,
	))
	return
}

func (c *Client) ListResource(ctx context.Context, coordinationNodePath string, resourcePath string, recursive bool) ([]string, error) {
	var res Ydb_RateLimiter.ListResourcesResult
	req := Ydb_RateLimiter.ListResourcesRequest{
		CoordinationNodePath: coordinationNodePath,
		ResourcePath:         resourcePath,
	}
	_, err := c.Driver.Call(ctx, ydb.Wrap(
		"/Ydb.RateLimiter.V1.RateLimiterService/ListResources", &req, &res,
	))
	if err != nil {
		return nil, err
	}
	return res.ResourcePaths, nil
}

func (c *Client) DescribeResource(ctx context.Context, coordinationNodePath string, resourcePath string) (*Resource, error) {
	var res Ydb_RateLimiter.DescribeResourceResult
	req := Ydb_RateLimiter.DescribeResourceRequest{
		CoordinationNodePath: coordinationNodePath,
		ResourcePath:         resourcePath,
	}
	_, err := c.Driver.Call(ctx, ydb.Wrap(
		"/Ydb.RateLimiter.V1.RateLimiterService/DescribeResource", &req, &res,
	))
	if err != nil {
		return nil, err
	}

	result := &Resource{
		ResourcePath: res.Resource.ResourcePath,
	}

	if res.Resource.GetHierarchicalDrr() != nil {
		result.HierarchicalDrr = HierarchicalDrrSettings{
			MaxUnitsPerSecond:       res.Resource.GetHierarchicalDrr().MaxUnitsPerSecond,
			MaxBurstSizeCoefficient: res.Resource.GetHierarchicalDrr().MaxBurstSizeCoefficient,
			PrefetchCoefficient:     res.Resource.GetHierarchicalDrr().PrefetchCoefficient,
			PrefetchWatermark:       res.Resource.GetHierarchicalDrr().PrefetchWatermark,
		}
	}

	return result, nil
}

func (c *Client) AcquireResource(ctx context.Context, coordinationNodePath string, resourcePath string, amount uint64, isUsedAmount bool) (err error) {
	var req Ydb_RateLimiter.AcquireResourceRequest
	if isUsedAmount {
		req = Ydb_RateLimiter.AcquireResourceRequest{
			CoordinationNodePath: coordinationNodePath,
			ResourcePath:         resourcePath,
			Units:                &Ydb_RateLimiter.AcquireResourceRequest_Used{Used: amount},
		}
	} else {
		req = Ydb_RateLimiter.AcquireResourceRequest{
			CoordinationNodePath: coordinationNodePath,
			ResourcePath:         resourcePath,
			Units:                &Ydb_RateLimiter.AcquireResourceRequest_Required{Required: amount},
		}
	}
	_, err = c.Driver.Call(ctx, ydb.Wrap(
		"/Ydb.RateLimiter.V1.RateLimiterService/AcquireResource", &req, nil,
	))
	return
}
