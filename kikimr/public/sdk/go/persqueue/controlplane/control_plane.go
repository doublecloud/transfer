package controlplane

import (
	"context"

	Ydb_Persqueue_Protos_V1 "github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/genproto/Ydb_PersQueue_V1"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/session"
)

type ControlPlane interface {
	DescribeTopic(ctx context.Context, topic string) (*Ydb_Persqueue_Protos_V1.DescribeTopicResult, error)
	CreateTopic(ctx context.Context, req *Ydb_Persqueue_Protos_V1.CreateTopicRequest) error
	DropTopic(ctx context.Context, req *Ydb_Persqueue_Protos_V1.DropTopicRequest) error
	AlterTopic(ctx context.Context, req *Ydb_Persqueue_Protos_V1.AlterTopicRequest) error
	AddReadRule(ctx context.Context, req *Ydb_Persqueue_Protos_V1.AddReadRuleRequest) error
	RemoveReadRule(ctx context.Context, req *Ydb_Persqueue_Protos_V1.RemoveReadRuleRequest) error
	Close() error
}

type controlPlane struct {
	lb *session.SessionV1
}

func (c *controlPlane) DescribeTopic(ctx context.Context, topic string) (*Ydb_Persqueue_Protos_V1.DescribeTopicResult, error) {
	var res Ydb_Persqueue_Protos_V1.DescribeTopicResult
	req := Ydb_Persqueue_Protos_V1.DescribeTopicRequest{
		Path: topic,
	}
	if err := c.lb.CallOperation(ctx, "/Ydb.PersQueue.V1.PersQueueService/DescribeTopic", &req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

func (c *controlPlane) CreateTopic(ctx context.Context, req *Ydb_Persqueue_Protos_V1.CreateTopicRequest) error {
	var res Ydb_Persqueue_Protos_V1.CreateTopicResult
	return c.lb.CallOperation(ctx, "/Ydb.PersQueue.V1.PersQueueService/CreateTopic", req, &res)
}

func (c *controlPlane) DropTopic(ctx context.Context, req *Ydb_Persqueue_Protos_V1.DropTopicRequest) error {
	var res Ydb_Persqueue_Protos_V1.DropTopicResult
	return c.lb.CallOperation(ctx, "/Ydb.PersQueue.V1.PersQueueService/DropTopic", req, &res)
}

func (c *controlPlane) AlterTopic(ctx context.Context, req *Ydb_Persqueue_Protos_V1.AlterTopicRequest) error {
	var res Ydb_Persqueue_Protos_V1.AlterTopicResult
	return c.lb.CallOperation(ctx, "/Ydb.PersQueue.V1.PersQueueService/AlterTopic", req, &res)
}

func (c *controlPlane) AddReadRule(ctx context.Context, req *Ydb_Persqueue_Protos_V1.AddReadRuleRequest) error {
	var res Ydb_Persqueue_Protos_V1.AddReadRuleResult
	return c.lb.CallOperation(ctx, "/Ydb.PersQueue.V1.PersQueueService/AddReadRule", req, &res)
}

func (c *controlPlane) RemoveReadRule(ctx context.Context, req *Ydb_Persqueue_Protos_V1.RemoveReadRuleRequest) error {
	var res Ydb_Persqueue_Protos_V1.RemoveReadRuleResult
	return c.lb.CallOperation(ctx, "/Ydb.PersQueue.V1.PersQueueService/RemoveReadRule", req, &res)
}

func (c *controlPlane) Close() error {
	return c.lb.Close()
}

func NewControlPlaneClient(ctx context.Context, opts session.Options) (ControlPlane, error) {
	lb, err := session.DialV1(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &controlPlane{
		lb: lb,
	}, nil
}
