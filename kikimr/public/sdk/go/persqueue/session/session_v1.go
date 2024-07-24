package session

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/genproto/Ydb_PersQueue_ClusterDiscovery"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/genproto/Ydb_PersQueue_V1"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/log"
	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

var (
	ErrPQOperationNotReady = errors.New("pq operation returned not ready status; this should never happen")
)

// Сессия для работы с LogBroker. Реализует discoverEndpoint и подключается к выбранной для нас ноде LogBroker
type SessionV1 struct {
	settings Options

	Cookie    uint64
	ProxyName string
	SessionID string // for debug only

	conn   *grpc.ClientConn // base connection
	Client Ydb_PersQueue_V1.PersQueueServiceClient

	logger log.Logger
}

func DialV1(ctx context.Context, settings Options) (*SessionV1, error) {
	var lgr log.Logger
	if settings.Logger != nil {
		lgr = settings.Logger
	} else {
		lgr = log.NopLogger
	}

	lb := &SessionV1{
		settings: settings,
		logger:   lgr,
	}

	err := lb.discoverCluster(ctx)
	if err != nil {
		return nil, err
	}

	err = lb.discoverEndpoint(ctx)
	if err != nil {
		return nil, err
	}

	err = lb.createConnection(ctx)
	if err != nil {
		return nil, err
	}

	return lb, nil
}

func (lb *SessionV1) Close() error {
	return lb.conn.Close()
}

func (lb *SessionV1) GetTransportCredentialsOptions() grpc.DialOption {
	if lb.settings.TLSConfig != nil {
		return grpc.WithTransportCredentials(
			credentials.NewTLS(lb.settings.TLSConfig),
		)
	}
	return grpc.WithInsecure()
}

func (lb *SessionV1) NewOutgoingContext(ctx context.Context) (context.Context, error) {
	meta := metadata.New(map[string]string{
		metaDatabase:           lb.settings.database(),
		"x-ydb-sdk-build-info": "go-sdk-2021.04.1",
	})

	if lb.settings.Credentials != nil {
		token, err := lb.settings.Credentials.Token(ctx)
		if err != nil {
			return nil, err
		}
		meta["x-ydb-auth-ticket"] = []string{token}
	}

	return metadata.NewOutgoingContext(ctx, meta), nil
}

func (lb *SessionV1) createConnection(ctx context.Context) error {
	ctx, err := lb.NewOutgoingContext(ctx)
	if err != nil {
		return err
	}

	conn, err := grpc.DialContext(
		ctx,
		lb.ProxyName,
		lb.GetTransportCredentialsOptions(),
		grpc.WithKeepaliveParams(
			keepalive.ClientParameters{
				Time:                90 * time.Second,
				Timeout:             time.Second,
				PermitWithoutStream: true,
			},
		),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMessageSize),
			grpc.MaxCallSendMsgSize(maxMessageSize),
		),
	)
	if err != nil {
		return err
	}

	lb.conn = conn
	lb.Client = Ydb_PersQueue_V1.NewPersQueueServiceClient(conn)
	return nil
}

func (lb *SessionV1) CallOperation(ctx context.Context, method string, req, res proto.Message) (err error) {
	ctx, err = lb.NewOutgoingContext(ctx)
	if err != nil {
		return err
	}
	var conn *grpc.ClientConn
	if lb.conn != nil {
		conn = lb.conn
	} else {
		c, err := grpc.DialContext(ctx, lb.settings.endpoint(), lb.GetTransportCredentialsOptions())
		if err != nil {
			return err
		}
		defer func() { _ = c.Close() }()
		conn = c
	}
	var resp Ydb_Operations.GetOperationResponse
	err = conn.Invoke(ctx, method, req, &resp)
	if err != nil {
		return err
	}
	op := resp.Operation
	if !op.Ready {
		return ErrPQOperationNotReady
	}

	if op.Status != Ydb.StatusIds_SUCCESS {
		return fmt.Errorf("%v failed: %s, %s", method, op.Status.String(), printIssues(op.GetIssues()))
	}

	if op.Result == nil {
		return nil
	}

	return proto.Unmarshal(op.Result.Value, res)
}

func (lb *SessionV1) discoverCluster(ctx context.Context) error {
	if !lb.settings.DiscoverCluster {
		return nil
	}
	var (
		res Ydb_PersQueue_ClusterDiscovery.DiscoverClustersResult
		req Ydb_PersQueue_ClusterDiscovery.DiscoverClustersRequest
	)
	req.WriteSessions = []*Ydb_PersQueue_ClusterDiscovery.WriteSessionParams{{
		Topic:                lb.settings.Topic,
		SourceId:             lb.settings.SourceID,
		PartitionGroup:       lb.settings.PartitionGroup,
		PreferredClusterName: lb.settings.PreferredClusterName,
	}}

	err := lb.CallOperation(ctx, "/Ydb.PersQueue.V1.ClusterDiscoveryService/DiscoverClusters", &req, &res)
	if err != nil {
		return err
	}

	for _, clusters := range res.GetWriteSessionsClusters() {
		for _, cluster := range clusters.GetClusters() {
			if !cluster.GetAvailable() {
				continue
			}
			if lb.settings.PreferredClusterName != "" && cluster.GetName() != lb.settings.PreferredClusterName {
				continue
			}
			lb.logger.Log(ctx, log.LevelInfo, "Cluster selected", map[string]interface{}{
				"cluster": cluster.GetName(),
			})
			lb.settings.Endpoint = cluster.Endpoint
			return nil
		}
	}

	return errors.New("cluster not found")
}

func (lb *SessionV1) discoverEndpoint(ctx context.Context) error {
	if lb.settings.proxy != "" {
		lb.ProxyName = lb.settings.proxy
		lb.logger.Log(ctx, log.LevelInfo, "Proxy selected", map[string]interface{}{
			"proxy": lb.ProxyName,
		})
		return nil
	}
	var res Ydb_Discovery.ListEndpointsResult
	req := Ydb_Discovery.ListEndpointsRequest{
		Database: lb.settings.database(),
	}
	err := lb.CallOperation(ctx, "/Ydb.Discovery.V1.DiscoveryService/ListEndpoints", &req, &res)
	if err != nil && len(res.Endpoints) == 0 {
		return err
	}

	if len(res.Endpoints) == 0 {
		return errors.New("endpoint not found")
	}

	lb.Cookie = uint64(magicCookie)
	for _, e := range res.Endpoints {
		proxy := fmt.Sprintf("%s:%d", e.Address, e.Port)
		lb.ProxyName = proxy
		break
	}

	lb.logger.Log(ctx, log.LevelInfo, "Proxy selected", map[string]interface{}{
		"proxy": lb.ProxyName,
	})
	return nil
}

func (lb *SessionV1) GetCredentials(ctx context.Context) []byte {
	if lb.settings.Credentials == nil {
		return nil
	}

	token, err := lb.settings.Credentials.Token(ctx)
	if err != nil {
		lb.logger.Log(ctx, log.LevelError, "credentials retrieving fault", map[string]interface{}{
			"error": err.Error(),
		})
		return nil
	}

	return []byte(token)
}
