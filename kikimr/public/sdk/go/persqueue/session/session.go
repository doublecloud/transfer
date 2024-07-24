package session

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"

	pqCredentials "github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/credentials"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/genproto/Ydb_PersQueue_ClusterDiscovery"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/genproto/Ydb_PersQueue_V0"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/log"
	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

const (
	defaultPort    = 2135
	magicCookie    = 123456789
	maxMessageSize = 1024 * 1024 * 130
	clientTimeout  = 15 * time.Second
	rootDatabase   = "/Root"
	metaDatabase   = "x-ydb-database"
	metaAuthTicket = "x-ydb-auth-ticket"
)

var (
	ErrDiscoveryNotReady = errors.New("endpoint discovery operations returned not ready status; " +
		"this should never happen")
)

type Options struct {
	Endpoint      string
	Port          int
	Credentials   pqCredentials.Credentials
	TLSConfig     *tls.Config
	Logger        log.Logger
	proxy         string
	Database      string
	ClientTimeout time.Duration

	DiscoverCluster      bool
	Topic                string
	SourceID             []byte
	PartitionGroup       uint32
	PreferredClusterName string
}

func (s *Options) endpoint() string {
	port := defaultPort
	if s.Port != 0 {
		port = s.Port
	}
	return formatEndpoint(s.Endpoint, port)
}

func (s *Options) database() string {
	if s.Database != "" {
		return s.Database
	} else {
		return rootDatabase
	}
}

func (s *Options) clientTimeout() time.Duration {
	if s.ClientTimeout == 0 {
		return clientTimeout
	}
	return s.ClientTimeout
}

func (s Options) WithProxy(proxy string) Options {
	s.proxy = proxy
	return s
}

// Сессия для работы с LogBroker. Реализует discoverEndpoint и подключается к выбранной для нас ноде LogBroker
type Session struct {
	settings Options

	Cookie    uint64
	ProxyName string
	SessionID string // for debug only

	conn   *grpc.ClientConn // base connection
	Client Ydb_PersQueue_V0.PersQueueServiceClient

	logger log.Logger
}

func Dial(ctx context.Context, settings Options) (*Session, error) {
	var lgr log.Logger
	if settings.Logger != nil {
		lgr = settings.Logger
	} else {
		lgr = log.NopLogger
	}

	lb := &Session{
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

func (lb *Session) Close() error {
	return lb.conn.Close()
}

func (lb *Session) GetTransportCredentialsOptions() grpc.DialOption {
	if lb.settings.TLSConfig != nil {
		return grpc.WithTransportCredentials(
			credentials.NewTLS(lb.settings.TLSConfig),
		)
	}
	return grpc.WithInsecure()
}

func (lb *Session) newOutgoingContext(ctx context.Context) (context.Context, error) {
	meta := metadata.New(map[string]string{
		metaDatabase: lb.settings.database(),
	})

	if lb.settings.Credentials != nil {
		token, err := lb.settings.Credentials.Token(ctx)
		if err != nil {
			return nil, err
		}
		meta[metaAuthTicket] = []string{token}
	}

	return metadata.NewOutgoingContext(ctx, meta), nil
}

func (lb *Session) createConnection(ctx context.Context) error {
	ctx, err := lb.newOutgoingContext(ctx)
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
	lb.Client = Ydb_PersQueue_V0.NewPersQueueServiceClient(conn)
	return nil
}

func (lb *Session) callOperation(ctx context.Context, method string, req, res proto.Message) (err error) {
	ctx, err = lb.newOutgoingContext(ctx)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, lb.settings.clientTimeout())
	defer cancel()

	conn, err := grpc.DialContext(ctx, lb.settings.endpoint(), lb.GetTransportCredentialsOptions())
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	var resp Ydb_Operations.GetOperationResponse
	err = conn.Invoke(ctx, method, req, &resp)
	if err != nil {
		return err
	}
	op := resp.Operation
	if !op.Ready {
		return ErrDiscoveryNotReady
	}

	if op.Status != Ydb.StatusIds_SUCCESS {
		return fmt.Errorf("endpoint discovery failed: %s, %s", op.Status.String(), printIssues(op.GetIssues()))
	}

	return proto.Unmarshal(op.Result.Value, res)
}

func (lb *Session) discoverCluster(ctx context.Context) error {
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

	err := lb.callOperation(ctx, "/Ydb.PersQueue.V1.ClusterDiscoveryService/DiscoverClusters", &req, &res)
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
			endpointParts := strings.Split(cluster.Endpoint, ":")
			lb.settings.Endpoint = endpointParts[0]
			return nil
		}
	}

	return errors.New("cluster not found")
}

func (lb *Session) discoverEndpoint(ctx context.Context) error {
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
	err := lb.callOperation(ctx, "/Ydb.Discovery.V1.DiscoveryService/ListEndpoints", &req, &res)
	if err != nil && len(res.Endpoints) == 0 {
		return err
	}

	if len(res.Endpoints) > 0 {
		lb.Cookie = uint64(magicCookie)
		for _, e := range res.Endpoints {
			proxy := fmt.Sprintf("%s:%d", e.Address, e.Port)
			lb.ProxyName = proxy
			break
		}

		lb.logger.Log(ctx, log.LevelInfo, "proxy selected", map[string]interface{}{
			"proxy": lb.ProxyName,
		})
		return nil
	}

	return errors.New("endpoint not found")
}

func (lb *Session) GetCredentials(ctx context.Context) *Ydb_PersQueue_V0.Credentials {
	if lb.settings.Credentials == nil {
		return &Ydb_PersQueue_V0.Credentials{}
	}

	token, err := lb.settings.Credentials.Token(ctx)
	if err != nil {
		lb.logger.Log(ctx, log.LevelError, "credentials retrieving fault", map[string]interface{}{
			"error": err.Error(),
		})
		return &Ydb_PersQueue_V0.Credentials{}
	}

	return &Ydb_PersQueue_V0.Credentials{
		Credentials: &Ydb_PersQueue_V0.Credentials_OauthToken{
			OauthToken: []byte(token), // TODO: there is one typeless token field in new V1 protocol.
		},
	}
}

func printIssues(issues []*Ydb_Issue.IssueMessage) string {
	res := ""
	for _, issue := range issues {
		childIssues := printIssues(issue.GetIssues())
		if issue.GetMessage() != "" {
			res = fmt.Sprintf("\n%v", issue.GetMessage())
		}
		if childIssues != "" {
			res = fmt.Sprintf("\n%v", childIssues)
		}
	}
	return res
}
