package yc

import (
	"context"

	"github.com/doublecloud/tross/cloud/bitbucket/public-api/yandex/cloud/mdb/clickhouse/v1"
	"github.com/doublecloud/tross/cloud/bitbucket/public-api/yandex/cloud/mdb/elasticsearch/v1"
	"github.com/doublecloud/tross/cloud/bitbucket/public-api/yandex/cloud/mdb/greenplum/v1"
	"github.com/doublecloud/tross/cloud/bitbucket/public-api/yandex/cloud/mdb/kafka/v1"
	"github.com/doublecloud/tross/cloud/bitbucket/public-api/yandex/cloud/mdb/mongodb/v1"
	"github.com/doublecloud/tross/cloud/bitbucket/public-api/yandex/cloud/mdb/mysql/v1"
	"github.com/doublecloud/tross/cloud/bitbucket/public-api/yandex/cloud/mdb/opensearch/v1"
	"github.com/doublecloud/tross/cloud/bitbucket/public-api/yandex/cloud/mdb/postgresql/v1"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"google.golang.org/grpc"
)

type PgClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (c *PgClient) ListHosts(ctx context.Context, in *postgresql.ListClusterHostsRequest, opts ...grpc.CallOption) (*postgresql.ListClusterHostsResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return postgresql.NewClusterServiceClient(conn).ListHosts(ctx, in, opts...)
}

type GreenplumClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (c *GreenplumClient) ListMasterHosts(ctx context.Context, in *greenplum.ListClusterHostsRequest, opts ...grpc.CallOption) (*greenplum.ListClusterHostsResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return greenplum.NewClusterServiceClient(conn).ListMasterHosts(ctx, in, opts...)
}

func (c *GreenplumClient) ListSegmentHosts(ctx context.Context, in *greenplum.ListClusterHostsRequest, opts ...grpc.CallOption) (*greenplum.ListClusterHostsResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return greenplum.NewClusterServiceClient(conn).ListSegmentHosts(ctx, in, opts...)
}

type MysqlClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (c *MysqlClient) ListHosts(ctx context.Context, in *mysql.ListClusterHostsRequest, opts ...grpc.CallOption) (*mysql.ListClusterHostsResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return mysql.NewClusterServiceClient(conn).ListHosts(ctx, in, opts...)
}

type ClickHouseClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (c *ClickHouseClient) ListHosts(ctx context.Context, in *clickhouse.ListClusterHostsRequest, opts ...grpc.CallOption) (*clickhouse.ListClusterHostsResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return clickhouse.NewClusterServiceClient(conn).ListHosts(ctx, in, opts...)
}

type MongoDBClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (c *MongoDBClient) ListHosts(ctx context.Context, in *mongodb.ListClusterHostsRequest, opts ...grpc.CallOption) (*mongodb.ListClusterHostsResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return mongodb.NewClusterServiceClient(conn).ListHosts(ctx, in, opts...)
}

type KafkaClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (c *KafkaClient) ListHosts(ctx context.Context, in *kafka.ListClusterHostsRequest, opts ...grpc.CallOption) (*kafka.ListClusterHostsResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return kafka.NewClusterServiceClient(conn).ListHosts(ctx, in, opts...)
}

type ElasticSearchClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (c *ElasticSearchClient) ListHosts(ctx context.Context, in *elasticsearch.ListClusterHostsRequest, opts ...grpc.CallOption) (*elasticsearch.ListClusterHostsResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return elasticsearch.NewClusterServiceClient(conn).ListHosts(ctx, in, opts...)
}

type OpenSearchClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (c *OpenSearchClient) hasRole(host *opensearch.Host, expectedRole opensearch.OpenSearch_GroupRole) bool {
	for _, currRole := range host.Roles {
		if currRole == expectedRole {
			return true
		}
	}
	return false
}

func (c *OpenSearchClient) ListDataHosts(ctx context.Context, in *opensearch.ListClusterHostsRequest, opts ...grpc.CallOption) (*opensearch.ListClusterHostsResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection, err: %w", err)
	}
	allHosts, err := opensearch.NewClusterServiceClient(conn).ListHosts(ctx, in, opts...)
	if err != nil {
		return nil, xerrors.Errorf("unable to list OpenSearch hosts, err: %w", err)
	}
	result := new(opensearch.ListClusterHostsResponse)
	for _, currHost := range allHosts.Hosts {
		if c.hasRole(currHost, opensearch.OpenSearch_DATA) {
			result.Hosts = append(result.Hosts, currHost)
		}
	}
	return result, nil
}

type MDB struct {
	sdk *SDK
}

func (m *MDB) PostgreSQL() *PgClient {
	return &PgClient{
		getConn: m.sdk.getConn(ManagedPostgresqlServiceID),
	}
}

func (m *MDB) Greenplum() *GreenplumClient {
	return &GreenplumClient{
		getConn: m.sdk.getConn(ManagedGreenplumServiceID),
	}
}

func (m *MDB) MySQL() *MysqlClient {
	return &MysqlClient{
		getConn: m.sdk.getConn(ManagedMysqlServiceID),
	}
}

func (m *MDB) ClickHouse() *ClickHouseClient {
	return &ClickHouseClient{
		getConn: m.sdk.getConn(ManagedClickhouseServiceID),
	}
}

func (m *MDB) MongoDB() *MongoDBClient {
	return &MongoDBClient{
		getConn: m.sdk.getConn(ManagedMongodbServiceID),
	}
}

func (m *MDB) Kafka() *KafkaClient {
	return &KafkaClient{
		getConn: m.sdk.getConn(ManagedKafkaServiceID),
	}
}

func (m *MDB) ElasticSearch() *ElasticSearchClient {
	return &ElasticSearchClient{
		getConn: m.sdk.getConn(ManagedElasticsearchServiceID),
	}
}

func (m *MDB) OpenSearch() *OpenSearchClient {
	return &OpenSearchClient{
		getConn: m.sdk.getConn(ManagedOpensearchServiceID),
	}
}
