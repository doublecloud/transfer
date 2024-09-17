package clickhouse

import (
	"context"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const defaultZKImage = "zookeeper:3.7"

const zkPort = nat.Port("2181/tcp")

// ClickHouseContainer represents the ClickHouse container type used in the module
type ZookeeperContainer struct {
	testcontainers.Container
	exposedPort nat.Port
	ipaddr      string
}

func (c *ZookeeperContainer) IP() string {
	return c.ipaddr
}

func (c *ZookeeperContainer) Port() nat.Port {
	return c.exposedPort
}

func PrepareZK(ctx context.Context) (*ZookeeperContainer, error) {
	zkcontainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			ExposedPorts: []string{zkPort.Port()},
			Image:        defaultZKImage,
			WaitingFor:   wait.ForListeningPort(zkPort),
		},
		Started: true,
	})
	if err != nil {
		return nil, err
	}

	zkExposedPort, err := zkcontainer.MappedPort(ctx, zkPort)
	if err != nil {
		return nil, err
	}

	ipaddr, err := zkcontainer.ContainerIP(ctx)
	if err != nil {
		return nil, err
	}
	return &ZookeeperContainer{Container: zkcontainer, exposedPort: zkExposedPort, ipaddr: ipaddr}, nil
}
