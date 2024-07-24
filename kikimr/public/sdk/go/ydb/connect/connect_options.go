package connect

import (
	"context"
	"fmt"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/auth/iam"
)

type ConnectOption func(client *Connection) error

func WithAccessTokenCredentials(accessToken string) ConnectOption {
	return WithCredentials(
		ydb.NewAuthTokenCredentials(accessToken, "connect.WithAccessTokenCredentials(accessToken)"), // hide access token for logs
	)
}

func WithAnonymousCredentials() ConnectOption {
	return WithCredentials(
		ydb.NewAnonymousCredentials("connect.WithAnonymousCredentials()"),
	)
}

func WithMetadataCredentials(ctx context.Context) ConnectOption {
	return WithCredentials(
		iam.InstanceServiceAccount(
			ydb.WithCredentialsSourceInfo(ctx, "connect.WithMetadataCredentials(ctx)"),
		),
	)
}

func WithServiceAccountKeyFileCredentials(serviceAccountKeyFile string) ConnectOption {
	return withCredentials(func() (ydb.Credentials, error) {
		credentials, err := iam.NewClient(
			iam.WithServiceFile(serviceAccountKeyFile),
			iam.WithDefaultEndpoint(),
			iam.WithSystemCertPool(),
			iam.WithSourceInfo("connect.WithServiceAccountKeyFileCredentials(\""+serviceAccountKeyFile+"\")"),
		)
		if err != nil {
			return nil, fmt.Errorf("configure credentials error: %w", err)
		}
		return credentials, nil
	})
}

func withCredentials(createCredentials func() (ydb.Credentials, error)) ConnectOption {
	return func(c *Connection) error {
		if c.driverConfig == nil {
			c.driverConfig = &ydb.DriverConfig{}
		}
		credentials, err := createCredentials()
		if err != nil {
			return err
		}
		c.driverConfig.Credentials = credentials
		return nil
	}
}

func WithCredentials(credentials ydb.Credentials) ConnectOption {
	return withCredentials(func() (ydb.Credentials, error) {
		return credentials, nil
	})
}

func WithDriverConfig(config *ydb.DriverConfig) ConnectOption {
	return func(c *Connection) error {
		c.driverConfig = config
		return nil
	}
}

func withDriver(driver ydb.Driver) ConnectOption {
	return func(c *Connection) error {
		c.table.client.Driver = driver
		return nil
	}
}

func WithSessionPoolSizeLimit(sizeLimit int) ConnectOption {
	return func(c *Connection) error {
		c.table.sessionPool.SizeLimit = sizeLimit
		return nil
	}
}

func WithSessionPoolKeepAliveMinSize(keepAliveMinSize int) ConnectOption {
	return func(c *Connection) error {
		c.table.sessionPool.KeepAliveMinSize = keepAliveMinSize
		return nil
	}
}

func WithSessionPoolIdleThreshold(idleThreshold time.Duration) ConnectOption {
	return func(c *Connection) error {
		c.table.sessionPool.IdleThreshold = idleThreshold
		return nil
	}
}

// Deprecated: has no effect now
func WithSessionPoolBusyCheckInterval(busyCheckInterval time.Duration) ConnectOption {
	return func(c *Connection) error {
		return nil
	}
}

// Deprecated: has no effect now
func WithSessionPoolKeepAliveBatchSize(keepAliveBatchSize int) ConnectOption {
	return func(c *Connection) error {
		return nil
	}
}

func WithSessionPoolKeepAliveTimeout(keepAliveTimeout time.Duration) ConnectOption {
	return func(c *Connection) error {
		c.table.sessionPool.KeepAliveTimeout = keepAliveTimeout
		return nil
	}
}

func WithSessionPoolCreateSessionTimeout(createSessionTimeout time.Duration) ConnectOption {
	return func(c *Connection) error {
		c.table.sessionPool.CreateSessionTimeout = createSessionTimeout
		return nil
	}
}

func WithSessionPoolDeleteTimeout(deleteTimeout time.Duration) ConnectOption {
	return func(c *Connection) error {
		c.table.sessionPool.DeleteTimeout = deleteTimeout
		return nil
	}
}
