package clickhouse

import (
	"context"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/providers/clickhouse/model"
	"github.com/doublecloud/transfer/pkg/transformer/registry/dbt"
)

func init() {
	dbt.Register(New)
}

type Adapter struct {
	*model.ChDestination
}

func (d *Adapter) DBTConfiguration(_ context.Context) (any, error) {
	hosts, err := model.ConnectionHosts(d.ToStorageParams(), "")
	if err != nil {
		return nil, xerrors.Errorf("failed to obtain a list of hosts for the destination ClickHouse: %w", err)
	}
	if len(hosts) == 0 {
		return nil, xerrors.New("hosts is required")
	}
	host := hosts[0]
	if host == "localhost" {
		host = "host.docker.internal" // DBT runs inside docker, so localhost there is a host.docker.internal
	}

	return map[string]any{
		"type":     "clickhouse",
		"schema":   d.Database,
		"host":     host,
		"port":     d.HTTPPort,
		"user":     d.User,
		"password": string(d.Password),
		"secure":   d.SSLEnabled || d.MdbClusterID != "",
	}, nil
}

func New(endpoint server.Destination) (dbt.SupportedDestination, error) {
	ch, ok := endpoint.(*model.ChDestination)
	if !ok {
		return nil, dbt.NotSupportedErr
	}
	return &Adapter{ch}, nil
}
