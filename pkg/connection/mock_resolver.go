package connection

import (
	"context"
	"reflect"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
)

var _ ConnResolver = (*MockConnectionResolver)(nil)

type MockConnectionResolver struct {
	connectionsByID map[string]ManagedConnection
	foldersByID     map[string]string
}

func (d *MockConnectionResolver) ResolveConnection(ctx context.Context, connectionID string, typ abstract.ProviderType) (ManagedConnection, error) {
	return d.GetConnection(ctx, connectionID, typ)
}

func (d *MockConnectionResolver) GetConnection(ctx context.Context, connectionID string, typ abstract.ProviderType) (ManagedConnection, error) {
	switch typ {
	case "pg":
		//nolint:descriptiveerrors
		return d.GetPGConnection(ctx, connectionID)
	case "ch":
		//nolint:descriptiveerrors
		return d.GetCHConnection(ctx, connectionID)
	default:
		return nil, xerrors.Errorf("Not implemented for provider %s", typ)
	}
}

func (d *MockConnectionResolver) Add(connectionID, folder string, connection any) error {
	conn, ok := connection.(ManagedConnection)
	if !ok {
		return xerrors.Errorf("Wrong connection type: %s", connection)
	}
	d.connectionsByID[connectionID] = conn
	d.foldersByID[connectionID] = folder
	return nil
}

func NewMockConnectionResolver() *MockConnectionResolver {
	return &MockConnectionResolver{
		connectionsByID: make(map[string]ManagedConnection),
		foldersByID:     make(map[string]string),
	}
}

func (d *MockConnectionResolver) ResolveConnectionFolder(ctx context.Context, connectionID string) (string, error) {
	res, ok := d.foldersByID[connectionID]
	if !ok {
		return "", xerrors.Errorf("Unable to resolve connection %s", connectionID)
	}
	return res, nil
}

func (d *MockConnectionResolver) ResolveClusterOrHosts(ctx context.Context, connectionID string) (string, []string, error) {
	conn, ok := d.connectionsByID[connectionID]
	if !ok {
		return "", nil, xerrors.Errorf("Unable to resolve connection %s", connectionID)
	}

	if pg, ok := conn.(*ConnectionPG); ok {
		return pg.ClusterID, pg.HostNames(), nil
	}

	return "", nil, xerrors.Errorf("Unable to get connection cluster and hosts %s", connectionID)
}

func (d *MockConnectionResolver) ResolveProviderType(ctx context.Context, connectionID string) (abstract.ProviderType, error) {
	conn, ok := d.connectionsByID[connectionID]
	if !ok {
		return "", xerrors.Errorf("Unable to resolve connection %s", connectionID)
	}
	if _, ok = conn.(*ConnectionPG); ok {
		return "pg", nil
	}
	if _, ok = conn.(*ConnectionCH); ok {
		return "ch", nil
	}
	return "", xerrors.Errorf("Unable to get connection type %s", connectionID)
}

func (d *MockConnectionResolver) GetPGConnection(ctx context.Context, connectionID string) (*ConnectionPG, error) {
	return d.ResolvePGConnection(ctx, connectionID)
}

func (d *MockConnectionResolver) ResolvePGConnection(ctx context.Context, connectionID string) (conn *ConnectionPG, err error) {
	get, err := d.get(connectionID, reflect.TypeOf(conn))
	if err != nil {
		return nil, err
	}
	return get.(*ConnectionPG), nil
}

func (d *MockConnectionResolver) GetCHConnection(ctx context.Context, connectionID string) (conn *ConnectionCH, err error) {
	return d.ResolveCHConnection(ctx, connectionID)
}

func (d *MockConnectionResolver) ResolveCHConnection(ctx context.Context, connectionID string) (conn *ConnectionCH, err error) {
	get, err := d.get(connectionID, reflect.TypeOf(conn))
	if err != nil {
		return nil, err
	}
	return get.(*ConnectionCH), nil
}

func (d *MockConnectionResolver) get(connectionID string, connType reflect.Type) (ManagedConnection, error) {
	res, ok := d.connectionsByID[connectionID]
	if !ok {
		return nil, xerrors.Errorf("Unable to resolve connection %s", connectionID)
	}
	if reflect.TypeOf(res) != connType {
		return nil, xerrors.Errorf("Unable to cast connection %s", connectionID)
	}
	return res, nil
}
