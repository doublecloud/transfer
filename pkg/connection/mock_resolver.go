package connection

import (
	"context"
	"reflect"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ ConnResolver = (*MockConnectionResolver)(nil)

type MockConnectionResolver struct {
	connectionsByID map[string]ManagedConnection
	foldersByID     map[string]string
	permissionsByID map[string]bool
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
	d.permissionsByID[connectionID] = true
	return nil
}

func (d *MockConnectionResolver) DropPermission(connectionID string) {
	d.permissionsByID[connectionID] = false
}

func (d *MockConnectionResolver) AddPermission(connectionID string) {
	d.permissionsByID[connectionID] = true
}

func (d *MockConnectionResolver) Remove(connectionID string) {
	d.connectionsByID[connectionID] = nil
	delete(d.connectionsByID, connectionID)
	delete(d.foldersByID, connectionID)
	delete(d.permissionsByID, connectionID)
}

func NewMockConnectionResolver() *MockConnectionResolver {
	return &MockConnectionResolver{
		connectionsByID: make(map[string]ManagedConnection),
		foldersByID:     make(map[string]string),
		permissionsByID: make(map[string]bool),
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
	conn, err := d.get(connectionID, nil)
	if err != nil {
		return "", nil, xerrors.Errorf("Unable to resolve connection %s: %w", connectionID, err)
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

	if connType != nil && reflect.TypeOf(res) != connType {
		return nil, xerrors.Errorf("Unable to cast connection %s", connectionID)
	}

	permission := d.permissionsByID[connectionID]
	if !permission {
		return nil, status.Errorf(codes.PermissionDenied,
			"Can't get connection '%v': trace-id = 111 span-id = 111 request-id = 111 rpc error: bebebebe", connectionID)
	}
	return res, nil
}
