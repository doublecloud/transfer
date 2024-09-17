package connection

import (
	"context"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
)

var current ConnResolver

type ManagedConnection interface {
	IsManagedConnection()
}

var _ ConnResolver = (*UnimplementedResolver)(nil)
var UninitializedResolverErr = xerrors.NewSentinel("connection resolver not initialized!")

type ConnResolver interface {
	ResolveConnection(ctx context.Context, connectionID string, typ abstract.ProviderType) (ManagedConnection, error)
}

func Init(resolver ConnResolver) {
	current = resolver
}

func Resolver() ConnResolver {
	if current != nil {
		return current
	}
	return &UnimplementedResolver{}
}

type UnimplementedResolver struct{}

func (u UnimplementedResolver) ResolveConnection(ctx context.Context, connectionID string, typ abstract.ProviderType) (ManagedConnection, error) {
	return nil, UninitializedResolverErr
}
