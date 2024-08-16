package fedtypes

import "github.com/doublecloud/transfer/library/go/yandex/ydb/internal/broadcast"

type ConnectionManager interface {
	GetState() (FederationState, error)
	SubscribeToDBInfoRenew() broadcast.OneTimeWaiter
}

type FederationState struct {
	Connections []*FedConnection
	Location    string
}

func NewFederationState(connections []*FedConnection, location string) FederationState {
	return FederationState{
		Connections: connections,
		Location:    location,
	}
}
