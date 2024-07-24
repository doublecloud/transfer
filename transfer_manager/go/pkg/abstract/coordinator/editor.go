package coordinator

import server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"

// Editor is obsolete interface to update entities on coordinator
//
// Deprecated: do not rely on it anymore
type Editor interface {
	// GetEndpointTransfers get all *other* linked transfer to either source or target of provider *transferID*
	GetEndpointTransfers(transferID string, isSource bool) ([]*server.Transfer, error)
	// GetTransfers return *related* transfers to *transferID* in desired statuses
	GetTransfers(statuses []server.TransferStatus, transferID string) ([]*server.Transfer, error)
	// GetEndpoint get source or target *server.EndpointParams* for provided *transferID*
	GetEndpoint(transferID string, isSource bool) (server.EndpointParams, error)
	// UpdateEndpoint update *server.EndpointParams* for provided *transferID*
	UpdateEndpoint(transferID string, endpoint server.EndpointParams) error
}
