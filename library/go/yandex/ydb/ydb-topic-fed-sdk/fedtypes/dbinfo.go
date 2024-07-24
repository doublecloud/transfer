package fedtypes

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_FederationDiscovery"
)

type DatabaseInfo struct {
	ID       string
	Name     string
	Path     string
	Endpoint string
	Location string
	Status   DatabaseInfoStatus
	Weight   int64
}

func (d *DatabaseInfo) Equals(other *DatabaseInfo) bool {
	switch {
	case d == other:
		return true
	case d == nil:
		return false
	case other == nil:
		return false
	default:
		// pass
	}

	return d.ID == other.ID &&
		d.Name == other.Name &&
		d.Path == other.Path &&
		d.Endpoint == other.Endpoint &&
		d.Location == other.Location &&
		d.Status == other.Status &&
		d.Weight == other.Weight
}

func (d *DatabaseInfo) String() string {
	return fmt.Sprintf("{id: %q, name: %q, path: %q, endpoint: %q, location: %q, status: %q, weight: %v}",
		d.ID, d.Name, d.Path, d.Endpoint, d.Location, d.Status, int(d.Weight),
	)
}

func NewDatabaseInfo(id, name, path, endpoint, location string, status DatabaseInfoStatus, weight int64) DatabaseInfo {
	return DatabaseInfo{
		ID:       id,
		Name:     name,
		Path:     path,
		Endpoint: endpoint,
		Location: location,
		Status:   status,
		Weight:   weight,
	}
}

type DatabaseInfoStatus int

const (
	DatabaseInfoStatusUNSPECIFIED = DatabaseInfoStatus(Ydb_FederationDiscovery.DatabaseInfo_STATUS_UNSPECIFIED)
	DatabaseInfoStatusAVAILABLE   = DatabaseInfoStatus(Ydb_FederationDiscovery.DatabaseInfo_AVAILABLE)
	DatabaseInfoStatusREADONLY    = DatabaseInfoStatus(Ydb_FederationDiscovery.DatabaseInfo_READ_ONLY)
	DatabaseInfoStatusUNAVAILABLE = DatabaseInfoStatus(Ydb_FederationDiscovery.DatabaseInfo_UNAVAILABLE)
)

func (status DatabaseInfoStatus) String() string {
	return Ydb_FederationDiscovery.DatabaseInfo_Status(status).String()
}
