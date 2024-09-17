package mysql

import (
	"time"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/dbaas"
)

type ConnectionParams struct {
	Host        string
	Port        int
	User        string
	Password    string
	Database    string
	TLS         bool
	CertPEMFile string
	Location    *time.Location
	RootCAFiles []string
}

func NewConnectionParams(config *MysqlStorageParams) (*ConnectionParams, error) {
	params := ConnectionParams{
		Host:        config.Host,
		Port:        config.Port,
		User:        config.User,
		Password:    config.Password,
		Database:    config.Database,
		TLS:         config.TLS,
		CertPEMFile: config.CertPEMFile,
		Location:    nil,
		RootCAFiles: config.RootCAFiles,
	}
	if err := params.ResolveLocation(config.Timezone); err != nil {
		return nil, xerrors.Errorf("Can't resolve location from value '%v': %w", config.Timezone, err)
	}
	if err := params.ResolveHost(config.ClusterID); err != nil {
		return nil, xerrors.Errorf("Can't resolve host: %w", err)
	}
	return &params, nil
}

func (params *ConnectionParams) ResolveLocation(locationStr string) error {
	location, err := time.LoadLocation(locationStr)
	if err != nil {
		return err
	}
	params.Location = location
	return nil
}

func (params *ConnectionParams) ResolveHost(clusterID string) error {
	if params.Host != "" {
		return nil
	}

	hosts, err := dbaas.ResolveClusterHosts(dbaas.ProviderTypeMysql, clusterID)
	if err != nil {
		return xerrors.Errorf("unable to get mysql hosts: %w", err)
	}

	for _, host := range hosts {
		if host.Role == dbaas.MASTER {
			params.Host = host.Name
			break
		}
	}
	if params.Host == "" {
		return xerrors.Errorf("No master host was found in %v", hosts)
	}
	return nil
}
