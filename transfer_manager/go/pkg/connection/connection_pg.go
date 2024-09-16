package connection

import (
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
)

type ConnectionPG struct {
	Hosts          []*Host
	User           string
	Password       model.SecretString
	Database       string
	HasTLS         bool
	CACertificates string
	ClusterID      string
}

func (pg *ConnectionPG) IsManagedConnection() {}

func (pg *ConnectionPG) MasterHost() *Host {
	for _, host := range pg.Hosts {
		if host.Role == Master {
			return host
		}
	}
	return nil
}

func (pg *ConnectionPG) Replicas() []*Host {
	hosts := make([]*Host, 0)
	for _, host := range pg.Hosts {
		if host.Role == Replica {
			hosts = append(hosts, host)
		}
	}
	return hosts
}

func (pg *ConnectionPG) SetHosts(hostNames []string, port int) {
	hosts := make([]*Host, 0, len(hostNames))
	for _, host := range hostNames {
		hosts = append(hosts, SimpleHost(host, port))
	}
	pg.Hosts = hosts
}

func (pg *ConnectionPG) GetPort(hostName string) uint16 {
	for _, host := range pg.Hosts {
		if host.Name == hostName {
			return uint16(host.Port)
		}
	}
	return 0
}

func (pg *ConnectionPG) HostNames() []string {
	hosts := make([]string, 0, len(pg.Hosts))
	for _, host := range pg.Hosts {
		hosts = append(hosts, host.Name)
	}
	return hosts
}

type Host struct {
	Name        string
	Port        int
	Role        Role
	ReplicaType ReplicaType
}

func SimpleHost(host string, port int) *Host {
	return &Host{
		Name:        host,
		Port:        port,
		Role:        RoleUnknown,
		ReplicaType: ReplicaUndefined,
	}
}

type Role string

const (
	Master      = Role("MASTER")
	Replica     = Role("REPLICA")
	RoleUnknown = Role("REPLICA")
)

type ReplicaType string

const (
	ReplicaSync      = ReplicaType("SYNC")
	ReplicaAsync     = ReplicaType("ASYNC")
	ReplicaUndefined = ReplicaType("UNDEFINED")
)
