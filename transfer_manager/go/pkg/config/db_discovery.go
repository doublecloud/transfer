package config

// go-sumtype:decl DBDiscovery
type DBDiscovery interface {
	TypeTagged
	isDBDiscovery()
}

type OnPremise struct {
	Host   EnvVarString `mapstructure:"host"`
	Port   EnvVarInt    `mapstructure:"port"`
	UseTLS bool         `mapstructure:"use_tls"`
}

type MDB struct {
	ClusterID string `mapstructure:"cluster_id"`
}

func (*MDB) isDBDiscovery() {}
func (*MDB) IsTypeTagged()  {}

func (*OnPremise) isDBDiscovery() {}
func (*OnPremise) IsTypeTagged()  {}

func init() {
	RegisterTypeTagged((*DBDiscovery)(nil), (*MDB)(nil), "mdb", nil)
	RegisterTypeTagged((*DBDiscovery)(nil), (*OnPremise)(nil), "on_premise", nil)
}
