package mongo

type MongoStorageParams struct {
	TLSFile           string
	ClusterID         string
	Hosts             []string
	Port              int
	ReplicaSet        string
	AuthSource        string
	User              string
	Password          string
	Collections       []MongoCollection
	DesiredPartSize   uint64
	PreventJSONRepack bool
	Direct            bool
	RootCAFiles       []string
	SRVMode           bool
}

func (s *MongoStorageParams) ConnectionOptions(defaultCACertPaths []string) MongoConnectionOptions {
	return connectionOptionsImpl(s.Hosts, s.Port, s.ReplicaSet, s.User, s.Password, s.ClusterID, s.AuthSource, s.TLSFile, defaultCACertPaths, s.Direct, s.SRVMode)
}
