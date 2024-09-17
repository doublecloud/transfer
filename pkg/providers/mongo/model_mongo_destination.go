package mongo

import (
	"sort"

	"github.com/doublecloud/transfer/pkg/abstract"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
)

type MongoDestination struct {
	ClusterID         string
	Hosts             []string
	Port              int
	Database          string
	ReplicaSet        string
	AuthSource        string
	User              string
	Password          server.SecretString
	TransformerConfig map[string]string
	Cleanup           server.CleanupType
	SubNetworkID      string
	SecurityGroupIDs  []string
	TLSFile           string
	// make a `direct` connection to mongo, see: https://www.mongodb.com/docs/drivers/go/current/fundamentals/connections/connection-guide/
	Direct bool

	RootCAFiles []string
	// indicates whether the mongoDB client uses a mongodb+srv connection
	SRVMode bool
}

var _ server.Destination = (*MongoDestination)(nil)

func (d *MongoDestination) MDBClusterID() string {
	return d.ClusterID
}

func (d *MongoDestination) WithDefaults() {
	if d.Port <= 0 {
		d.Port = 27018
	}
	if d.Cleanup == "" {
		d.Cleanup = server.Drop
	}
	if len(d.Hosts) > 1 {
		sort.Strings(d.Hosts)
	}
}

func (d *MongoDestination) CleanupMode() server.CleanupType {
	return d.Cleanup
}

func (d *MongoDestination) Transformer() map[string]string {
	return d.TransformerConfig
}

func (MongoDestination) IsDestination() {
}

func (d *MongoDestination) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (d *MongoDestination) HasTLS() bool {
	return d.ClusterID != "" || d.TLSFile != ""
}

func (d *MongoDestination) ConnectionOptions(caCertPaths []string) MongoConnectionOptions {
	var caCert TrustedCACertificate
	if d.TLSFile != "" {
		caCert = InlineCACertificatePEM(d.TLSFile)
	} else if d.ClusterID != "" {
		caCert = CACertificatePEMFilePaths(caCertPaths)
	}
	return MongoConnectionOptions{
		ClusterID:  d.ClusterID,
		Hosts:      d.Hosts,
		Port:       d.Port,
		ReplicaSet: d.ReplicaSet,
		AuthSource: d.AuthSource,
		User:       d.User,
		Password:   string(d.Password),
		CACert:     caCert,
		Direct:     d.Direct,
		SRVMode:    d.SRVMode,
	}
}

func (d *MongoDestination) Validate() error {
	return nil
}

func (d *MongoDestination) ToStorageParams() *MongoStorageParams {
	return &MongoStorageParams{
		TLSFile:           d.TLSFile,
		ClusterID:         d.ClusterID,
		Hosts:             d.Hosts,
		Port:              d.Port,
		ReplicaSet:        d.ReplicaSet,
		AuthSource:        d.AuthSource,
		User:              d.User,
		Password:          string(d.Password),
		Collections:       make([]MongoCollection, 0),
		DesiredPartSize:   TablePartByteSize,
		PreventJSONRepack: false,
		Direct:            d.Direct,
		RootCAFiles:       d.RootCAFiles,
		SRVMode:           d.SRVMode,
	}
}
