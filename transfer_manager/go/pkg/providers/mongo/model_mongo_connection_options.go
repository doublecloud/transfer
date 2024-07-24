package mongo

import "strings"

type MongoConnectionOptions struct {
	ClusterID  string
	Hosts      []string
	Port       int
	ReplicaSet string
	AuthSource string
	User       string
	Password   string
	CACert     TrustedCACertificate
	Direct     bool
	SRVMode    bool
}

// IsDocDB check if we connect to amazon doc DB
func (o MongoConnectionOptions) IsDocDB() bool {
	for _, h := range o.Hosts {
		if strings.Contains(h, "docdb.amazonaws.com") {
			return true
		}
	}
	return false
}

type TrustedCACertificate interface {
	isTrustedCACertificate()
}

type InlineCACertificatePEM []byte
type CACertificatePEMFilePaths []string

func (InlineCACertificatePEM) isTrustedCACertificate()    {}
func (CACertificatePEMFilePaths) isTrustedCACertificate() {}
