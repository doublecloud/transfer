package elastic

import (
	"github.com/doublecloud/transfer/cloud/bitbucket/private-api/yandex/cloud/priv/dynamicform/v1"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
)

type ElasticSearchHostPort struct {
	Host string
	Port int
}

type ElasticSearchDestination struct {
	ClusterID        string // Deprecated: new endpoints should be on premise only
	DataNodes        []ElasticSearchHostPort
	User             string
	Password         server.SecretString
	SSLEnabled       bool
	TLSFile          string
	SubNetworkID     string
	SecurityGroupIDs []string
	Cleanup          server.CleanupType

	SanitizeDocKeys bool
}

var _ server.Destination = (*ElasticSearchDestination)(nil)

func (d *ElasticSearchDestination) ToElasticSearchDestination() (*ElasticSearchDestination, ServerType) {
	return d, ElasticSearch
}

func (d *ElasticSearchDestination) Hosts() []string {
	result := make([]string, 0)
	for _, el := range d.DataNodes {
		result = append(result, el.Host)
	}
	return result
}

func (d *ElasticSearchDestination) GetLink(linkID string, folderID string) (*dynamicform.Link, error) {
	if linkID == "elasticsearch_mdb" {
		if d.ClusterID != "" {
			return &dynamicform.Link{
				Type: dynamicform.LinkType_ELASTICSEARCH,
				Link: &dynamicform.Link_Cloud{
					Cloud: &dynamicform.CloudLink{
						ClusterId: d.ClusterID,
						FolderId:  folderID,
					},
				},
			}, nil
		}
		return nil, nil
	}
	return nil, xerrors.Errorf("unknown link id :'%s'", linkID)
}

func (d *ElasticSearchDestination) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (d *ElasticSearchDestination) Validate() error {
	if d.MDBClusterID() == "" &&
		len(d.DataNodes) == 0 {
		return xerrors.Errorf("no host specified")
	}
	if !d.SSLEnabled && len(d.TLSFile) > 0 {
		return xerrors.Errorf("can't use CA certificate with disabled SSL")
	}
	return nil
}

func (d *ElasticSearchDestination) WithDefaults() {
}

func (d *ElasticSearchDestination) VPCSubnets() []string {
	if d.SubNetworkID == "" {
		return nil
	}
	return []string{d.SubNetworkID}
}

func (d *ElasticSearchDestination) VPCSecurityGroups() []string {
	return d.SecurityGroupIDs
}

func (d *ElasticSearchDestination) MDBClusterID() string {
	return d.ClusterID
}

func (d *ElasticSearchDestination) IsDestination() {}

func (d *ElasticSearchDestination) Transformer() map[string]string {
	// TODO: this is a legacy method. Drop it when it is dropped from the interface.
	return make(map[string]string)
}

func (d *ElasticSearchDestination) CleanupMode() server.CleanupType {
	return d.Cleanup
}

func (d *ElasticSearchDestination) Compatible(src server.Source, transferType abstract.TransferType) error {
	if transferType == abstract.TransferTypeSnapshotOnly || server.IsAppendOnlySource(src) {
		return nil
	}
	return xerrors.Errorf("ElasticSearch target supports only AppendOnly sources or snapshot transfers")
}
