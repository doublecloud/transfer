package postgres

import (
	"time"

	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/middlewares/async/bufferer"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/clickhouse/model"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/postgres/utils"
)

type PgDestination struct {
	// oneof
	ClusterID string `json:"Cluster"`
	Host      string // legacy field for back compatibility; for now, we are using only 'Hosts' field
	Hosts     []string

	Database               string `json:"Name"`
	User                   string
	Password               server.SecretString
	Port                   int
	TLSFile                string
	Token                  string
	MaintainTables         bool
	AllowDuplicates        bool
	LoozeMode              bool
	IgnoreUniqueConstraint bool
	Tables                 map[string]string
	TransformerConfig      map[string]string
	SubNetworkID           string
	SecurityGroupIDs       []string
	CopyUpload             bool // THIS IS NOT PARAMETER. If you set it on endpoint into true/false - nothing happened. It's workaround, this flag is set by common code (Activate/UploadTable) automatically. You have not options to turn-off CopyUpload behaviour.
	PerTransactionPush     bool
	Cleanup                server.CleanupType
	BatchSize              int // deprecated: use BufferTriggingSize instead
	BufferTriggingSize     uint64
	BufferTriggingInterval time.Duration
	QueryTimeout           time.Duration
	DisableSQLFallback     bool
}

var _ server.Destination = (*PgDestination)(nil)

const PGDefaultQueryTimeout time.Duration = 30 * time.Minute

func (d *PgDestination) MDBClusterID() string {
	return d.ClusterID
}

func (d *PgDestination) FillDependentFields(transfer *server.Transfer) {
	_, isHomo := transfer.Src.(*PgSource)
	if !isHomo && !d.MaintainTables {
		d.MaintainTables = true
	}
}

// AllHosts - function to move from legacy 'Host' into modern 'Hosts'
func (d *PgDestination) AllHosts() []string {
	return utils.HandleHostAndHosts(d.Host, d.Hosts)
}

func (d *PgDestination) HasTLS() bool {
	return d.TLSFile != ""
}

func (d *PgDestination) CleanupMode() server.CleanupType {
	return d.Cleanup
}

func (d *PgDestination) ReliesOnSystemTablesTransferring() bool {
	return d.PerTransactionPush
}

func (d *PgDestination) WithDefaults() {
	if d.Tables == nil {
		d.Tables = make(map[string]string)
	}
	if d.Port == 0 {
		d.Port = 6432
	}
	if d.Cleanup == "" {
		d.Cleanup = server.Drop
	}

	if d.BufferTriggingSize == 0 {
		d.BufferTriggingSize = model.BufferTriggingSizeDefault
	}

	if d.QueryTimeout == 0 {
		d.QueryTimeout = PGDefaultQueryTimeout
	}
}

func (d *PgDestination) BuffererConfig() bufferer.BuffererConfig {
	return bufferer.BuffererConfig{
		TriggingCount:    d.BatchSize,
		TriggingSize:     d.BufferTriggingSize,
		TriggingInterval: d.BufferTriggingInterval,
	}
}

func (d *PgDestination) Transformer() map[string]string {
	return d.TransformerConfig
}

func (PgDestination) IsDestination() {
}

func (d *PgDestination) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (d *PgDestination) Validate() error {
	return nil
}

// SinkParams

type PgDestinationWrapper struct {
	Model *PgDestination
}

func (d PgDestinationWrapper) ClusterID() string {
	return d.Model.ClusterID
}

func (d PgDestinationWrapper) AllHosts() []string {
	return d.Model.AllHosts()
}

func (d PgDestinationWrapper) Port() int {
	return d.Model.Port
}

func (d PgDestinationWrapper) Database() string {
	return d.Model.Database
}

func (d PgDestinationWrapper) User() string {
	return d.Model.User
}

func (d PgDestinationWrapper) Password() string {
	return string(d.Model.Password)
}

func (d PgDestinationWrapper) HasTLS() bool {
	return d.Model.HasTLS()
}

func (d PgDestinationWrapper) Token() string {
	return d.Model.Token
}

func (d PgDestinationWrapper) TLSFile() string {
	return d.Model.TLSFile
}

func (d PgDestinationWrapper) MaintainTables() bool {
	return d.Model.MaintainTables
}

func (d PgDestinationWrapper) PerTransactionPush() bool {
	return d.Model.PerTransactionPush
}

func (d PgDestinationWrapper) LoozeMode() bool {
	return d.Model.LoozeMode
}

func (d PgDestinationWrapper) CleanupMode() server.CleanupType {
	return d.Model.CleanupMode()
}

func (d PgDestinationWrapper) Tables() map[string]string {
	return d.Model.Tables
}

func (d PgDestinationWrapper) CopyUpload() bool {
	return d.Model.CopyUpload
}

func (d PgDestinationWrapper) IgnoreUniqueConstraint() bool {
	return d.Model.IgnoreUniqueConstraint
}

func (d PgDestinationWrapper) DisableSQLFallback() bool {
	return d.Model.DisableSQLFallback
}

func (d PgDestinationWrapper) QueryTimeout() time.Duration {
	if d.Model.QueryTimeout <= 0 {
		return PGDefaultQueryTimeout
	}
	return d.Model.QueryTimeout
}

func (d *PgDestination) ToSinkParams() PgDestinationWrapper {
	copyPgDestination := *d
	return PgDestinationWrapper{
		Model: &copyPgDestination,
	}
}

func (d *PgDestination) ToStorageParams() *PgStorageParams {
	return &PgStorageParams{
		AllHosts:                    d.AllHosts(),
		Port:                        d.Port,
		User:                        d.User,
		Password:                    string(d.Password),
		Database:                    d.Database,
		ClusterID:                   d.ClusterID,
		Token:                       d.Token,
		TLSFile:                     d.TLSFile,
		UseFakePrimaryKey:           false,
		DBFilter:                    nil,
		IgnoreUserTypes:             false,
		PreferReplica:               false,
		ExcludeDescendants:          false,
		DesiredTableSize:            pgDesiredTableSize,
		SnapshotDegreeOfParallelism: 4,
		ConnString:                  "",
		TableFilter:                 nil,
		TryHostCACertificates:       false,
		UseBinarySerialization:      false,
		SlotID:                      "",
		ShardingKeyFields:           map[string][]string{},
	}
}
