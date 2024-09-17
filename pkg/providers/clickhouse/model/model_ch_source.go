package model

import (
	"strings"
	"time"

	"github.com/doublecloud/transfer/pkg/abstract"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
)

type ClickhouseIOFormat string

const (
	ClickhouseIOFormatCSV         = ClickhouseIOFormat("CSV")
	ClickhouseIOFormatJSONCompact = ClickhouseIOFormat("JSONCompactEachRow")
	DefaultUser                   = "admin"
)

type ClickHouseShard struct {
	Name  string
	Hosts []string
}

type ChSource struct {
	MdbClusterID     string `json:"ClusterID"`
	ChClusterName    string // CH cluster from which data will be transfered. Other clusters would be ignored.
	ShardsList       []ClickHouseShard
	HTTPPort         int
	NativePort       int
	User             string
	Password         server.SecretString
	SSLEnabled       bool
	PemFileContent   string
	Database         string
	SubNetworkID     string
	SecurityGroupIDs []string
	IncludeTables    []string
	ExcludeTables    []string
	IsHomo           bool
	BufferSize       uint64
	IOHomoFormat     ClickhouseIOFormat // one of - https://clickhouse.com/docs/en/interfaces/formats
	RootCACertPaths  []string
}

var _ server.Source = (*ChSource)(nil)

func (s *ChSource) MDBClusterID() string {
	return s.MdbClusterID
}

func (s *ChSource) WithDefaults() {
	if s.NativePort == 0 {
		s.NativePort = 9440
	}
	if s.HTTPPort == 0 {
		s.HTTPPort = 8443
	}
	if s.BufferSize == 0 {
		s.BufferSize = 50 * 1024 * 1024
	}
}

func (*ChSource) IsSource()                      {}
func (*ChSource) IsIncremental()                 {}
func (*ChSource) SupportsStartCursorValue() bool { return true }

func (s *ChSource) GetProviderType() abstract.ProviderType {
	return "ch"
}

func (s *ChSource) ClusterID() string {
	return s.MdbClusterID
}

func (s *ChSource) Validate() error {
	return nil
}

func (s *ChSource) fulfilledIncludesImpl(tID abstract.TableID, firstIncludeOnly bool) (result []string) {
	tIDVariants := []string{
		tID.Fqtn(),
		strings.Join([]string{tID.Namespace, ".", tID.Name}, ""),
		strings.Join([]string{tID.Namespace, ".", "\"", tID.Name, "\""}, ""),
		strings.Join([]string{tID.Namespace, ".", "*"}, ""),
	}
	tIDNameVariant := strings.Join([]string{"\"", tID.Name, "\""}, tID.Name)
	if s.Database == "*" {
		return []string{tID.Fqtn()}
	}
	for _, table := range s.ExcludeTables {
		for _, variant := range tIDVariants {
			if table == variant {
				return result
			}
		}
		if tID.Namespace == s.Database && (table == tID.Name || table == tIDNameVariant) {
			return result
		}
	}
	if len(s.IncludeTables) == 0 {
		if s.Database == "" {
			return []string{""}
		}
		if s.Database == tID.Namespace {
			return []string{""}
		}
		return result
	}
	for _, table := range s.IncludeTables {
		if tID.Namespace == s.Database && (table == tID.Name || table == tIDNameVariant) {
			result = append(result, table)
			if firstIncludeOnly {
				return result
			}
			continue
		}
		for _, variant := range tIDVariants {
			if table == variant {
				result = append(result, table)
				if firstIncludeOnly {
					return result
				}
				break
			}
		}
	}
	return result
}

func (s *ChSource) Include(tID abstract.TableID) bool {
	return len(s.fulfilledIncludesImpl(tID, true)) > 0
}

func (s *ChSource) FulfilledIncludes(tID abstract.TableID) (result []string) {
	return s.fulfilledIncludesImpl(tID, false)
}

func (s *ChSource) AllIncludes() []string {
	return s.IncludeTables
}

func (s *ChSource) IsAbstract2(dst server.Destination) bool {
	if _, ok := dst.(*ChDestination); ok {
		return true
	}
	return false
}

// SinkParams

type ChSourceWrapper struct {
	Model    *ChSource
	host     string   // host is here, bcs it needed only in SinkServer/SinkTable
	altHosts []string // same
}

func (s ChSourceWrapper) InsertSettings() InsertParams {
	return InsertParams{MaterializedViewsIgnoreErrors: false}
}

func (s ChSourceWrapper) RootCertPaths() []string {
	return s.Model.RootCACertPaths
}

func (s ChSourceWrapper) ShardByRoundRobin() bool {
	return false
}

func (s ChSourceWrapper) Cleanup() server.CleanupType {
	return server.DisabledCleanup
}

func (s ChSourceWrapper) MdbClusterID() string {
	return s.Model.MdbClusterID
}

func (s ChSourceWrapper) ChClusterName() string {
	return s.Model.ChClusterName
}

func (s ChSourceWrapper) User() string {
	return s.Model.User
}

func (s ChSourceWrapper) Password() string {
	return string(s.Model.Password)
}

func (s ChSourceWrapper) ResolvePassword() (string, error) {
	password, err := ResolvePassword(s.MdbClusterID(), s.User(), string(s.Model.Password))
	return password, err
}

func (s ChSourceWrapper) Database() string {
	return s.Model.Database
}

func (s ChSourceWrapper) Partition() string {
	return ""
}

func (s ChSourceWrapper) Host() *string {
	return &s.host
}

func (s ChSourceWrapper) SSLEnabled() bool {
	return s.Model.SSLEnabled || s.MdbClusterID() != ""
}

func (s ChSourceWrapper) HTTPPort() int {
	return s.Model.HTTPPort
}

func (s ChSourceWrapper) NativePort() int {
	return s.Model.NativePort
}

func (s ChSourceWrapper) TTL() string {
	return ""
}

func (s ChSourceWrapper) IsUpdateable() bool {
	return false
}

func (s ChSourceWrapper) UpsertAbsentToastedRows() bool {
	return false
}

func (s ChSourceWrapper) InferSchema() bool {
	return false
}

func (s ChSourceWrapper) MigrationOptions() ChSinkMigrationOptions {
	return ChSinkMigrationOptions{false}
}

func (s ChSourceWrapper) UploadAsJSON() bool {
	return false
}

func (s ChSourceWrapper) AnyAsString() bool {
	return false
}

func (s ChSourceWrapper) SystemColumnsFirst() bool {
	return false
}

func (s ChSourceWrapper) AltHosts() []string {
	return s.altHosts
}

func (s ChSourceWrapper) RetryCount() int {
	return 10
}

func (s ChSourceWrapper) UseSchemaInTableName() bool {
	return false
}

func (s ChSourceWrapper) ShardCol() string {
	return ""
}

func (s ChSourceWrapper) Interval() time.Duration {
	return time.Second
}

func (s ChSourceWrapper) BufferTriggingSize() uint64 {
	return 0
}

func (s ChSourceWrapper) Tables() map[string]string {
	return map[string]string{}
}

func (s ChSourceWrapper) ShardByTransferID() bool {
	return false
}

func (s ChSourceWrapper) Rotation() *server.RotatorConfig {
	return nil
}

func (s ChSourceWrapper) Shards() map[string][]string {
	shardsMap := map[string][]string{}
	for _, shard := range s.Model.ShardsList {
		shardsMap[shard.Name] = shard.Hosts
	}
	return shardsMap
}

func (s ChSourceWrapper) ColumnToShardName() map[string]string {
	return map[string]string{}
}

func (s ChSourceWrapper) PemFileContent() string {
	return s.Model.PemFileContent
}

func (s ChSourceWrapper) MakeChildServerParams(host string) ChSinkServerParams {
	newChSource := *s.Model
	newChSourceWrapper := ChSourceWrapper{
		Model:    &newChSource,
		host:     host,
		altHosts: s.altHosts,
	}
	return newChSourceWrapper
}

func (s ChSourceWrapper) MakeChildShardParams(altHosts []string) ChSinkShardParams {
	newChSource := *s.Model
	newChSourceWrapper := ChSourceWrapper{
		Model:    &newChSource,
		host:     s.host,
		altHosts: altHosts,
	}
	return newChSourceWrapper
}

func (s ChSourceWrapper) SetShards(shards map[string][]string) {
	s.Model.ShardsList = []ClickHouseShard{}
	for shardName, hosts := range shards {
		s.Model.ShardsList = append(s.Model.ShardsList, ClickHouseShard{
			Name:  shardName,
			Hosts: hosts,
		})
	}
}
