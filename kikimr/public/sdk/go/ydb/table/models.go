package table

import (
	"time"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/internal"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
)

type SessionStatus byte

const (
	SessionStatusUnknown SessionStatus = iota
	SessionReady
	SessionBusy
)

func (s SessionStatus) String() string {
	switch s {
	case SessionReady:
		return "ready"
	case SessionBusy:
		return "busy"
	default:
		return "unknown"
	}
}

type SessionInfo struct {
	Status SessionStatus
}

type Column struct {
	Name   string
	Type   ydb.Type
	Family string
}

func (c Column) toYDB() *Ydb_Table.ColumnMeta {
	return &Ydb_Table.ColumnMeta{
		Name:   c.Name,
		Type:   internal.TypeToYDB(c.Type),
		Family: c.Family,
	}
}

type IndexDescription struct {
	Name         string
	IndexColumns []string
	Status       Ydb_Table.TableIndexDescription_Status
}

func (i IndexDescription) toYDB() *Ydb_Table.TableIndexDescription {
	return &Ydb_Table.TableIndexDescription{
		Name:         i.Name,
		IndexColumns: i.IndexColumns,
		Status:       i.Status,
	}
}

type Description struct {
	Name                 string
	Columns              []Column
	PrimaryKey           []string
	KeyRanges            []KeyRange
	Stats                *TableStats
	ColumnFamilies       []ColumnFamily
	Attributes           map[string]string
	ReadReplicaSettings  ReadReplicasSettings
	StorageSettings      StorageSettings
	KeyBloomFilter       ydb.FeatureFlag
	PartitioningSettings PartitioningSettings
	Indexes              []IndexDescription

	// Deprecated: use TimeToLiveSettings instead.
	// Will be removed after Jan 2022.
	TTLSettings        *TTLSettings
	TimeToLiveSettings *TimeToLiveSettings
}

type TableStats struct {
	PartitionStats   []PartitionStats
	RowsEstimate     uint64
	StoreSize        uint64
	Partitions       uint64
	CreationTime     time.Time
	ModificationTime time.Time
}

type PartitionStats struct {
	RowsEstimate uint64
	StoreSize    uint64
}

type ColumnFamily struct {
	Name         string
	Data         StoragePool
	Compression  ColumnFamilyCompression
	KeepInMemory ydb.FeatureFlag
}

func (c ColumnFamily) toYDB() *Ydb_Table.ColumnFamily {
	return &Ydb_Table.ColumnFamily{
		Name:         c.Name,
		Data:         c.Data.toYDB(),
		Compression:  c.Compression.toYDB(),
		KeepInMemory: c.KeepInMemory.ToYDB(),
	}
}

func columnFamily(c *Ydb_Table.ColumnFamily) ColumnFamily {
	return ColumnFamily{
		Name:         c.Name,
		Data:         storagePool(c.GetData()),
		Compression:  columnFamilyCompression(c.Compression),
		KeepInMemory: internal.FeatureFlagFromYDB(c.KeepInMemory),
	}
}

type StoragePool struct {
	Media string
}

func (s StoragePool) toYDB() *Ydb_Table.StoragePool {
	if s.Media == "" {
		return nil
	}
	return &Ydb_Table.StoragePool{
		Media: s.Media,
	}
}

func storagePool(s *Ydb_Table.StoragePool) StoragePool {
	return StoragePool{
		Media: s.GetMedia(),
	}
}

type ColumnFamilyCompression byte

const (
	ColumnFamilyCompressionUnknown ColumnFamilyCompression = iota
	ColumnFamilyCompressionNone
	ColumnFamilyCompressionLZ4
)

func (c ColumnFamilyCompression) String() string {
	switch c {
	case ColumnFamilyCompressionNone:
		return "none"
	case ColumnFamilyCompressionLZ4:
		return "lz4"
	default:
		return "unknown"
	}
}

func (c ColumnFamilyCompression) toYDB() Ydb_Table.ColumnFamily_Compression {
	switch c {
	case ColumnFamilyCompressionNone:
		return Ydb_Table.ColumnFamily_COMPRESSION_NONE
	case ColumnFamilyCompressionLZ4:
		return Ydb_Table.ColumnFamily_COMPRESSION_LZ4
	default:
		return Ydb_Table.ColumnFamily_COMPRESSION_UNSPECIFIED
	}
}

func columnFamilyCompression(c Ydb_Table.ColumnFamily_Compression) ColumnFamilyCompression {
	switch c {
	case Ydb_Table.ColumnFamily_COMPRESSION_NONE:
		return ColumnFamilyCompressionNone
	case Ydb_Table.ColumnFamily_COMPRESSION_LZ4:
		return ColumnFamilyCompressionLZ4
	default:
		return ColumnFamilyCompressionUnknown
	}
}

type (
	describeTableDesc   Ydb_Table.DescribeTableRequest
	DescribeTableOption func(d *describeTableDesc)
)

type ReadReplicasSettings struct {
	Type  ReadReplicasType
	Count uint64
}

func (rr ReadReplicasSettings) toYDB() *Ydb_Table.ReadReplicasSettings {
	switch rr.Type {
	case ReadReplicasPerAzReadReplicas:
		return &Ydb_Table.ReadReplicasSettings{
			Settings: &Ydb_Table.ReadReplicasSettings_PerAzReadReplicasCount{
				PerAzReadReplicasCount: rr.Count,
			},
		}

	default:
		return &Ydb_Table.ReadReplicasSettings{
			Settings: &Ydb_Table.ReadReplicasSettings_AnyAzReadReplicasCount{
				AnyAzReadReplicasCount: rr.Count,
			},
		}
	}
}

func readReplicasSettings(rr *Ydb_Table.ReadReplicasSettings) ReadReplicasSettings {
	t, c := ReadReplicasPerAzReadReplicas, uint64(0)

	if c = rr.GetPerAzReadReplicasCount(); c != 0 {
		t = ReadReplicasPerAzReadReplicas
	} else if c = rr.GetAnyAzReadReplicasCount(); c != 0 {
		t = ReadReplicasAnyAzReadReplicas
	}

	return ReadReplicasSettings{
		Type:  t,
		Count: c,
	}
}

type ReadReplicasType byte

const (
	ReadReplicasPerAzReadReplicas ReadReplicasType = iota
	ReadReplicasAnyAzReadReplicas
)

type StorageSettings struct {
	TableCommitLog0    StoragePool
	TableCommitLog1    StoragePool
	External           StoragePool
	StoreExternalBlobs ydb.FeatureFlag
}

func (ss StorageSettings) toYDB() *Ydb_Table.StorageSettings {
	return &Ydb_Table.StorageSettings{
		TabletCommitLog0:   ss.TableCommitLog0.toYDB(),
		TabletCommitLog1:   ss.TableCommitLog1.toYDB(),
		External:           ss.External.toYDB(),
		StoreExternalBlobs: ss.StoreExternalBlobs.ToYDB(),
	}
}

func storageSettings(ss *Ydb_Table.StorageSettings) StorageSettings {
	return StorageSettings{
		TableCommitLog0:    storagePool(ss.GetTabletCommitLog0()),
		TableCommitLog1:    storagePool(ss.GetTabletCommitLog1()),
		External:           storagePool(ss.GetExternal()),
		StoreExternalBlobs: internal.FeatureFlagFromYDB(ss.GetStoreExternalBlobs()),
	}
}

type PartitioningSettings struct {
	PartitioningBySize ydb.FeatureFlag
	PartitionSizeMb    uint64
	PartitioningByLoad ydb.FeatureFlag
	MinPartitionsCount uint64
	MaxPartitionsCount uint64
}

func (ps PartitioningSettings) toYDB() *Ydb_Table.PartitioningSettings {
	return &Ydb_Table.PartitioningSettings{
		PartitioningBySize: ps.PartitioningBySize.ToYDB(),
		PartitionSizeMb:    ps.PartitionSizeMb,
		PartitioningByLoad: ps.PartitioningByLoad.ToYDB(),
		MinPartitionsCount: ps.MinPartitionsCount,
		MaxPartitionsCount: ps.MaxPartitionsCount,
	}
}

func partitioningSettings(ps *Ydb_Table.PartitioningSettings) PartitioningSettings {
	return PartitioningSettings{
		PartitioningBySize: internal.FeatureFlagFromYDB(ps.GetPartitioningBySize()),
		PartitionSizeMb:    ps.GetPartitionSizeMb(),
		PartitioningByLoad: internal.FeatureFlagFromYDB(ps.GetPartitioningByLoad()),
		MinPartitionsCount: ps.GetMinPartitionsCount(),
		MaxPartitionsCount: ps.GetMaxPartitionsCount(),
	}
}

func ttlSettings(s *Ydb_Table.TtlSettings) *TTLSettings {
	if s == nil {
		return nil
	}
	switch mode := s.Mode.(type) {
	// for the time being the only implementation of Mode
	case *Ydb_Table.TtlSettings_DateTypeColumn:
		c := mode.DateTypeColumn
		return &TTLSettings{
			DateTimeColumn: c.ColumnName,
			TTLSeconds:     c.ExpireAfterSeconds,
		}
	default:
		return nil
	}
}

type IndexType interface {
	setup(*indexDesc)
}

type globalIndex struct{}

func GlobalIndex() IndexType {
	return globalIndex{}
}

func (globalIndex) setup(d *indexDesc) {
	d.Type = &Ydb_Table.TableIndex_GlobalIndex{
		GlobalIndex: new(Ydb_Table.GlobalIndex),
	}
}

type globalAsyncIndex struct{}

func GlobalAsyncIndex() IndexType {
	return globalAsyncIndex{}
}

func (globalAsyncIndex) setup(d *indexDesc) {
	d.Type = &Ydb_Table.TableIndex_GlobalAsyncIndex{
		GlobalAsyncIndex: new(Ydb_Table.GlobalAsyncIndex),
	}
}

type PartitioningMode byte

const (
	PartitioningUnknown PartitioningMode = iota
	PartitioningDisabled
	PartitioningAutoSplit
	PartitioningAutoSplitMerge
)

func (p PartitioningMode) toYDB() Ydb_Table.PartitioningPolicy_AutoPartitioningPolicy {
	switch p {
	case PartitioningDisabled:
		return Ydb_Table.PartitioningPolicy_DISABLED
	case PartitioningAutoSplit:
		return Ydb_Table.PartitioningPolicy_AUTO_SPLIT
	case PartitioningAutoSplitMerge:
		return Ydb_Table.PartitioningPolicy_AUTO_SPLIT_MERGE
	default:
		panic("ydb: unknown partitioning mode")
	}
}

type ExecuteScanQueryRequestMode byte

const (
	ExecuteScanQueryRequestModeExec ExecuteScanQueryRequestMode = iota
	ExecuteScanQueryRequestModeExplain
)

func (p ExecuteScanQueryRequestMode) toYDB() Ydb_Table.ExecuteScanQueryRequest_Mode {
	switch p {
	case ExecuteScanQueryRequestModeExec:
		return Ydb_Table.ExecuteScanQueryRequest_MODE_EXEC
	case ExecuteScanQueryRequestModeExplain:
		return Ydb_Table.ExecuteScanQueryRequest_MODE_EXPLAIN
	default:
		panic("ydb: unknown execute scan query mode")
	}
}

type TableOptionsDescription struct {
	TableProfilePresets       []TableProfileDescription
	StoragePolicyPresets      []StoragePolicyDescription
	CompactionPolicyPresets   []CompactionPolicyDescription
	PartitioningPolicyPresets []PartitioningPolicyDescription
	ExecutionPolicyPresets    []ExecutionPolicyDescription
	ReplicationPolicyPresets  []ReplicationPolicyDescription
	CachingPolicyPresets      []CachingPolicyDescription
}

type (
	TableProfileDescription struct {
		Name   string
		Labels map[string]string

		DefaultStoragePolicy      string
		DefaultCompactionPolicy   string
		DefaultPartitioningPolicy string
		DefaultExecutionPolicy    string
		DefaultReplicationPolicy  string
		DefaultCachingPolicy      string

		AllowedStoragePolicies      []string
		AllowedCompactionPolicies   []string
		AllowedPartitioningPolicies []string
		AllowedExecutionPolicies    []string
		AllowedReplicationPolicies  []string
		AllowedCachingPolicies      []string
	}
	StoragePolicyDescription struct {
		Name   string
		Labels map[string]string
	}
	CompactionPolicyDescription struct {
		Name   string
		Labels map[string]string
	}
	PartitioningPolicyDescription struct {
		Name   string
		Labels map[string]string
	}
	ExecutionPolicyDescription struct {
		Name   string
		Labels map[string]string
	}
	ReplicationPolicyDescription struct {
		Name   string
		Labels map[string]string
	}
	CachingPolicyDescription struct {
		Name   string
		Labels map[string]string
	}
)

type KeyRange struct {
	From ydb.Value
	To   ydb.Value
}

// Deprecated use TimeToLiveSettings instead.
// Will be removed after Jan 2022.
type TTLSettings struct {
	DateTimeColumn string
	TTLSeconds     uint32
}

type TimeToLiveSettings struct {
	ColumnName         string
	ExpireAfterSeconds uint32

	ColumnUnit *TimeToLiveUnit // valid with Mode = TimeToLiveModeValueSinceUnixEpoch
	// Specifies mode
	Mode TimeToLiveMode
}

type TimeToLiveMode byte

const (
	TimeToLiveModeDateType TimeToLiveMode = iota
	TimeToLiveModeValueSinceUnixEpoch
)

func (ttl *TimeToLiveSettings) ToYDB() *Ydb_Table.TtlSettings {
	if ttl == nil {
		return nil
	}
	switch ttl.Mode {
	case TimeToLiveModeValueSinceUnixEpoch:
		return &Ydb_Table.TtlSettings{
			Mode: &Ydb_Table.TtlSettings_ValueSinceUnixEpoch{
				ValueSinceUnixEpoch: &Ydb_Table.ValueSinceUnixEpochModeSettings{
					ColumnName:         ttl.ColumnName,
					ColumnUnit:         ttl.ColumnUnit.ToYDB(),
					ExpireAfterSeconds: ttl.ExpireAfterSeconds,
				},
			},
		}
	default: // currently use TimeToLiveModeDateType mode as default
		return &Ydb_Table.TtlSettings{
			Mode: &Ydb_Table.TtlSettings_DateTypeColumn{
				DateTypeColumn: &Ydb_Table.DateTypeColumnModeSettings{
					ColumnName:         ttl.ColumnName,
					ExpireAfterSeconds: ttl.ExpireAfterSeconds,
				},
			},
		}
	}
}

func timeToLiveSettings(settings *Ydb_Table.TtlSettings) *TimeToLiveSettings {
	if settings == nil {
		return nil
	}
	switch mode := settings.Mode.(type) {
	case *Ydb_Table.TtlSettings_DateTypeColumn:
		return &TimeToLiveSettings{
			ColumnName:         mode.DateTypeColumn.ColumnName,
			ExpireAfterSeconds: mode.DateTypeColumn.ExpireAfterSeconds,
			Mode:               TimeToLiveModeDateType,
		}

	case *Ydb_Table.TtlSettings_ValueSinceUnixEpoch:
		return &TimeToLiveSettings{
			ColumnName:         mode.ValueSinceUnixEpoch.ColumnName,
			ColumnUnit:         timeToLiveUnit(mode.ValueSinceUnixEpoch.ColumnUnit),
			ExpireAfterSeconds: mode.ValueSinceUnixEpoch.ExpireAfterSeconds,
			Mode:               TimeToLiveModeValueSinceUnixEpoch,
		}
	}
	return nil
}

type TimeToLiveUnit int32

const (
	TimeToLiveUnitUnspecified TimeToLiveUnit = iota
	TimeToLiveUnitSeconds
	TimeToLiveUnitMilliseconds
	TimeToLiveUnitMicroseconds
	TimeToLiveUnitNanoseconds
)

func (unit *TimeToLiveUnit) ToYDB() Ydb_Table.ValueSinceUnixEpochModeSettings_Unit {
	if unit == nil {
		return Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_UNSPECIFIED
	}
	switch *unit {
	case TimeToLiveUnitSeconds:
		return Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_SECONDS
	case TimeToLiveUnitMilliseconds:
		return Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_MILLISECONDS
	case TimeToLiveUnitMicroseconds:
		return Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_MICROSECONDS
	case TimeToLiveUnitNanoseconds:
		return Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_NANOSECONDS
	case TimeToLiveUnitUnspecified:
		return Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_UNSPECIFIED
	default:
		panic("ydb: unknown unit for value since epoch")
	}
}

func timeToLiveUnit(unit Ydb_Table.ValueSinceUnixEpochModeSettings_Unit) *TimeToLiveUnit {
	var res TimeToLiveUnit
	switch unit {
	case Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_SECONDS:
		res = TimeToLiveUnitSeconds
	case Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_MILLISECONDS:
		res = TimeToLiveUnitMilliseconds
	case Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_MICROSECONDS:
		res = TimeToLiveUnitMicroseconds
	case Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_NANOSECONDS:
		res = TimeToLiveUnitNanoseconds
	case Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_UNSPECIFIED:
		res = TimeToLiveUnitUnspecified
	default:
		panic("ydb: unknown Ydb unit for value since epoch")
	}
	return &res
}
