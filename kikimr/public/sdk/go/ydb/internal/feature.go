package internal

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type FeatureFlag int

const (
	FeatureUnknown FeatureFlag = iota
	FeatureEnabled
	FeatureDisabled
)

func (f FeatureFlag) ToYDB() Ydb.FeatureFlag_Status {
	switch f {
	case FeatureEnabled:
		return Ydb.FeatureFlag_ENABLED
	case FeatureDisabled:
		return Ydb.FeatureFlag_DISABLED
	case FeatureUnknown:
		return Ydb.FeatureFlag_STATUS_UNSPECIFIED
	default:
		panic("ydb: unknown feature flag status")
	}
}

func FeatureFlagFromYDB(f Ydb.FeatureFlag_Status) FeatureFlag {
	switch f {
	case Ydb.FeatureFlag_ENABLED:
		return FeatureEnabled
	case Ydb.FeatureFlag_DISABLED:
		return FeatureDisabled
	case Ydb.FeatureFlag_STATUS_UNSPECIFIED:
		return FeatureUnknown
	default:
		panic("ydb: unknown Ydb feature flag status")
	}
}
