package logbroker

import (
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/parsers"
)

type LfSource struct {
	Instance                LogbrokerInstance
	Cluster                 LogbrokerCluster
	Database                string
	Token                   string
	Consumer                string
	HashColumn              string
	MaxReadSize             server.BytesSize
	MaxMemory               server.BytesSize
	MaxTimeLag              time.Duration
	InferSchema             bool
	Extra                   map[string]string
	MaxConcurrentPartitions int
	Topics                  []string
	EnrichTopic             bool
	MaxIdleTime             time.Duration
	MaxReadMessagesCount    uint32
	OnlyLocal               bool
	LfParser                bool
	Credentials             ydb.Credentials
	Port                    int
	AllowTTLRewind          bool

	// See DTSUPPORT-293, once it enable it will look for timezone in date strings, and if none presented will set it
	// to UTC not MSK, breaking change so hided behind flag
	InferTimezone bool
	Sniff         bool // will print abstract.Sniff result for each parsed batch

	IsLbSink bool // it's like IsHomo

	ParserConfig map[string]interface{}

	TLS         TLSMode
	RootCAFiles []string
}

var _ server.Source = (*LfSource)(nil)

type LogbrokerInstance string
type LogbrokerCluster string

func (s *LfSource) WithDefaults() {
	if s.MaxReadSize == 0 {
		// By default, 1 mb, we will extract it in 10-15 mbs.
		s.MaxReadSize = 1 * 1024 * 1024
	}

	if s.MaxMemory == 0 {
		// large then max memory to be able to hold at least 2 message batch in memory
		s.MaxMemory = s.MaxReadSize * 50
	}

	if s.Extra == nil {
		s.Extra = map[string]string{}
	}
}

func (LfSource) IsSource() {}

func (s *LfSource) GetProviderType() abstract.ProviderType {
	return ProviderWithParserType
}

func (s *LfSource) Validate() error {
	parserConfigStruct, err := parsers.ParserConfigMapToStruct(s.ParserConfig)
	if err != nil {
		return xerrors.Errorf("unable to make parser from config, err: %w", err)
	}
	return parserConfigStruct.Validate()
}

func (s *LfSource) IsAppendOnly() bool {
	parserConfigStruct, _ := parsers.ParserConfigMapToStruct(s.ParserConfig)
	if parserConfigStruct == nil {
		return false
	}
	return parserConfigStruct.IsAppendOnly()
}

func (s *LfSource) Parser() map[string]interface{} {
	return s.ParserConfig
}

func (s *LfSource) MultiYtEnabled() {}
