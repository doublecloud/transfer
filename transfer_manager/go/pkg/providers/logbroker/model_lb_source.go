package logbroker

import (
	"strings"

	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/ydb"
)

type LbSource struct {
	Instance       string
	Topic          string
	Token          string
	Consumer       string
	Database       string
	AllowTTLRewind bool
	Credentials    ydb.TokenCredentials
	Port           int

	IsLbSink bool // it's like IsHomo

	RootCAFiles []string
	TLS         TLSMode
}

var _ server.Source = (*LbSource)(nil)

const (
	Logbroker            LogbrokerCluster = "logbroker"
	Lbkx                 LogbrokerCluster = "lbkx"
	Messenger            LogbrokerCluster = "messenger"
	LogbrokerPrestable   LogbrokerCluster = "logbroker-prestable"
	Lbkxt                LogbrokerCluster = "lbkxt"
	YcLogbroker          LogbrokerCluster = "yc-logbroker"
	YcLogbrokerPrestable LogbrokerCluster = "yc-logbroker-prestable"
)

func (s *LbSource) WithDefaults() {
}

func (LbSource) IsSource() {
}

func (s *LbSource) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (s *LbSource) Validate() error {
	return nil
}

func (s *LbSource) IsTransitional() {}

func (s *LbSource) TransitionalWith(right server.TransitionalEndpoint) bool {
	if dst, ok := right.(*LbDestination); ok {
		return dst.Instance == s.Instance && dst.Topic == s.Topic
	}
	return false
}

func (s *LbSource) MultiYtEnabled() {}

func withoutLeadingSlash(str string) string {
	if strings.HasPrefix(str, "/") {
		return str[1:]
	}
	return str
}
