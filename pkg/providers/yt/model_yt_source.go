package yt

import (
	"github.com/doublecloud/transfer/pkg/abstract"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/config/env"
	ytclient "github.com/doublecloud/transfer/pkg/providers/yt/client"
	"github.com/dustin/go-humanize"
	"go.ytsaurus.tech/yt/go/yt"
)

type ConnectionData struct {
	Hosts                 []string
	Subnet                string
	SecurityGroups        []string
	DisableProxyDiscovery bool
}

type YtSource struct {
	Cluster          string
	Proxy            string
	Paths            []string
	YtToken          string
	RowIdxColumnName string

	DesiredPartSizeBytes int64
	Connection           ConnectionData
}

var _ server.Source = (*YtSource)(nil)

func (s *YtSource) IsSource()       {}
func (s *YtSource) IsStrictSource() {}

func (s *YtSource) WithDefaults() {
	if s.Cluster == "" && env.In(env.EnvironmentInternal) {
		s.Cluster = "hahn"
	}
	if s.DesiredPartSizeBytes == 0 {
		s.DesiredPartSizeBytes = 1 * humanize.GiByte
	}
}

func (s *YtSource) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (s *YtSource) Validate() error {
	return nil
}

func (s *YtSource) IsAbstract2(server.Destination) bool { return true }

func (s *YtSource) RowIdxEnabled() bool {
	return s.RowIdxColumnName != ""
}

func (s *YtSource) IsAsyncShardPartsSource() {}

func (s *YtSource) ConnParams() ytclient.ConnParams {
	return ytSrcWrapper{s}
}

type ytSrcWrapper struct {
	*YtSource
}

func (y ytSrcWrapper) Proxy() string {
	if y.YtSource.Proxy != "" {
		return y.YtSource.Proxy
	}
	return y.YtSource.Cluster
}

func (y ytSrcWrapper) Token() string {
	return y.YtToken
}

func (y ytSrcWrapper) DisableProxyDiscovery() bool {
	return y.Connection.DisableProxyDiscovery
}

func (y ytSrcWrapper) CompressionCodec() yt.ClientCompressionCodec {
	return yt.ClientCodecBrotliFastest
}
