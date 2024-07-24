package ytclient

import (
	"github.com/doublecloud/tross/library/go/core/log"
	"go.ytsaurus.tech/yt/go/yt"
)

type ConnParams interface {
	Proxy() string
	Token() string
	DisableProxyDiscovery() bool
	CompressionCodec() yt.ClientCompressionCodec
}

func FromConnParams(cfg ConnParams, lgr log.Logger) (yt.Client, error) {
	ytConfig := yt.Config{
		Proxy:                 cfg.Proxy(),
		Token:                 cfg.Token(),
		AllowRequestsFromJob:  true,
		CompressionCodec:      yt.ClientCodecBrotliFastest,
		DisableProxyDiscovery: cfg.DisableProxyDiscovery(),
	}
	if cfg.CompressionCodec() != yt.ClientCodecDefault {
		ytConfig.CompressionCodec = cfg.CompressionCodec()
	}

	return NewYtClientWrapper(HTTP, lgr, &ytConfig)
}
