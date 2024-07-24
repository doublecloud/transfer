package ydb

import "google.golang.org/grpc/resolver"

type dnsResolver struct {
	builder resolver.Builder
	scheme  string
}

func (d *dnsResolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	return d.builder.Build(target, cc, opts)
}

func (d *dnsResolver) Scheme() string {
	return d.scheme
}

func newResolver(scheme string) resolver.Builder {
	return &dnsResolver{
		builder: resolver.Get("dns"),
		scheme:  scheme,
	}
}
