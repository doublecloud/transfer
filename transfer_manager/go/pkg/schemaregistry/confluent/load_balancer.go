package confluent

import (
	"net/http"
	"net/url"
)

type balancer interface {
	Next() *url.URL
}

type roundRobinBalancer struct {
	backendServers []*url.URL
	currentIndex   int
}

func newRoundRobinLoadBalancer(backendServers []*url.URL) *roundRobinBalancer {
	return &roundRobinBalancer{
		backendServers: backendServers,
		currentIndex:   0,
	}
}

func (b *roundRobinBalancer) Next() *url.URL {
	backendURL := b.backendServers[b.currentIndex]
	b.currentIndex = (b.currentIndex + 1) % len(b.backendServers)
	return backendURL
}

type httpDoClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type balancedClient struct {
	balancer balancer
	httpDoClient
}

func newBalancedClient(balancer balancer, client httpDoClient) *balancedClient {
	return &balancedClient{
		balancer:     balancer,
		httpDoClient: client,
	}
}

func (c *balancedClient) Do(req *http.Request) (*http.Response, error) {
	target := c.balancer.Next()
	req.URL.Scheme = target.Scheme
	req.URL.Host = target.Host
	if target.Path != "" {
		req.URL.Path = target.JoinPath(req.URL.Path).Path
	}
	return c.httpDoClient.Do(req)
}
