package podagent

import "go.ytsaurus.tech/library/go/core/log"

type Option func(client *Client)

func WithEndpoint(endpointURL string) Option {
	return func(c *Client) {
		c.httpc.SetBaseURL(endpointURL)
	}
}

func WithLogger(l log.Fmt) Option {
	return func(c *Client) {
		c.httpc.SetLogger(l)
	}
}
