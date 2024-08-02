package httppusher

import (
	"context"
	"time"

	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/library/go/core/metrics/solomon"
	"github.com/doublecloud/tross/library/go/yandex/solomon/reporters/pusher"
	"github.com/doublecloud/tross/library/go/yandex/tvm"
	"github.com/go-resty/resty/v2"
)

type PusherOpt = func(*Pusher) error

// WithHTTPHost rewrites default HTTP host in client.
func WithHTTPHost(hostURL string) PusherOpt {
	return func(p *Pusher) error {
		p.httpc.SetBaseURL(hostURL)
		return nil
	}
}

// WithLogger redirects default resty debug output to custom logger.
func WithLogger(l log.Structured) PusherOpt {
	return func(p *Pusher) error {
		p.logger = l
		return nil
	}
}

// WithOAuthToken sets global authorization OAuth token to any client request
func WithOAuthToken(token string) PusherOpt {
	return func(p *Pusher) error {
		p.httpc.SetHeader("Authorization", "OAuth "+token)
		return nil
	}
}

// WithIAMTokenProvider allows custom providers with token in runtime
func WithIAMTokenProvider(provider func(ctx context.Context) (string, error)) PusherOpt {
	return func(p *Pusher) error {
		p.authProvider = func(ctx context.Context) (string, string, error) {
			header := "Authorization"
			value := ""
			token, err := provider(ctx)
			if err != nil {
				return header, value, err
			}
			value = "Bearer " + token
			return header, value, nil
		}
		return nil
	}
}

// WithTVMTokenProvider allows to receive TVM service tickets at runtime
func WithTVMTokenProvider(client tvm.Client, dstID tvm.ClientID) PusherOpt {
	return func(p *Pusher) error {
		p.authProvider = func(ctx context.Context) (string, string, error) {
			header := "X-Ya-Service-Ticket"
			token, err := client.GetServiceTicketForID(ctx, dstID)

			if err != nil {
				return header, "", err
			}

			return header, token, nil
		}
		return nil
	}
}

// WithIAMToken sets global authorization IAM token to any client request
func WithIAMToken(token string) PusherOpt {
	return func(p *Pusher) error {
		p.httpc.SetHeader("Authorization", "Bearer "+token)
		return nil
	}
}

func WithSpack(compression solomon.CompressionType) PusherOpt {
	return func(p *Pusher) error {
		p.useSpack = &compression
		return nil
	}
}

func WithMetricsChunkSize(metricsChunkSize int) PusherOpt {
	return func(p *Pusher) error {
		p.metricsChunkSize = metricsChunkSize
		return nil
	}
}

// SetCluster overrides cluster request query arg value
func SetCluster(cluster string) PusherOpt {
	return func(p *Pusher) error {
		p.httpc.SetQueryParam("cluster", cluster)
		return nil
	}
}

// SetProject overrides project request query arg value
func SetProject(project string) PusherOpt {
	return func(p *Pusher) error {
		p.httpc.SetQueryParam("project", project)
		return nil
	}
}

// SetService overrides service request query arg value
func SetService(service string) PusherOpt {
	return func(p *Pusher) error {
		p.httpc.SetQueryParam("service", service)
		return nil
	}
}

// WithLastSend enables last send of metrics after context is done.
// Timeout must be greater than zero, otherwise the last send is disabled.
func WithLastSend(timeout time.Duration) PusherOpt {
	return func(p *Pusher) error {
		p.lastSendTimeout = timeout
		return nil
	}
}

// WithCluster overrides cluster request query arg value
func WithCluster(cluster string) pusher.PusherRequestOpt {
	return func(r *resty.Request) error {
		r.SetQueryParam("cluster", cluster)
		return nil
	}
}

// WithProject overrides project request query arg value
func WithProject(project string) pusher.PusherRequestOpt {
	return func(r *resty.Request) error {
		r.SetQueryParam("project", project)
		return nil
	}
}

// WithService overrides service request query arg value
func WithService(service string) pusher.PusherRequestOpt {
	return func(r *resty.Request) error {
		r.SetQueryParam("service", service)
		return nil
	}
}
