package iam

import (
	"context"
	"sync"
	"time"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb"
)

var _ ydb.Credentials = &instanceServiceAccountCredentials{}

const metadataURL = "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token"

type instanceServiceAccountCredentials struct {
	mu  *sync.RWMutex
	ctx context.Context

	token string
	err   error

	expiry time.Time
	timer  *time.Timer

	metadataURL string

	caller string
}

// Returns cached token if it is valid. Otherwise, will try to renew.
func (m *instanceServiceAccountCredentials) Token(ctx context.Context) (token string, err error) {
	for {
		select {
		case <-ctx.Done():
			return "", &CreateTokenError{
				Cause:  ctx.Err(),
				Reason: ctx.Err().Error(),
			}
		default:
			m.mu.RLock()
			token, err = m.token, m.err
			m.mu.RUnlock()
			if token != "" || err != nil {
				return token, err
			}
			// not yet initialized, wait
		}
	}
}

func (m *instanceServiceAccountCredentials) String() string {
	if m.caller == "" {
		return "InstanceServiceAccountCredentials (metadataURL=" + m.metadataURL + ")"
	}
	return "InstanceServiceAccountCredentials created from " + m.caller + " (metadataURL=" + m.metadataURL + ")"
}

func (m *instanceServiceAccountCredentials) refreshLoop() {
	defer m.timer.Stop()
	for {
		select {
		case <-m.ctx.Done():
			// Set up error
			m.mu.Lock()
			m.token, m.err = "", &CreateTokenError{
				Cause:  m.ctx.Err(),
				Reason: m.ctx.Err().Error(),
			}
			m.mu.Unlock()
			return
		case <-m.timer.C:
			m.refreshOnce()
		}
	}
}

// Perform single refresh iteration.
// If token was obtained:
// 1. Clear current err;
// 2. Set up new token and expiration;
// Otherwise, if current token has expired, clear it and set up err.
func (m *instanceServiceAccountCredentials) refreshOnce() {
	now := time.Now()
	tok, err := metaCall(m.metadataURL)

	// Call has been performed, now updating fields
	m.mu.Lock()
	defer m.mu.Unlock()

	defer func() {
		const minInterval = 5 * time.Second
		// Reset timer: trigger after 10% of expiry.
		// NB: we are guaranteed to have drained timer here.
		interval := time.Until(m.expiry) / 10
		if interval < minInterval {
			interval = minInterval
		}
		m.timer.Reset(interval)
	}()

	if err != nil {
		// Check if current value is still good.
		if m.expiry.After(now) {
			// Will leave old token in place
			return
		}
		// Clear token and set up err
		m.token = ""
		m.err = err
		return
	}
	// Renew values.
	m.token, m.expiry, m.err = tok.Token, now.Add(tok.ExpiresIn), nil
}

// Credentials provider that uses instance metadata url to obtain token for service account attached to instance.
// Cancelling context will lead to credentials refresh halt.
// It should be used during application stop or credentials recreation.
func InstanceServiceAccountURL(ctx context.Context, url string) ydb.Credentials {
	caller, _ := ydb.ContextCredentialsSourceInfo(ctx)
	credentials := &instanceServiceAccountCredentials{
		metadataURL: url,
		mu:          &sync.RWMutex{},
		ctx:         ctx,
		timer:       time.NewTimer(0), // Allocate expired
		caller:      caller,
	}
	// Start refresh loop.
	go credentials.refreshLoop()
	return credentials
}

// Credentials provider that uses instance metadata with default url to obtain token for service account attached to instance.
// Cancelling context will lead to credentials refresh halt.
// It should be used during application stop or credentials recreation.
func InstanceServiceAccount(ctx context.Context) ydb.Credentials {
	return InstanceServiceAccountURL(ctx, metadataURL)
}
