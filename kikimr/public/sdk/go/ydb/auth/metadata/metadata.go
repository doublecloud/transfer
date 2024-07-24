package metadata

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/timeutil"
)

var (
	DefaultAddr = "localhost:6770"
	DefaultPath = "/latest/meta-data/iam/security-credentials/default"
)

const (
	codeSuccess = "Success"
)

type Client struct {
	Addr string
	Path string

	Trace ClientTrace
	Dial  func(ctx context.Context, network, addr string) (net.Conn, error)

	once     sync.Once
	addr     string
	reqBytes []byte
	req      *http.Request

	mu      sync.RWMutex
	expires time.Time
	token   string

	promise *tokenPromise
}

func (c *Client) Token(ctx context.Context) (token string, err error) {
	c.mu.RLock()
	if !c.expired() {
		token = c.token
	}
	c.mu.RUnlock()
	if token != "" {
		return
	}

	c.mu.Lock()
	if !c.expired() {
		token = c.token
		c.mu.Unlock()
		return
	}
	p := c.obtain()
	c.mu.Unlock()

	select {
	case <-p.done:
		if err = p.err; err != nil {
			return
		}
		if p.expires.Before(timeutil.Now()) {
			return "", fmt.Errorf("metadata: received already expired token")
		}
		return p.token, nil

	case <-ctx.Done():
		c.mu.Lock()
		c.gone(p)
		c.mu.Unlock()

		return "", ctx.Err()
	}
}

// c.mu must be held (any type).
func (c *Client) expired() bool {
	return c.expires.Before(timeutil.Now())
}

type tokenPromise struct {
	done    chan struct{}
	token   string
	expires time.Time
	err     error

	waiters int64
	cancel  func()
}

// c.mu must be held.
func (c *Client) gone(p *tokenPromise) {
	if c.promise != p {
		return
	}
	if p.waiters == 1 {
		p.cancel()
		c.promise = nil
	} else {
		p.waiters--
	}
}

// c.mu must be held.
func (c *Client) obtain() *tokenPromise {
	if c.promise != nil {
		c.promise.waiters++
		return c.promise
	}

	ctx, cancel := context.WithCancel(context.Background())
	p := &tokenPromise{
		done:    make(chan struct{}),
		cancel:  cancel,
		waiters: 1,
	}
	c.promise = p

	go func() {
		p.token, p.expires, p.err = c.request(ctx)

		c.mu.Lock()
		defer func() {
			c.mu.Unlock()
			close(p.done)
		}()
		if c.promise != p {
			return
		}
		c.promise = nil
		if p.err == nil {
			c.token = p.token
			c.expires = p.expires
		}
	}()

	return p
}

type response struct {
	Code       string
	Expiration string
	Token      string
}

func (c *Client) request(ctx context.Context) (
	token string, expires time.Time, err error,
) {
	c.init()
	conn, err := c.dial(ctx)
	if err != nil {
		return
	}
	defer conn.Close()

	if ctx != context.Background() {
		cancel := setupDeadline(ctx, conn)
		defer cancel(&err)
	}
	clientTraceWriteRequestDone := clientTraceOnWriteRequest(c.Trace, conn)
	_, err = conn.Write(c.reqBytes)
	clientTraceWriteRequestDone(conn, err)
	if err != nil {
		return
	}

	var res response
	clientTraceReadResponseDone := clientTraceOnReadResponse(c.Trace, conn)
	defer func() {
		clientTraceReadResponseDone(conn, res.Code, expires, err)
	}()

	buf := bufio.NewReader(conn)
	resp, err := http.ReadResponse(buf, c.req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&res)
	if err != nil {
		return
	}
	if res.Code != codeSuccess {
		err = fmt.Errorf("metadata: unexpected status code: %q", res.Code)
		return
	}
	expires, err = time.Parse(time.RFC3339, res.Expiration)
	if err != nil {
		return
	}

	return res.Token, expires, nil
}

var zeroDialer net.Dialer

func (c *Client) dial(ctx context.Context) (net.Conn, error) {
	dial := c.Dial
	if dial == nil {
		dial = zeroDialer.DialContext
	}
	clientTraceDialDone := clientTraceOnDial(c.Trace, "tcp", c.addr)
	conn, err := dial(ctx, "tcp", c.addr)
	clientTraceDialDone("tcp", c.addr, conn, err)

	return conn, err
}

func (c *Client) init() {
	c.once.Do(func() {
		c.addr = c.Addr
		if c.addr == "" {
			c.addr = DefaultAddr
		}

		path := c.Path
		if path == "" {
			path = DefaultPath
		}
		var buf bytes.Buffer
		req, err := http.NewRequest(http.MethodGet, path, nil)
		if err == nil {
			req.Host = c.addr
			err = req.Write(&buf)
		}
		if err != nil {
			panic("metadata: could not build http request: " + err.Error())
		}
		c.req = req
		c.reqBytes = buf.Bytes()
	})
}

var aLongTimeAgo = time.Unix(0, 0)

func setupDeadline(ctx context.Context, conn net.Conn) (cancel func(*error)) {
	var (
		done      = make(chan struct{})
		interrupt = make(chan error, 1)
	)
	go func() {
		select {
		case <-done:
			interrupt <- nil
		case <-ctx.Done():
			_ = conn.SetDeadline(aLongTimeAgo)
			interrupt <- ctx.Err()
		}
	}()
	return func(errp *error) {
		close(done)
		if err := <-interrupt; err != nil {
			*errp = err
		}
	}
}
