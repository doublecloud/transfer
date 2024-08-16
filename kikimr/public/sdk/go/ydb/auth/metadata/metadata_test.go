package metadata

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/timeutil"
)

type conn struct {
	done <-chan struct{}
	exit chan<- struct{}
	conn net.Conn
}

type MetadataServer struct {
	T          testing.TB
	ReadDelay  time.Duration
	WriteDelay time.Duration
	Token      func(exit <-chan struct{}) (code, token string, expires time.Time)
	Debug      bool

	mu    sync.Mutex
	conns []conn

	reqCount int64
	resCount int64
}

func (m *MetadataServer) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, c := range m.conns {
		close(c.exit)
		<-c.done
	}
}

func (m *MetadataServer) DialFunc(_ context.Context, network, addr string) (net.Conn, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	done := make(chan struct{})
	exit := make(chan struct{})
	server, client := net.Pipe()
	m.conns = append(m.conns, conn{
		done: done,
		exit: exit,
		conn: client,
	})

	go func() {
		select {
		case <-done:
		case <-exit:
			_ = client.SetDeadline(aLongTimeAgo)
		}
	}()
	go func() {
		defer func() {
			_ = client.Close()
			close(done)
		}()
		var (
			r io.Reader = client
			w io.Writer = client
		)
		if m.Debug {
			r = io.TeeReader(r, os.Stderr)
			w = io.MultiWriter(w, os.Stderr)
		}
		if d := m.ReadDelay; d != 0 {
			r = SleepyReader(r, d)
		}
		if d := m.WriteDelay; d != 0 {
			w = SleepyWriter(w, d)
		}
		for {
			req, err := http.ReadRequest(bufio.NewReader(r))
			if err != nil {
				if m.Debug {
					m.T.Logf("metadata stub: read request error: %v", err)
				}
				return
			}

			atomic.AddInt64(&m.reqCount, 1)

			var res response
			if m.Token != nil {
				var expires time.Time
				res.Code, res.Token, expires = m.Token(exit)
				res.Expiration = expires.Format(time.RFC3339)
			} else {
				res = response{Code: "<undefined>"}
			}
			bts, err := json.Marshal(&res)
			if err != nil {
				m.T.Fatal(err)
			}
			err = (&http.Response{
				Request:       req,
				StatusCode:    200,
				ProtoMajor:    1,
				ProtoMinor:    1,
				ContentLength: int64(len(bts)),
				Body:          ioutil.NopCloser(bytes.NewReader(bts)),
			}).Write(w)
			if err != nil {
				if m.Debug {
					m.T.Logf("metadata stub: write response error: %v", err)
				}
				return
			}
			atomic.AddInt64(&m.resCount, 1)
		}
	}()

	return server, nil
}

func (m *MetadataServer) ReqCount() int64 {
	return atomic.LoadInt64(&m.reqCount)
}
func (m *MetadataServer) ResCount() int64 {
	return atomic.LoadInt64(&m.resCount)
}

func TestClientRequest(t *testing.T) {
	tokenCh := make(chan struct{})
	m := MetadataServer{
		T: t,
		Token: func(_ <-chan struct{}) (code, token string, expires time.Time) {
			tokenCh <- struct{}{}
			return codeSuccess, "foo", timeutil.Now().Add(time.Hour)
		},
	}
	defer m.Close()

	c := Client{
		Dial: m.DialFunc,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ops := make(chan tokenAndError, 100)
	for i := 0; i < cap(ops); i++ {
		go func() { ops <- getToken(ctx, &c) }()
	}

	<-tokenCh

	for i := 0; i < cap(ops); i++ {
		if x := <-ops; x.err != nil {
			t.Fatal(x.err)
		}
	}
}

func TestClientRequestCancelation(t *testing.T) {
	tokenCh := make(chan struct{})
	m := MetadataServer{
		T: t,
		Token: func(exit <-chan struct{}) (_, _ string, _ time.Time) {
			close(tokenCh)
			<-exit
			return
		},
	}

	c := Client{
		Dial: m.DialFunc,
	}

	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan tokenAndError)
	go func() { ch <- getToken(ctx, &c) }()
	<-tokenCh
	cancel()

	m.Close()
	if n := m.ResCount(); n != 0 {
		t.Errorf("unexpected number of responses: %d; want %d", n, 0)
	}
}

func TestClientRequestChaining(t *testing.T) {
	var (
		wantToken = make(chan struct{})
		sendToken = make(chan struct{})
	)
	m := MetadataServer{
		T: t,
		Token: func(exit <-chan struct{}) (code, token string, expires time.Time) {
			close(wantToken)
			<-sendToken
			return codeSuccess, "foo", timeutil.Now().Add(time.Hour)
		},
	}
	defer m.Close()

	c := Client{
		Dial: m.DialFunc,
	}

	ch := make(chan tokenAndError, 2)

	ctx1, cancelFirst := context.WithCancel(context.Background())
	go func() { ch <- getToken(ctx1, &c) }()

	ctx2, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	go func() { ch <- getToken(ctx2, &c) }()

	<-wantToken

	cancelFirst()
	if x1 := <-ch; x1.err != context.Canceled {
		t.Errorf("unexpected error: %v", x1.err)
	}

	close(sendToken)
	if x2 := <-ch; x2.err != nil {
		t.Fatalf("unexpected error: %v", x2.err)
	}
}

type tokenAndError struct {
	token string
	err   error
}

func getToken(ctx context.Context, c *Client) tokenAndError {
	token, err := c.Token(ctx)
	return tokenAndError{token, err}
}

func SleepyWriter(w io.Writer, delay time.Duration) io.Writer {
	return writerFunc(func(p []byte) (n int, err error) {
		for _, b := range p {
			_, err = w.Write([]byte{b})
			if err != nil {
				return
			}
			n++
			time.Sleep(delay)
		}
		return
	})
}

func SleepyReader(r io.Reader, delay time.Duration) io.Reader {
	return readerFunc(func(p []byte) (int, error) {
		_, err := r.Read(p[:1])
		if err != nil {
			return 0, err
		}
		time.Sleep(delay)
		return 1, nil
	})
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) {
	return f(p)
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) {
	return f(p)
}
