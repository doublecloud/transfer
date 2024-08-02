package httpyav

import (
	"bytes"
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	stdlog "log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/library/go/core/log/zap"
	"github.com/doublecloud/tross/library/go/test/output"
	"github.com/go-resty/resty/v2"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ssh_agent "golang.org/x/crypto/ssh/agent"
)

func TestWithOAuthToken(t *testing.T) {
	testCases := []struct {
		name         string
		token        string
		expectClient *Client
	}{
		{
			"nonempty_token",
			"looken-tooken",
			&Client{
				httpc: resty.New().
					SetHeader("Authorization", "looken-tooken"),
			},
		},
	}

	cmpOpts := cmp.Options{
		cmp.AllowUnexported(Client{}),
		cmpopts.IgnoreUnexported(stdlog.Logger{}, resty.Client{}),
		cmpopts.IgnoreFields(resty.Client{}, "JSONMarshal", "JSONUnmarshal", "XMLMarshal", "XMLUnmarshal"),
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Client{httpc: resty.New()}
			opt := WithOAuthToken(tc.token)
			_ = opt(c)

			assert.True(t, cmp.Equal(tc.expectClient, c, cmpOpts...), cmp.Diff(tc.expectClient, c, cmpOpts...))
		})
	}
}

func TestWithTVMTickets(t *testing.T) {
	testCases := []struct {
		name          string
		userTicket    string
		serviceTicket string
		expectClient  *Client
	}{
		{
			"nonempty_tickets",
			"shimba",
			"boomba",
			&Client{
				httpc: resty.New().
					SetHeader("X-Ya-Service-Ticket", "boomba").
					SetHeader("X-Ya-User-Ticket", "shimba"),
			},
		},
	}

	cmpOpts := cmp.Options{
		cmp.AllowUnexported(Client{}),
		cmpopts.IgnoreUnexported(stdlog.Logger{}, resty.Client{}),
		cmpopts.IgnoreFields(resty.Client{}, "JSONMarshal", "JSONUnmarshal", "XMLMarshal", "XMLUnmarshal"),
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Client{httpc: resty.New()}
			opt := WithTVMTickets(tc.serviceTicket, tc.userTicket)
			_ = opt(c)

			assert.True(t, cmp.Equal(tc.expectClient, c, cmpOpts...), cmp.Diff(tc.expectClient, c, cmpOpts...))
		})
	}
}

func TestWithHTTPHost(t *testing.T) {
	testCases := []struct {
		name        string
		client      *Client
		value       string
		expectValue string
	}{
		{
			"empty_url",
			&Client{httpc: resty.New()},
			"",
			DefaultHTTPHost,
		},
		{
			"nonempty_url",
			&Client{httpc: resty.New()},
			"https://vault-api-mock.passport.yandex.net",
			"https://vault-api-mock.passport.yandex.net",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opt := WithHTTPHost(tc.value)
			_ = opt(tc.client)
			assert.Equal(t, tc.value, tc.client.httpc.BaseURL)
		})
	}
}

func TestWithLogger(t *testing.T) {
	err := output.Replace(output.Stdout)
	require.NoError(t, err)

	defer output.Reset(output.Stdout)

	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
	}))
	defer srv.Close()

	c := &Client{
		httpc: resty.New().
			SetBaseURL(srv.URL).
			SetDebug(true),
	}

	l, err := zap.NewQloudLogger(log.DebugLevel)
	assert.NoError(t, err)

	opt := WithLogger(l)
	err = opt(c)
	assert.NoError(t, err)

	ctx := context.Background()
	_, _ = c.GetVersion(ctx, "ololo")

	result := output.Catch(output.Stdout)
	assert.NotEmpty(t, result)
}

func TestWithDebug(t *testing.T) {
	testCases := []struct {
		name         string
		expectClient *Client
	}{
		{
			"with_debug",
			&Client{
				httpc: resty.New().
					SetDebug(true),
			},
		},
	}

	cmpOpts := cmp.Options{
		cmp.AllowUnexported(Client{}),
		cmpopts.IgnoreUnexported(stdlog.Logger{}, resty.Client{}),
		cmpopts.IgnoreFields(resty.Client{}, "JSONMarshal", "JSONUnmarshal", "XMLMarshal", "XMLUnmarshal"),
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Client{httpc: resty.New()}
			opt := WithDebug()
			_ = opt(c)

			assert.True(t, cmp.Equal(tc.expectClient, c, cmpOpts...), cmp.Diff(tc.expectClient, c, cmpOpts...))
		})
	}
}

func TestWithRSAKey(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer ts.Close()

	privPem, _ := pem.Decode([]byte(testPrivateRSAKey))
	pk, err := x509.ParsePKCS1PrivateKey(privPem.Bytes)
	assert.NoError(t, err)
	assert.IsType(t, &rsa.PrivateKey{}, pk)

	c := &Client{httpc: resty.New().SetBaseURL(ts.URL)}

	// test empty private key
	opt := WithRSAKey("shimba", nil)
	err = opt(c)
	assert.Error(t, err)
	assert.EqualError(t, errors.New("private key cannot be nil"), err.Error())

	// test non-empty private key
	opt = WithRSAKey("boomba", pk)
	err = opt(c)
	assert.NoError(t, err)

	req := c.httpc.R()
	_, _ = req.Get("/")

	assert.Contains(t, req.Header, "X-Ya-Rsa-Login")
	assert.Contains(t, req.Header, "X-Ya-Rsa-Signature")
	assert.Contains(t, req.Header, "X-Ya-Rsa-Timestamp")
}

// based on x/crypto/ssh/agent/client_test.go
// startOpenSSHAgent executes ssh-agent, and returns an Agent interface to it.
func startOpenSSHAgent(t *testing.T) (client ssh_agent.ExtendedAgent, socket string, cleanup func()) {
	bin, err := exec.LookPath("ssh-agent")
	if err != nil {
		t.Skip("could not find ssh-agent")
	}

	cmd := exec.Command(bin, "-s")
	cmd.Env = []string{} // Do not let the user's environment influence ssh-agent behavior.
	cmd.Stderr = new(bytes.Buffer)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("%s failed: %v\n%s", strings.Join(cmd.Args, " "), err, cmd.Stderr)
	}

	// Output looks like:
	//
	//	SSH_AUTH_SOCK=/tmp/ssh-P65gpcqArqvH/agent.15541; export SSH_AUTH_SOCK;
	//	SSH_AGENT_PID=15542; export SSH_AGENT_PID;
	//	echo Agent pid 15542;

	fields := bytes.Split(out, []byte(";"))
	line := bytes.SplitN(fields[0], []byte("="), 2)
	line[0] = bytes.TrimLeft(line[0], "\n")
	if string(line[0]) != "SSH_AUTH_SOCK" {
		t.Fatalf("could not find key SSH_AUTH_SOCK in %q", fields[0])
	}
	socket = string(line[1])

	line = bytes.SplitN(fields[2], []byte("="), 2)
	line[0] = bytes.TrimLeft(line[0], "\n")
	if string(line[0]) != "SSH_AGENT_PID" {
		t.Fatalf("could not find key SSH_AGENT_PID in %q", fields[2])
	}
	pidStr := line[1]
	pid, err := strconv.Atoi(string(pidStr))
	if err != nil {
		t.Fatalf("Atoi(%q): %v", pidStr, err)
	}

	conn, err := net.Dial("unix", string(socket))
	if err != nil {
		t.Fatalf("net.Dial: %v", err)
	}

	ac := ssh_agent.NewClient(conn)
	return ac, socket, func() {
		proc, _ := os.FindProcess(pid)
		if proc != nil {
			_ = proc.Kill()
		}
		_ = conn.Close()
		_ = os.RemoveAll(filepath.Dir(socket))
	}
}

func TestWithAgentKey(t *testing.T) {
	timeNow = func() time.Time {
		return time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	login := "shimba"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer ts.Close()
	ts.URL = "http://127.0.0.1:43812" //url is a part of signed data

	c := &Client{httpc: resty.New().SetBaseURL(ts.URL)}

	agent, socketPath, cleanup := startOpenSSHAgent(t)
	defer cleanup()

	// The agent should start up empty.
	keys, err := agent.List()
	assert.NoError(t, err)
	assert.Len(t, keys, 0, "expected no keys")

	// Test empty private key
	opt := WithAgentKeySocketPath(login, socketPath)
	err = opt(c)
	assert.Error(t, err)
	assert.EqualError(t, errors.New("ssh agent returned empty key list"), err.Error())

	// Attempt to insert the key
	privPem, _ := pem.Decode([]byte(testPrivateRSAKey))
	pk, err := x509.ParsePKCS1PrivateKey(privPem.Bytes)
	assert.NoError(t, err)
	assert.IsType(t, &rsa.PrivateKey{}, pk)

	err = agent.Add(ssh_agent.AddedKey{PrivateKey: pk, Comment: "comment", LifetimeSecs: 0})
	assert.NoError(t, err, "insert error")

	time.Sleep(1100 * time.Millisecond)
	keys, err = agent.List()
	assert.NoError(t, err)
	assert.Len(t, keys, 1, "expected 1 key")

	// Do query with the key
	err = opt(c)
	assert.NoError(t, err)

	req := c.httpc.R()
	_, _ = req.Get("/")
	expectedSign := "AAAAB3NzaC1yc2EAAAEAbNBvldp8VA0R5OpbhN4jBGoo8cxe_6HjGhYW06WnxI34oZLVXdFzj52MZWq-OLFiU9FGb3bRLX4" +
		"yejt1rivtlzQGJ90sTc2cb9UNVZUoA8QYALhua-5dY61IgJVqOK46XYumVIMEa6pv4vfsdFppAKOFwvmt-HKZdl1CpP0BDr7kUF9UFZMgtD" +
		"gb3SBQ4nQbVvJh67rPhvaiBDVI7klCDYUYD_RZ2TBqTgc27jrZk8qzMpRpWDYiOGQ5AO6F727nggdMV1qo_Bbk8EQjOBBljdV-7Ufn7Iasg" +
		"kl7rHkyoxLfZQGjKcRgPrFoHtnTm9TfNK9RPKuEMh51noLlfkkp0Q=="
	assert.Contains(t, req.Header, "X-Ya-Rsa-Login")
	assert.Equal(t, req.Header["X-Ya-Rsa-Login"], []string{login})
	assert.Contains(t, req.Header, "X-Ya-Rsa-Signature")
	assert.Equal(t, req.Header["X-Ya-Rsa-Signature"], []string{expectedSign})
	assert.Contains(t, req.Header, "X-Ya-Rsa-Timestamp")
	assert.Equal(t, req.Header["X-Ya-Rsa-Timestamp"], []string{"946684800"})
}
