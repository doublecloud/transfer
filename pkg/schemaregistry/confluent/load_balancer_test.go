package confluent

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRoundRobinLoadBalancer(t *testing.T) {
	t.Parallel()

	type message struct {
		serverID int
		body     string
	}
	var gotRequests []message
	var sentRequests []message

	newHandlerFuncWithID := func(serverId int) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			bodyBytes, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			gotRequests = append(gotRequests, message{serverID: serverId, body: string(bodyBytes)})
		}
	}

	var servers []*httptest.Server
	for i := 0; i < 4; i++ {
		servers = append(servers, httptest.NewServer(newHandlerFuncWithID(i)))
	}

	var urls []*url.URL
	for _, server := range servers {
		parsedURL, err := url.Parse(server.URL)
		require.NoError(t, err)
		urls = append(urls, parsedURL)
	}

	// stop one server
	servers[0].Close()

	balancedClient := newBalancedClient(newRoundRobinLoadBalancer(urls), http.DefaultClient)
	for i := 0; i < 5; i++ {
		for j := 0; j < len(servers); j++ {
			body := fmt.Sprintf("iteration: %d, subiteration %d", i, j)
			req, err := http.NewRequest(http.MethodPost, "http://it-will-be-replaced-anyway.com", strings.NewReader(body))
			require.NoError(t, err)
			resp, err := balancedClient.Do(req)
			if err == nil {
				defer resp.Body.Close()
			}
			if j == 0 {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				sentRequests = append(sentRequests, message{serverID: j, body: body})
			}
		}
	}
	require.Equal(t, sentRequests, gotRequests)
}
