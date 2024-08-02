package httpyav

import (
	"crypto/rsa"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/user"

	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/go-resty/resty/v2"
	"golang.org/x/crypto/ssh/agent"
)

func getUser() (string, error) {
	login := os.Getenv("SUDO_USER")
	if login == "" {
		u, err := user.Current()
		if err != nil {
			return "", fmt.Errorf("user lookup error: %w", err)
		}
		login = u.Username
	}

	return login, nil
}

type ClientOpt func(*Client) error

// WithOAuthToken sets global authorization token to any client request
func WithOAuthToken(token string) ClientOpt {
	return func(c *Client) error {
		c.httpc.SetHeader("Authorization", token)
		return nil
	}
}

// WithTVMTickets sets global user and service TVM tickets to any client request
func WithTVMTickets(serviceTicket, userTicket string) ClientOpt {
	return func(c *Client) error {
		c.httpc.SetHeader("X-Ya-Service-Ticket", serviceTicket)
		c.httpc.SetHeader("X-Ya-User-Ticket", userTicket)
		return nil
	}
}

// WithRSAKey sets global RSA based authorization to any client request
func WithRSAKey(login string, key *rsa.PrivateKey) ClientOpt {
	return func(c *Client) error {
		if key == nil {
			return errors.New("private key cannot be nil")
		}
		var err error
		if login == "" {
			login, err = getUser()
			if err != nil {
				return fmt.Errorf("unable to get current user %w", err)
			}
			_, _ = fmt.Fprintf(os.Stderr, "WARNING! RSA private key passed without explicit passing rsa_login. Your system login '%s' will be used for request signing", login)
		}

		c.httpc.SetPreRequestHook(func(client *resty.Client, request *http.Request) error {
			return rsaKeySignRequest(request, login, key)
		})

		return nil
	}
}

// WithAgentKey sets global SSH-Agent based authorization to any client request
func WithAgentKey(login string) ClientOpt {
	socketPath := os.Getenv("SSH_AUTH_SOCK")
	return WithAgentKeySocketPath(login, socketPath)

}

// WithAgentKeySocketPath sets global SSH-Agent based authorization to any client request with socket path argument
func WithAgentKeySocketPath(login, socketPath string) ClientOpt {
	return func(c *Client) error {
		conn, err := net.Dial("unix", socketPath)
		if err != nil {
			return fmt.Errorf("failed to open SSH_AUTH_SOCK: %w", err)
		}
		agentClient := agent.NewClient(conn)
		signers, err := agentClient.Signers()
		if err != nil {
			return fmt.Errorf("unable to get SSH agent signers: %w", err)
		}
		if len(signers) == 0 {
			return errors.New("ssh agent returned empty key list")
		}

		if login == "" {
			login, err = getUser()
			if err != nil {
				return fmt.Errorf("unable to get current user %w", err)
			}
			_, _ = fmt.Fprintf(os.Stderr, "WARNING! RSA private key passed without explicit passing rsa_login. Your system login '%s' will be used for request signing", login)
		}

		c.httpc.SetPreRequestHook(func(client *resty.Client, request *http.Request) error {
			// TODO: pick next key in case of error
			return rsaSignRequest(request, login, signers[0])
		})

		return nil
	}
}

// WithHTTPHost rewrites default HTTP host in client.
func WithHTTPHost(host string) ClientOpt {
	return func(c *Client) error {
		c.httpc.SetBaseURL(host)
		return nil
	}
}

// WithDebug enables debug resty output
func WithDebug() ClientOpt {
	return func(c *Client) error {
		c.httpc.SetDebug(true)
		return nil
	}
}

// WithLogger redirects default resty debug output to custom logger.
func WithLogger(l log.Logger) ClientOpt {
	return func(c *Client) error {
		c.httpc.SetLogger(l)
		return nil
	}
}
