package credentials

import (
	"context"
)

// Credentials is an interface of credentials provider
type Credentials interface {
	// Token must return actual token or error
	Token(context.Context) (string, error)
}
