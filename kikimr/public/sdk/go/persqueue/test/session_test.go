package test

import (
	"context"
	"testing"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/log/corelogadapter"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/recipe"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/session"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestSession(t *testing.T) {
	defer goleak.VerifyNone(t)

	env := recipe.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	ss, err := session.Dial(ctx, session.Options{
		Endpoint:    env.Endpoint,
		Port:        env.Port,
		Credentials: env.Creds,
		Logger:      corelogadapter.NewTest(t),
	})

	require.NoError(t, err)

	defer func() { _ = ss.Close() }()
}

func TestSessionWithClusterDiscovery(t *testing.T) {
	defer goleak.VerifyNone(t)

	env := recipe.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	options := env.ProducerOptions()

	ss, err := session.Dial(ctx, session.Options{
		Endpoint:        env.Endpoint,
		Port:            env.Port,
		Credentials:     env.Creds,
		Logger:          corelogadapter.NewTest(t),
		DiscoverCluster: true,
		Topic:           options.Topic,
		SourceID:        options.SourceID,
		PartitionGroup:  options.PartitionGroup,
	})

	require.NoError(t, err)

	defer func() { _ = ss.Close() }()
}
