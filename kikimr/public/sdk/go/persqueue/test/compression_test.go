package test

import (
	"testing"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue"
	"github.com/stretchr/testify/require"
)

func readMessages(c persqueue.Reader, n int) (msgs []persqueue.ReadMessage) {
	for m := range c.C() {
		switch e := m.(type) {
		case *persqueue.Data:
			for _, b := range e.Batches() {
				msgs = append(msgs, b.Messages...)
			}
		}

		if len(msgs) >= n {
			break
		}
	}

	c.Shutdown()
	for range c.C() {
	}

	return
}

func TestMessageCompression(t *testing.T) {
	env, stop := newEnv(t)
	defer stop()

	ping := []byte("ping")

	opts := env.ProducerOptions()
	for _, codec := range []persqueue.Codec{persqueue.Raw, persqueue.Gzip} {
		opts.Codec = codec

		p := persqueue.NewWriter(opts)
		_, err := p.Init(env.ctx)
		require.NoError(t, err)

		require.NoError(t, p.Write(&persqueue.WriteMessage{Data: ping}))
		require.NoError(t, p.Close())
	}

	for _, unpack := range []bool{false, true} {
		opts := env.ConsumerOptions()
		opts.DecompressionDisabled = !unpack

		c := persqueue.NewReader(opts)
		_, err := c.Start(env.ctx)
		require.NoError(t, err)

		msgs := readMessages(c, 2)
		require.Equal(t, len(msgs), 2)

		if !unpack {
			require.Equal(t, persqueue.Raw, msgs[0].Codec)
			require.Equal(t, ping, msgs[0].Data)

			require.Equal(t, persqueue.Gzip, msgs[1].Codec)
			require.NotEqual(t, ping, msgs[1].Data)
		} else {
			require.Equal(t, persqueue.Raw, msgs[0].Codec)
			require.Equal(t, ping, msgs[0].Data)

			require.Equal(t, persqueue.Raw, msgs[1].Codec)
			require.Equal(t, ping, msgs[1].Data)
		}
	}
}
