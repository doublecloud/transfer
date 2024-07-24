package test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/genproto/Ydb_PersQueue_V0"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/session"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/test/Ydb_PersQueue_V0_Mock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestWriteSingleMessage(t *testing.T) {
	env, stop := newEnv(t)
	defer stop()

	p := persqueue.NewWriter(env.ProducerOptions())

	_, err := p.Init(env.ctx)
	require.NoError(t, err)

	defer func() { _ = p.Close() }()

	require.NoError(t, p.Write(&persqueue.WriteMessage{Data: []byte("foo")}))
	require.NoError(t, p.Close())
}

func TestWriteSingleMessageWithClusterDiscovery(t *testing.T) {
	env, stop := newEnv(t)
	defer stop()

	options := env.ProducerOptions()
	options.DiscoverCluster = true
	p := persqueue.NewWriter(options)

	_, err := p.Init(env.ctx)
	require.NoError(t, err)

	defer func() { _ = p.Close() }()

	require.NoError(t, p.Write(&persqueue.WriteMessage{Data: []byte("foo")}))
	require.NoError(t, p.Close())
}

func TestProducerCancel(t *testing.T) {
	env, stop := newEnv(t)
	defer stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := persqueue.NewWriter(env.ProducerOptions())

	_, err := p.Init(ctx)
	require.NoError(t, err)

	require.NoError(t, p.Write(&persqueue.WriteMessage{Data: []byte("foo")}))

	cancel()

	for i := 0; i < 1000; i++ {
		if err := p.Write(&persqueue.WriteMessage{Data: []byte("bar")}); err != nil {
			return
		}

		time.Sleep(time.Millisecond)
	}

	t.Fatalf("Producer is not terminated after context cancel")
}

func TestPassiveProducerCancel(t *testing.T) {
	env, stop := newEnv(t)
	defer stop()

	p := persqueue.NewWriter(env.ProducerOptions())

	ctx, cancel := context.WithCancel(context.Background())

	_, err := p.Init(ctx)
	require.NoError(t, err)

	cancel()
}

func TestProducerReconnect(t *testing.T) {
	env, stop := newEnv(t)
	defer stop()

	p := persqueue.NewWriter(env.ProducerOptions())

	started, reset := withFailingConnections(t)
	defer reset()

	_, err := p.Init(env.ctx)
	require.NoError(t, err)
	close(started)

	go persqueue.ReceiveIssues(p)

	for i := 0; i < 1000; i++ {
		require.NoError(t, p.Write(&persqueue.WriteMessage{Data: []byte("bar")}))
		time.Sleep(time.Millisecond)
	}

	require.NoError(t, p.Close())
	time.Sleep(time.Millisecond * 1000)
}

func TestMemoryLimit(t *testing.T) {
	env, stop := newEnv(t)
	defer stop()

	opts := env.ProducerOptions()
	opts.MaxMemory = 1
	p := persqueue.NewWriter(opts)

	_, err := p.Init(env.ctx)
	require.NoError(t, err)
	go persqueue.ReceiveIssues(p)

	for i := 0; i < 10; i++ {
		require.NoError(t, p.Write(&persqueue.WriteMessage{Data: []byte("bar")}))
	}
	require.NoError(t, p.Close())
}

func TestManualSeqNoAssignment(t *testing.T) {
	env, stop := newEnv(t)
	defer stop()

	p := persqueue.NewWriter(env.ProducerOptions())

	started, reset := withFailingConnections(t)
	defer reset()

	_, err := p.Init(env.ctx)
	require.NoError(t, err)
	close(started)
	go func() {
		defer require.NoError(t, p.Close())
		for rsp := range p.C() {
			switch m := rsp.(type) {
			case *persqueue.Ack:
				require.Equal(t, uint64(321), m.SeqNo)
			case *persqueue.Issue:
				t.Error("Must be ack")
				t.Fail()
			}
			return
		}
	}()
	e1 := &persqueue.WriteMessage{Data: []byte("bar")}
	e2 := &persqueue.WriteMessage{Data: []byte("bar")}

	require.NoError(t, p.Write(e1.WithSeqNo(uint64(321))))
	require.NoError(t, p.Write(e2.WithSeqNo(uint64(321))))

	<-p.Closed()
}

func TestConnectionGoroutinesLeak(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	env, stop := newEnv(t)
	defer stop()

	p := persqueue.NewWriter(env.ProducerOptions())
	mockSession := Ydb_PersQueue_V0_Mock.NewMockWriteSessionClent(ctrl)

	// Init request and response
	mockSession.EXPECT().SendMsg(gomock.Any()).Return(nil)
	mockSession.EXPECT().RecvMsg(gomock.Any()).Return(nil).
		Do(func(wrapper *Ydb_PersQueue_V0.WriteResponse) {
			wrapper.Response = &Ydb_PersQueue_V0.WriteResponse_Init_{
				Init: &Ydb_PersQueue_V0.WriteResponse_Init{},
			}
		})

	// Write request and error instead of ack
	mockSession.EXPECT().SendMsg(gomock.Any()).Return(nil)
	mockSession.EXPECT().RecvMsg(gomock.Any()).Return(errors.New("recv error"))

	// Init writer one more time, error in sending init request
	mockSession.EXPECT().SendMsg(gomock.Any()).Return(errors.New("send error"))

	// Init request and response
	mockSession.EXPECT().SendMsg(gomock.Any()).Return(nil)
	mockSession.EXPECT().RecvMsg(gomock.Any()).Return(nil).
		Do(func(wrapper *Ydb_PersQueue_V0.WriteResponse) {
			wrapper.Response = &Ydb_PersQueue_V0.WriteResponse_Init_{
				Init: &Ydb_PersQueue_V0.WriteResponse_Init{},
			}
		})

	// Write request, ack response and lock before close
	mockSession.EXPECT().SendMsg(gomock.Any()).Return(nil)
	mockSession.EXPECT().RecvMsg(gomock.Any()).Return(nil).
		Do(func(wrapper *Ydb_PersQueue_V0.WriteResponse) {
			wrapper.Response = &Ydb_PersQueue_V0.WriteResponse_AckBatch_{
				AckBatch: &Ydb_PersQueue_V0.WriteResponse_AckBatch{
					Ack: []*Ydb_PersQueue_V0.WriteResponse_Ack{
						{SeqNo: 1},
					},
				},
			}
		})
	mockSession.EXPECT().RecvMsg(gomock.Any()).Return(io.EOF).
		Do(func(wrapper *Ydb_PersQueue_V0.WriteResponse) {
			time.Sleep(100 * time.Millisecond)
		})

	mockSession.EXPECT().Context().Return(context.Background()).AnyTimes()

	mockClient := Ydb_PersQueue_V0_Mock.NewMockPersQueueServiceClient(ctrl)
	mockClient.EXPECT().WriteSession(gomock.Any()).Return(mockSession, nil).Times(3)

	persqueue.InternalSessionHook = func(session *session.Session) {
		session.Client = mockClient
	}

	_, err := p.Init(env.ctx)
	require.NoError(t, err)

	defer func() { _ = p.Close() }()

	require.NoError(t, p.Write(&persqueue.WriteMessage{Data: []byte("foo")}))
	require.NoError(t, p.Close())
}
