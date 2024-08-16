package table

import (
	"context"
	"fmt"
	"math/rand"
	"path"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/testutil"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/timeutil"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/timeutil/timetest"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
)

func TestSessionPoolCreateAbnormalResult(t *testing.T) {
	p := &SessionPool{
		limit: 1000,
		index: make(map[*Session]sessionInfo),
		Builder: &StubBuilder{
			T:     t,
			Limit: 1000,
			Handler: methodHandlers{
				testutil.TableDeleteSession: okHandler,
			},
		},
	}
	defer func() {
		_ = p.Close(context.Background())
	}()
	for i := 0; i < 10000; i++ {
		t.Run("", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(
				context.Background(),
				time.Duration(rand.Float32()+float32(time.Second)),
			)
			defer cancel()
			s, err := p.createSession(
				withCreateSessionTrace(
					ctx,
					createSessionTrace{
						OnStartSelect: func() {
							runtime.Gosched() // for force run create session goroutine
						},
						OnReadResult: func(r createSessionResult) {
							if r.s == nil && r.err == nil {
								t.Fatalf("unexpected result: <%v, %vz>", r.s, r.err)
							}
						},
						OnPutSession: func(s *Session, err error) {
							fmt.Println("put session", s, err)
						},
					},
				),
			)
			if s == nil && err == nil {
				t.Fatalf("unexpected result: <%v, %vz>", s, err)
			}
		})
	}
}

func TestSessionPoolKeeperWake(t *testing.T) {
	timer := timetest.StubSingleTimer(t)
	defer timer.Cleanup()

	shiftTime, cleanupNow := timeutil.StubTestHookTimeNow(time.Unix(0, 0))
	defer cleanupNow()

	var (
		keepalive = make(chan struct{})
	)
	p := &SessionPool{
		SizeLimit:     1,
		IdleThreshold: time.Hour,
		Builder: &StubBuilder{
			T:     t,
			Limit: 1,
			Handler: methodHandlers{
				testutil.TableKeepAlive: func(req, res interface{}) error {
					keepalive <- struct{}{}
					return nil
				},
				testutil.TableDeleteSession: okHandler,
			},
		},
	}
	defer func() {
		_ = p.Close(context.Background())
	}()

	s := mustGetSession(t, p)

	// Wait for keeper goroutine become initialized.
	<-timer.Created

	// Trigger keepalive timer event.
	// NOTE: code below would blocked if KeepAlive() call will happen for this
	// event.
	done := p.touchCond()
	shiftTime(p.IdleThreshold)
	timer.C <- timeutil.Now()
	<-done

	// Return session to wake up the keeper.
	mustPutSession(t, p, s)
	<-timer.Reset

	// Trigger timer event and expect keepalive to be prepared.
	shiftTime(p.IdleThreshold)
	timer.C <- timeutil.Now()
	<-keepalive
	<-timer.Reset
}

func TestSessionPoolCloseWhenWaiting(t *testing.T) {
	for _, test := range []struct {
		name string
		racy bool
	}{
		{
			name: "normal",
			racy: false,
		},
		{
			name: "racy",
			racy: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := &SessionPool{
				SizeLimit: 1,
				Builder: &StubBuilder{
					T:     t,
					Limit: 1,
				},
			}
			defer func() {
				_ = p.Close(context.Background())
			}()

			mustGetSession(t, p)

			var (
				get  = make(chan struct{})
				wait = make(chan struct{})
				got  = make(chan error)
			)
			go func() {
				_, err := p.Get(WithSessionPoolTrace(context.Background(), SessionPoolTrace{
					OnGet: func(SessionPoolGetStartInfo) func(SessionPoolGetDoneInfo) {
						get <- struct{}{}
						return nil
					},
					OnWait: func(SessionPoolWaitStartInfo) func(SessionPoolWaitDoneInfo) {
						wait <- struct{}{}
						return nil
					},
				}))
				got <- err
			}()

			regwait := whenWantWaitCh(p)
			<-get     // Await for getter blocked on awaiting session.
			<-regwait // Let the getter register itself in the wait queue.

			if test.racy {
				// We testing the case, when session consumer registered
				// himself in the wait queue, but not ready to receive the
				// session when session arrives (that is, stuck between
				// pushing channel in the list and reading from the channel).
				_ = p.Close(context.Background())
				<-wait
			} else {
				// We testing the normal case, when session consumer registered
				// himself in the wait queue and successfully blocked on
				// reading from signaling channel.
				<-wait
				// Let the waiting goroutine to block on reading from channel.
				runtime.Gosched()
				_ = p.Close(context.Background())
			}

			const timeout = time.Second
			select {
			case err := <-got:
				if err != ErrSessionPoolClosed {
					t.Fatalf(
						"unexpected error: %v; want %v",
						err, ErrSessionPoolClosed,
					)
				}
			case <-time.After(timeout):
				t.Fatalf("no result after %s", timeout)
			}
		})
	}

}

func TestSessionPoolClose(t *testing.T) {
	p := &SessionPool{
		SizeLimit:     3,
		IdleThreshold: time.Hour,
		Builder: &StubBuilder{
			T:     t,
			Limit: 3,
		},
	}
	defer func() {
		_ = p.Close(context.Background())
	}()

	s1 := mustGetSession(t, p)
	s2 := mustGetSession(t, p)
	s3 := mustGetSession(t, p)

	var (
		closed1 bool
		closed2 bool
		closed3 bool
	)
	s1.OnClose(func() { closed1 = true })
	s2.OnClose(func() { closed2 = true })
	s3.OnClose(func() { closed3 = true })

	mustPutSession(t, p, s1)

	mustPutSession(t, p, s2)

	mustClose(t, p)

	if !closed1 {
		t.Fatalf("session was not closed")
	}
	if !closed2 {
		t.Fatalf("session was not closed")
	}
	if closed3 {
		t.Fatalf("unexpected session close")
	}

	wg := sync.WaitGroup{}
	if err := p.Put(
		WithSessionPoolTrace(
			context.Background(),
			SessionPoolTrace{
				OnPut: func(info SessionPoolPutStartInfo) func(SessionPoolPutDoneInfo) {
					wg.Add(1)
					return func(info SessionPoolPutDoneInfo) {
						wg.Done()
					}
				},
				OnCloseSession: func(info SessionPoolCloseSessionStartInfo) func(doneInfo SessionPoolCloseSessionDoneInfo) {
					wg.Add(1)
					return func(info SessionPoolCloseSessionDoneInfo) {
						wg.Done()
					}
				},
			},
		),
		s3,
	); err != ErrSessionPoolClosed {
		t.Errorf(
			"unexpected Put() error: %v; want %v",
			err, ErrSessionPoolClosed,
		)
	}
	wg.Wait()

	if !closed3 {
		t.Fatalf("session was not closed")
	}
}

func TestSessionPoolDeleteReleaseWait(t *testing.T) {
	for _, test := range []struct {
		name string
		racy bool
	}{
		{
			name: "normal",
			racy: false,
		},
		{
			name: "racy",
			racy: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := &SessionPool{
				SizeLimit: 1,
				Builder: &StubBuilder{
					T:     t,
					Limit: 2,
				},
			}
			defer func() {
				_ = p.Close(context.Background())
			}()
			s := mustGetSession(t, p)
			var (
				get  = make(chan struct{})
				wait = make(chan struct{})
				got  = make(chan struct{})
			)
			go func() {
				defer func() {
					close(got)
				}()
				_, _ = p.Get(WithSessionPoolTrace(context.Background(), SessionPoolTrace{
					OnGet: func(SessionPoolGetStartInfo) func(SessionPoolGetDoneInfo) {
						get <- struct{}{}
						return nil
					},
					OnWait: func(SessionPoolWaitStartInfo) func(SessionPoolWaitDoneInfo) {
						wait <- struct{}{}
						return nil
					},
				}))
			}()

			regwait := whenWantWaitCh(p)
			<-get     // Await for getter blocked on awaiting session.
			<-regwait // Let the getter register itself in the wait queue.

			if test.racy {
				// We testing the case, when session consumer registered
				// himself in the wait queue, but not ready to receive the
				// session when session arrives (that is, stucked between
				// pushing channel in the list and reading from the channel).
				_ = s.Close(context.Background())
				<-wait
			} else {
				// We testing the normal case, when session consumer registered
				// himself in the wait queue and successfully blocked on
				// reading from signaling channel.
				<-wait
				// Let the waiting goroutine to block on reading from channel.
				runtime.Gosched()
				_ = s.Close(context.Background())
			}

			const timeout = time.Second
			select {
			case <-got:
			case <-time.After(timeout):
				t.Fatalf("no get after %s", timeout)
			}
		})
	}
}

func TestSessionPoolRacyGet(t *testing.T) {
	type createReq struct {
		release chan struct{}
		session *Session
	}
	create := make(chan createReq)
	p := &SessionPool{
		SizeLimit:     1,
		IdleThreshold: -1,
		Builder: &StubBuilder{
			Limit: 1,
			OnCreateSession: func(ctx context.Context) (*Session, error) {
				req := createReq{
					release: make(chan struct{}),
					session: simpleSession(),
				}
				create <- req
				<-req.release
				return req.session, nil
			},
		},
	}
	var (
		expSession *Session
		done       = make(chan struct{}, 2)
	)

	var err error
	// nolint:SA2002
	for i := 0; i < 2; i++ {
		go func() {
			defer func() {
				done <- struct{}{}
			}()
			s, e := p.Get(context.Background())
			if e != nil {
				err = e
				return
			}
			if s != expSession {
				err = fmt.Errorf("unexpected session: %v; want %v", s, expSession)
				return
			}
			mustPutSession(t, p, s)
		}()
	}
	if err != nil {
		t.Fatal(err)
	}
	// Wait for both requests are created.
	r1 := <-create
	select {
	case <-create:
		t.Fatalf("session 2 on race created while pool size 1")
	case <-time.After(time.Millisecond * 5):
		// ok
	}

	// Release the first create session request.
	// Created session must be stored in the pool.
	expSession = r1.session
	expSession.OnClose(func() {
		t.Fatalf("unexpected first session close")
	})
	close(r1.release)

	// Wait for r1's session will be stored in the pool.
	<-done

	// Ensure that session is in the pool.
	s := mustGetSession(t, p)
	mustPutSession(t, p, s)
}

func TestSessionPoolPutInFull(t *testing.T) {
	p := &SessionPool{
		SizeLimit:     1,
		IdleThreshold: -1,
		Builder: &StubBuilder{
			T:     t,
			Limit: 1,
		},
	}

	s := mustGetSession(t, p)
	if err := p.Put(context.Background(), s); err != nil {
		t.Fatalf("unexpected error on put session into non-full pool: %v, wand: %v", err, nil)
	}

	if err := p.Put(context.Background(), simpleSession()); err != ErrSessionPoolOverflow {
		t.Fatalf("unexpected error on put session into full pool: %v, wand: %v", err, ErrSessionPoolOverflow)
	}
}

func TestSessionPoolSizeLimitOverflow(t *testing.T) {
	type sessionAndError struct {
		session *Session
		err     error
	}
	for _, test := range []struct {
		name string
		racy bool
	}{
		{
			name: "normal",
			racy: false,
		},
		{
			name: "racy",
			racy: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := &SessionPool{
				SizeLimit: 1,
				Builder: &StubBuilder{
					T:     t,
					Limit: 1,
				},
			}
			defer func() {
				_ = p.Close(context.Background())
			}()
			s := mustGetSession(t, p)
			{
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if _, err := p.Get(ctx); err != context.Canceled {
					t.Fatalf(
						"unexpected error: %v; want %v",
						err, context.Canceled,
					)
				}
			}
			var (
				get  = make(chan struct{})
				wait = make(chan struct{})
				got  = make(chan sessionAndError)
			)
			go func() {
				ctx := WithSessionPoolTrace(context.Background(), SessionPoolTrace{
					OnGet: func(SessionPoolGetStartInfo) func(SessionPoolGetDoneInfo) {
						get <- struct{}{}
						return nil
					},
					OnWait: func(SessionPoolWaitStartInfo) func(SessionPoolWaitDoneInfo) {
						wait <- struct{}{}
						return nil
					},
				})
				s, err := p.Get(ctx)
				got <- sessionAndError{s, err}
			}()

			regwait := whenWantWaitCh(p)
			<-get     // Await for getter blocked on awaiting session.
			<-regwait // Let the getter register itself in the wait queue.

			if test.racy {
				// We testing the case, when session consumer registered
				// himself in the wait queue, but not ready to receive the
				// session when session arrives (that is, stucked between
				// pushing channel in the list and reading from the channel).
				_ = p.Put(context.Background(), s)
				<-wait
			} else {
				// We testing the normal case, when session consumer registered
				// himself in the wait queue and successfully blocked on
				// reading from signaling channel.
				<-wait
				// Let the waiting goroutine to block on reading from channel.
				runtime.Gosched()
				_ = p.Put(context.Background(), s)
			}

			const timeout = time.Second
			select {
			case se := <-got:
				if se.err != nil {
					t.Fatal(se.err)
				}
				if se.session != s {
					t.Fatalf("unexpected session")
				}
			case <-time.After(timeout):
				t.Fatalf("no session after %s", timeout)
			}
		})
	}
}

// TestSessionPoolGetDisconnected tests case when session successfully created,
// but after that connection become broken and cant be reestablished.
func TestSessionPoolGetDisconnected(t *testing.T) {
	timer := timetest.StubSingleTimer(t)
	defer timer.Cleanup()

	shiftTime, cleanupNow := timeutil.StubTestHookTimeNow(time.Unix(0, 0))
	defer cleanupNow()

	var (
		release   = make(chan struct{})
		keepalive = make(chan struct{})
	)
	p := &SessionPool{
		SizeLimit:     1,
		IdleThreshold: time.Hour,
		Builder: &StubBuilder{
			T:     t,
			Limit: 1,
			Handler: methodHandlers{
				testutil.TableKeepAlive: func(req, res interface{}) error {
					keepalive <- struct{}{}
					// Here we emulating blocked connection initialization.
					<-release
					return nil
				},
				testutil.TableDeleteSession: okHandler,
			},
		},
	}
	defer func() {
		_ = p.Close(context.Background())
	}()

	touched := p.touchCond()

	s := mustGetSession(t, p)
	mustPutSession(t, p, s)

	<-timer.Created

	// Trigger next KeepAlive iteration.
	shiftTime(p.IdleThreshold)
	timer.C <- timeutil.Now()
	<-keepalive

	// Here we are in touching state. That is, there are no session in the pool
	// – it is removed for keepalive operation.
	//
	// We expect that Get() method will fail on ctx cancelation.
	ctx1, cancel := context.WithCancel(context.Background())
	time.AfterFunc(500*time.Millisecond, cancel)

	x, err := p.Get(ctx1)
	if err != context.Canceled {
		t.Fatalf("unexpected error: %v", err)
	}
	if x != nil {
		t.Fatalf("obtained unexpected session")
	}

	// Release session s – connection established.
	release <- struct{}{}
	<-timer.Reset
	<-touched // Wait until first keep alive loop finished.

	mustTakeSession(t, p, s)
	mustPutSession(t, p, s)

	// Trigger next KeepAlive iteration.
	shiftTime(p.IdleThreshold)
	timer.C <- timeutil.Now()
	<-keepalive

	ctx2, cancel := context.WithCancel(context.Background())
	time.AfterFunc(500*time.Millisecond, cancel)

	took, err := p.Take(ctx2, s)
	if err != context.Canceled {
		t.Fatalf("unexpected error: %v", err)
	}
	if took {
		t.Fatalf("unexpected take over session")
	}

	// Release session s to not block on defer p.Close().
	release <- struct{}{}
	<-timer.Reset
}

func TestSessionPoolGetPut(t *testing.T) {
	var (
		created int
		deleted int
	)
	assertCreated := func(exp int) {
		if act := created; act != exp {
			t.Errorf(
				"unexpected number of created sessions: %v; want %v",
				act, exp,
			)
		}
	}
	assertDeleted := func(exp int) {
		if act := deleted; act != exp {
			t.Errorf(
				"unexpected number of deleted sessions: %v; want %v",
				act, exp,
			)
		}
	}
	p := &SessionPool{
		SizeLimit: 1,
		Builder: &StubBuilder{
			Handler: methodHandlers{
				testutil.TableCreateSession: func(req, res interface{}) error {
					created++
					return nil
				},
				testutil.TableDeleteSession: func(req, res interface{}) error {
					deleted++
					return nil
				},
			},
		},
	}
	defer func() {
		_ = p.Close(context.Background())
	}()

	s := mustGetSession(t, p)
	assertCreated(1)

	mustPutSession(t, p, s)
	assertDeleted(0)

	mustGetSession(t, p)
	assertCreated(1)

	_ = s.Close(context.Background())
	assertDeleted(1)

	mustGetSession(t, p)
	assertCreated(2)
}

func TestSessionPoolDisableBackgroundGoroutines(t *testing.T) {
	timer := timetest.StubSingleTimer(t)
	defer timer.Cleanup()

	p := &SessionPool{
		SizeLimit:     1,
		IdleThreshold: -1,
		Builder: &StubBuilder{
			T:       t,
			Limit:   1,
			Handler: methodHandlers{},
		},
	}

	s := mustGetSession(t, p)
	mustPutSession(t, p, s)

	const timeout = time.Second
	select {
	case <-timer.Created:
		t.Fatalf("unexpected created timer")
	case <-time.After(timeout):
	}
}

func TestSessionPoolKeepAlive(t *testing.T) {
	timer := timetest.StubSingleTimer(t)
	defer timer.Cleanup()

	shiftTime, cleanup := timeutil.StubTestHookTimeNow(time.Unix(0, 0))
	defer cleanup()

	var (
		idleThreshold = 4 * time.Second

		keepAliveCount uint32
	)
	p := &SessionPool{
		SizeLimit:     2,
		IdleThreshold: idleThreshold,
		Builder: &StubBuilder{
			T:     t,
			Limit: 2,
			Handler: methodHandlers{
				testutil.TableKeepAlive: func(req, res interface{}) error {
					atomic.AddUint32(&keepAliveCount, 1)
					return nil
				},
				testutil.TableDeleteSession: okHandler,
			},
		},
	}
	defer func() {
		_ = p.Close(context.Background())
	}()

	s1 := mustGetSession(t, p)
	s2 := mustGetSession(t, p)

	// Put both session at the absolutely same time.
	// That is, both sessions must be keepalived by a single tick (due to
	// KeepAliveBatchSize is unlimited).
	mustPutSession(t, p, s1)
	mustPutSession(t, p, s2)

	interval := <-timer.Created
	if interval != idleThreshold {
		t.Fatalf(
			"unexpected ticker duration: %s; want %s",
			interval, idleThreshold,
		)
	}

	// Emulate first simple tick event. We expect two sessions be keepalived.
	shiftTime(idleThreshold)
	timer.C <- timeutil.Now()
	mustResetTimer(t, timer.Reset, idleThreshold)
	if !atomic.CompareAndSwapUint32(&keepAliveCount, 2, 0) {
		t.Fatal("unexpected number of keepalives")
	}

	// Now get first session and "spent" some time working within it.
	x := mustGetSession(t, p)
	shiftTime(idleThreshold / 2)

	// Now put that session back and emulate keepalive moment.
	mustPutSession(t, p, x)
	shiftTime(idleThreshold / 2)
	// We expect here next tick to be registered after half of a idleThreshold.
	// That is, x was touched half of idleThreshold ago, so we need to wait for
	// the second half until we must touch it.
	timer.C <- timeutil.Now()
	mustResetTimer(t, timer.Reset, idleThreshold/2)
}

func TestSessionPoolKeepAliveOrdering(t *testing.T) {
	timer := timetest.StubSingleTimer(t)
	defer timer.Cleanup()

	shiftTime, cleanup := timeutil.StubTestHookTimeNow(time.Unix(0, 0))
	defer cleanup()

	var (
		idleThreshold = 4 * time.Second
		keepalive     = make(chan chan<- struct{})
	)
	p := &SessionPool{
		SizeLimit:     2,
		IdleThreshold: idleThreshold,
		Builder: &StubBuilder{
			T:     t,
			Limit: 2,
			Handler: methodHandlers{
				testutil.TableKeepAlive: func(req, res interface{}) error {
					done := make(chan struct{})
					keepalive <- done
					<-done
					return nil
				},
				testutil.TableDeleteSession: okHandler,
			},
		},
	}
	defer func() {
		_ = p.Close(context.Background())
	}()

	s1 := mustGetSession(t, p)
	s2 := mustGetSession(t, p)

	<-timer.Created

	// Put s1 to a pull. Shift time that pool need to keepalive s1.
	mustPutSession(t, p, s1)
	shiftTime(idleThreshold)
	timer.C <- timeutil.Now()

	// Await for keepalive request came in.
	var releaseKeepAlive chan<- struct{}
	select {
	case releaseKeepAlive = <-keepalive:
	case <-time.After(time.Millisecond * 100):
		t.Fatal("no keepalive request")
	}

	touchDone := p.touchCond()

	// Now keeper routine sticked on awaiting result of keep alive request.
	// That is perfect time to emulate race condition of pushing s2 back to the
	// pool with time, that is greater than `now` of s1 being touched.
	shiftTime(idleThreshold / 2)
	mustPutSession(t, p, s2)

	// Now release keepalive request, leading keeper to push s1 to the list
	// with touch time lower, than list's back element (s2).
	close(releaseKeepAlive)
	// Wait for touching routine exits.
	<-timer.Reset
	<-touchDone

	if x1 := mustGetSession(t, p); x1 != s1 {
		t.Errorf("reordering of sessions did not occur")
	}
}

func TestSessionPoolDoublePut(t *testing.T) {
	p := &SessionPool{
		SizeLimit:     2, // Skip panic on full pool.
		IdleThreshold: -1,
		Builder: &StubBuilder{
			T:     t,
			Limit: 1,
		},
	}

	s := mustGetSession(t, p)
	mustPutSession(t, p, s)

	defer func() {
		if thePanic := recover(); thePanic == nil {
			t.Fatalf("no panic")
		}
	}()
	_ = p.Put(context.Background(), s)
}

func TestSessionPoolReuseWaitChannel(t *testing.T) {
	p := SessionPool{}
	ch1 := p.getWaitCh()
	p.putWaitCh(ch1)
	ch2 := p.getWaitCh()
	if ch1 != ch2 {
		t.Errorf("unexpected reused channel")
	}
}

func TestSessionPoolKeepAliveCondFairness(t *testing.T) {
	timer := timetest.StubSingleTimer(t)
	defer timer.Cleanup()

	shiftTime, cleanupNow := timeutil.StubTestHookTimeNow(time.Unix(0, 0))
	defer cleanupNow()

	var (
		keepalive           = make(chan interface{})
		keepaliveResult     = make(chan error)
		deleteSession       = make(chan interface{})
		deleteSessionResult = make(chan error)
	)
	p := &SessionPool{
		SizeLimit:     1,
		IdleThreshold: time.Second,
		Builder: &StubBuilder{
			T:     t,
			Limit: 1,
			Handler: methodHandlers{
				testutil.TableKeepAlive: func(req, res interface{}) error {
					keepalive <- res
					return <-keepaliveResult
				},
				testutil.TableDeleteSession: func(req, res interface{}) error {
					deleteSession <- res
					return <-deleteSessionResult
				},
			},
		},
	}

	// First Get&Put to initialize pool's timers.
	mustPutSession(t, p, mustGetSession(t, p))
	<-timer.Created

	// Now the most interesting and delicate part: we want to emulate a race
	// condition between awaiting the session by touchCond() call and session
	// deletion after failed Keepalive().
	//
	// So first step is to force keepalive. Note that we do not send keepalive
	// result here making Keepalive() being blocked.
	shiftTime(5 * time.Second)
	timer.C <- timeutil.Now()
	<-keepalive

	cond := p.touchCond()
	assertFilled := func(want bool) {
		const timeout = time.Millisecond
		select {
		case e := <-cond:
			if !want {
				t.Fatalf("unexpected cond event: %v", e)
			}
		case <-time.After(timeout):
			if want {
				t.Fatalf("no cond event after %s", timeout)
			}
		}
	}

	// Now fail the Keepalive() call from above.
	keepaliveResult <- &ydb.OpError{
		Reason: ydb.StatusBadSession,
	}

	// Block the keeper()'s deletion routine.
	// While delete is not finished cond must not be fulfilled.
	<-deleteSession
	assertFilled(false)

	// Complete the session deletion routine. After that cond must become
	// fulfilled.
	deleteSessionResult <- nil
	assertFilled(true)
}

func TestSessionPoolKeepAliveMinSize(t *testing.T) {
	keepAliveCounter := make(map[string]int)

	p := &SessionPool{
		Builder: &StubBuilder{
			T:     t,
			Limit: 4,
			Handler: methodHandlers{
				testutil.TableDeleteSession: okHandler,
				testutil.TableKeepAlive: func(req, res interface{}) error {
					r := req.(*Ydb_Table.KeepAliveRequest)
					keepAliveCounter[r.SessionId]++
					return nil
				},
			},
		},
		SizeLimit:              3,
		KeepAliveMinSize:       1,
		IdleKeepAliveThreshold: 2,
		IdleThreshold:          2 * time.Second,
	}
	defer func() {
		_ = p.Close(context.Background())
	}()
	sessionBuilder := func(t *testing.T, poll *SessionPool) (*Session, chan bool) {
		s := mustCreateSession(t, poll)
		closed := make(chan bool)
		s.OnClose(func() {
			close(closed)
		})
		return s, closed
	}
	s1, c1 := sessionBuilder(t, p)
	s2, c2 := sessionBuilder(t, p)
	s3, c3 := sessionBuilder(t, p)
	mustPutSession(t, p, s1)
	mustPutSession(t, p, s2)
	mustPutSession(t, p, s3)
	<-c1
	<-c2

	if s3.closed {
		t.Fatalf("lower bound for sessions in the pool is not equal KeepAliveMinSize")
	}

	mustTakeSession(t, p, s3)
	s4, c4 := sessionBuilder(t, p)
	mustPutSession(t, p, s3)
	mustPutSession(t, p, s4)
	<-c3

	s := mustGetSession(t, p)
	if s != s4 {
		t.Fatalf("session is not reused")
	}
	_ = s.Close(context.Background())
	<-c4

	if keepAliveCounter[s1.ID] != 2 || keepAliveCounter[s2.ID] != 2 {
		t.Fatalf("incorrect keepAlive attempts: excpected: 2 found: %d %d", keepAliveCounter[s1.ID], keepAliveCounter[s2.ID])
	}
	if keepAliveCounter[s3.ID] != 5 {
		t.Fatalf("incorrect keepAlive attempts for reused session: excpected: 5 found: %d", keepAliveCounter[s3.ID])
	}
}

func TestSessionPoolKeepAliveWithBadSession(t *testing.T) {
	p := &SessionPool{
		Builder: &StubBuilder{
			T:     t,
			Limit: 4,
			Handler: methodHandlers{
				testutil.TableDeleteSession: okHandler,
				testutil.TableKeepAlive: func(req, res interface{}) error {
					return &ydb.OpError{
						Reason: ydb.StatusBadSession,
					}
				},
			},
		},
		SizeLimit:     3,
		IdleThreshold: 2 * time.Second,
	}
	defer func() {
		_ = p.Close(context.Background())
	}()
	s := mustCreateSession(t, p)
	closed := make(chan bool)
	s.OnClose(func() {
		close(closed)
	})
	mustPutSession(t, p, s)
	<-closed
}

func TestSessionPoolKeeperRetry(t *testing.T) {
	timer := timetest.StubSingleTimer(t)
	defer timer.Cleanup()
	shiftTime, cleanupNow := timeutil.StubTestHookTimeNow(time.Unix(0, 0))
	defer cleanupNow()

	retry := true
	p := &SessionPool{
		Builder: &StubBuilder{
			T:     t,
			Limit: 2,
			Handler: methodHandlers{
				testutil.TableDeleteSession: okHandler,
				testutil.TableKeepAlive: func(req, res interface{}) error {
					if retry {
						retry = false
						return context.DeadlineExceeded
					}
					return nil
				},
			},
		},
		IdleKeepAliveThreshold: -1,
		SizeLimit:              3,
		IdleThreshold:          3 * time.Second,
	}
	defer func() {
		_ = p.Close(context.Background())
	}()
	s := mustCreateSession(t, p)
	<-timer.Created
	s2 := mustCreateSession(t, p)
	mustPutSession(t, p, s)
	mustPutSession(t, p, s2)
	//retry
	shiftTime(p.IdleThreshold)
	timer.C <- timeutil.Now()
	<-timer.Reset
	//get first session
	s1 := mustGetSession(t, p)
	if s2 == s1 {
		t.Fatalf("retry session is not returned")
	}
	mustPutSession(t, p, s1)
	//keepalive success
	shiftTime(p.IdleThreshold)
	timer.C <- timeutil.Now()
	<-timer.Reset

	// get retry session
	s1 = mustGetSession(t, p)
	if s == s1 {
		t.Fatalf("second session is not returned")
	}
}

func mustResetTimer(t *testing.T, ch <-chan time.Duration, exp time.Duration) {
	select {
	case act := <-ch:
		if act != exp {
			t.Errorf(
				"unexpected timer reset: %s; want %s",
				act, exp,
			)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("%s: no timer reset", caller())
	}
}

func mustCreateSession(t *testing.T, p *SessionPool) *Session {
	wg := sync.WaitGroup{}
	defer wg.Wait()
	s, err := p.Create(
		WithSessionPoolTrace(
			context.Background(),
			SessionPoolTrace{
				OnCreate: func(info SessionPoolCreateStartInfo) func(SessionPoolCreateDoneInfo) {
					wg.Add(1)
					return func(info SessionPoolCreateDoneInfo) {
						wg.Done()
					}
				},
			},
		),
	)
	if err != nil {
		t.Fatalf("%s: %v", caller(), err)
	}
	return s
}

func mustGetSession(t *testing.T, p *SessionPool) *Session {
	wg := sync.WaitGroup{}
	defer wg.Wait()
	s, err := p.Get(
		WithSessionPoolTrace(
			context.Background(),
			SessionPoolTrace{
				OnGet: func(info SessionPoolGetStartInfo) func(SessionPoolGetDoneInfo) {
					wg.Add(1)
					return func(info SessionPoolGetDoneInfo) {
						wg.Done()
					}
				},
			},
		),
	)
	if err != nil {
		t.Fatalf("%s: %v", caller(), err)
	}
	return s
}

func mustPutSession(t *testing.T, p *SessionPool, s *Session) {
	wg := sync.WaitGroup{}
	defer wg.Wait()
	if err := p.Put(
		WithSessionPoolTrace(
			context.Background(),
			SessionPoolTrace{
				OnPut: func(info SessionPoolPutStartInfo) func(SessionPoolPutDoneInfo) {
					wg.Add(1)
					return func(info SessionPoolPutDoneInfo) {
						wg.Done()
					}
				},
				OnCloseSession: func(info SessionPoolCloseSessionStartInfo) func(doneInfo SessionPoolCloseSessionDoneInfo) {
					wg.Add(1)
					return func(info SessionPoolCloseSessionDoneInfo) {
						wg.Done()
					}
				},
			},
		),
		s,
	); err != nil {
		t.Fatalf("%s: %v", caller(), err)
	}
}

func mustTakeSession(t *testing.T, p *SessionPool, s *Session) {
	wg := sync.WaitGroup{}
	defer wg.Wait()
	took, err := p.Take(
		WithSessionPoolTrace(
			context.Background(),
			SessionPoolTrace{
				OnTake: func(info SessionPoolTakeStartInfo) func(SessionPoolTakeDoneInfo) {
					wg.Add(1)
					return func(info SessionPoolTakeDoneInfo) {
						wg.Done()
					}
				},
			},
		),
		s,
	)
	if !took {
		t.Fatalf("%s: can not take session (%v)", caller(), err)
	}
}

func mustClose(t *testing.T, p *SessionPool) {
	wg := sync.WaitGroup{}
	defer wg.Wait()
	if err := p.Close(
		WithSessionPoolTrace(
			context.Background(),
			SessionPoolTrace{
				OnCloseSession: func(info SessionPoolCloseSessionStartInfo) func(doneInfo SessionPoolCloseSessionDoneInfo) {
					wg.Add(1)
					return func(info SessionPoolCloseSessionDoneInfo) {
						wg.Done()
					}
				},
			},
		),
	); err != nil {
		t.Fatalf("%s: %v", caller(), err)
	}
}

func caller() string {
	_, file, line, _ := runtime.Caller(2)
	return fmt.Sprintf("%s:%d", path.Base(file), line)
}

type (
	methodHandler  func(req, res interface{}) error
	methodHandlers map[testutil.MethodCode]methodHandler
)

var okHandler = func(req, res interface{}) error {
	return nil
}

func simpleSession() *Session {
	return newSession(nil, 0, nil)
}

func newSession(t *testing.T, id int, h methodHandlers) *Session {
	return &Session{
		c: Client{
			Driver: &testutil.Driver{
				OnCall: func(ctx context.Context, m testutil.MethodCode, req, res interface{}) error {
					if h == nil {
						return nil
					}
					f := h[m]
					if f == nil {
						t.Fatalf("unexpected operation: %s", m)
					}
					return f(req, res)
				},
			},
		},
		ID: strconv.Itoa(id),
	}
}

type StubBuilder struct {
	OnCreateSession func(ctx context.Context) (*Session, error)

	Handler methodHandlers
	Limit   int
	T       *testing.T

	mu     sync.Mutex
	actual int
}

func (s *StubBuilder) CreateSession(ctx context.Context) (*Session, error) {
	s.mu.Lock()
	if s.Limit > 0 && s.actual == s.Limit {
		if s.T != nil {
			s.T.Errorf("create session limit overflow")
		}
		return nil, fmt.Errorf("stub builder: limit overflow")
	}
	s.actual++
	s.mu.Unlock()

	if f := s.OnCreateSession; f != nil {
		return f(ctx)
	}

	if s.Handler == nil || s.Handler[testutil.TableCreateSession] == nil {
		return newSession(s.T, s.actual, s.Handler), nil
	}
	var (
		req Ydb_Table.CreateSessionRequest
		res Ydb_Table.CreateSessionResult
	)
	err := s.Handler[testutil.TableCreateSession](&req, &res)
	if err != nil {
		return nil, err
	}

	r := newSession(s.T, s.actual, s.Handler)
	r.ID = res.SessionId

	return r, nil
}

func (p *SessionPool) debug() {
	fmt.Printf("head ")
	for el := p.idle.Front(); el != nil; el = el.Next() {
		s := el.Value.(*Session)
		x := p.index[s]
		fmt.Printf("<-> %s(%d) ", s.ID, x.touched.Unix())
	}
	fmt.Printf("<-> tail\n")
}

func whenWantWaitCh(p *SessionPool) <-chan struct{} {
	var (
		prev = p.testHookGetWaitCh
		ch   = make(chan struct{})
	)
	p.testHookGetWaitCh = func() {
		p.testHookGetWaitCh = prev
		close(ch)
	}
	return ch
}
