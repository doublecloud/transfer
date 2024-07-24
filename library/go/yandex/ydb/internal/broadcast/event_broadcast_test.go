package broadcast

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEventBroadcast(t *testing.T) {
	t.Run("Simple", func(t *testing.T) {
		b := &EventBroadcast{}
		waiter := b.Waiter()
		b.Notify()
		select {
		case <-waiter.Done():
		case <-time.After(time.Second):
			t.Fail()
		}
	})

	t.Run("SubscribeAndEventsInRace", func(t *testing.T) {
		testDuration := time.Second / 100

		b := &EventBroadcast{}
		var events atomic.Int64

		var backgroundCounter atomic.Int64
		firstWaiterStarted := atomic.Bool{}

		stopSubscribe := atomic.Bool{}

		subscribeStopped := make(chan struct{})
		broadcastStopped := make(chan struct{})

		// Add subscribers
		go func() {
			defer close(subscribeStopped)
			for {
				backgroundCounter.Add(1)
				waiter := b.Waiter()
				firstWaiterStarted.Store(true)
				go func() {
					<-waiter.Done()
					backgroundCounter.Add(-1)
				}()
				if stopSubscribe.Load() {
					return
				}
			}
		}()

		stopBroadcast := atomic.Bool{}
		go func() {
			defer close(broadcastStopped)

			// Fire events
			for {
				events.Add(1)
				b.Notify()
				runtime.Gosched()
				if stopBroadcast.Load() {
					return
				}
			}
		}()

		for !firstWaiterStarted.Load() {
			runtime.Gosched()
		}

		<-time.After(testDuration)

		stopSubscribe.Store(true)
		<-subscribeStopped

		for {
			oldCounter := backgroundCounter.Load()
			if oldCounter == 0 {
				break
			}

			t.Log("background counter", oldCounter)
			for backgroundCounter.Load() >= oldCounter {
				runtime.Gosched()
			}
		}
		stopBroadcast.Store(true)
		<-broadcastStopped

		require.Greater(t, events.Load(), int64(0))
	})
}
