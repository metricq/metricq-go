package metricq

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestTransformerCloseCancelsAndWaitsForConsumer(t *testing.T) {
	consumerCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var stopped atomic.Bool
	transformer := &Transformer{Agent: &Agent{}, inputCancel: cancel, inputDone: done}
	go func() {
		<-consumerCtx.Done()
		time.Sleep(20 * time.Millisecond)
		stopped.Store(true)
		close(done)
	}()

	transformer.Close()
	if !stopped.Load() {
		t.Fatal("Close returned before the input consumer stopped")
	}

	// Closing repeatedly must neither panic nor wait on the already consumed done channel.
	transformer.Close()
}
