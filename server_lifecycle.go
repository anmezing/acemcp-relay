package main

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"
)

// Seal admission before waiting so a late handler cannot race WaitGroup.Add.
type backgroundWrites struct {
	mu     sync.Mutex
	closed bool
	active sync.WaitGroup
}

func (w *backgroundWrites) run(fn func()) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return false
	}
	w.active.Add(1)
	go func() { defer w.active.Done(); fn() }()
	return true
}

func (w *backgroundWrites) drain() {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	w.active.Wait()
}

var requestLogWrites backgroundWrites

func serveUntilCancelled(ctx context.Context, server *http.Server) error {
	finished := make(chan error, 1)
	go func() { finished <- server.ListenAndServe() }()
	select {
	case err := <-finished:
		return err
	case <-ctx.Done():
	}
	// Long-lived SSE connections do not drain by themselves.
	mcpSSEStreamsMu.Lock()
	for id, stream := range mcpSSEStreams {
		delete(mcpSSEStreams, id)
		stream.close()
	}
	mcpSSEStreamsMu.Unlock()
	shutdown, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err := server.Shutdown(shutdown)
	if err != nil {
		_ = server.Close()
	}
	serveErr := <-finished
	if err != nil {
		return err
	}
	if !errors.Is(serveErr, http.ErrServerClosed) {
		return serveErr
	}
	return nil
}
