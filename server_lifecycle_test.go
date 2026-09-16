package main

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestServerShutdownDrainsActiveRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listening := make(chan string, 1)
	entered, release := make(chan struct{}), make(chan struct{})
	server := &http.Server{Addr: "127.0.0.1:0", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		select {
		case <-release:
			w.WriteHeader(http.StatusNoContent)
		case <-r.Context().Done():
		}
	}), BaseContext: func(listener net.Listener) context.Context {
		listening <- listener.Addr().String()
		return context.Background()
	}}
	stopped := make(chan error, 1)
	go func() { stopped <- serveUntilCancelled(ctx, server) }()
	response := make(chan error, 1)
	go func() {
		client := &http.Client{Timeout: 3 * time.Second}
		res, err := client.Get("http://" + <-listening)
		if err == nil {
			res.Body.Close()
			if res.StatusCode != http.StatusNoContent {
				t.Errorf("status=%d", res.StatusCode)
			}
		}
		response <- err
	}()
	<-entered
	cancel()
	close(release)
	if err := <-response; err != nil {
		t.Fatal(err)
	}
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
}

func TestSSEBackpressureClosesSlowConsumer(t *testing.T) {
	stream := registerMCPSSEStream("slow-consumer")
	defer unregisterMCPSSEStream("slow-consumer", stream)
	for i := 0; i < cap(stream.events); i++ {
		if !publishMCPSSEEvent("slow-consumer", []byte("event")) {
			t.Fatal("unexpected send failure")
		}
	}
	if publishMCPSSEEvent("slow-consumer", []byte("overflow")) {
		t.Fatal("overflow accepted")
	}
	select {
	case <-stream.done:
	default:
		t.Fatal("slow stream remains open")
	}
}

func TestBackgroundWritesDrainAndRejectLateAdmission(t *testing.T) {
	var writes backgroundWrites
	release, drained := make(chan struct{}), make(chan struct{})
	if !writes.run(func() { <-release }) {
		t.Fatal("initial write rejected")
	}
	go func() { writes.drain(); close(drained) }()
	// Observe sealed admission without relying on scheduling sleeps.
	for {
		writes.mu.Lock()
		closed := writes.closed
		writes.mu.Unlock()
		if closed {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if writes.run(func() { t.Error("late write executed") }) {
		t.Fatal("late write accepted")
	}
	select {
	case <-drained:
		t.Fatal("drained before write completed")
	default:
	}
	close(release)
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("drain did not complete")
	}
}

func TestBusinessIdleSessionExpiresWithConnectedSSE(t *testing.T) {
	const id = "idle-connected-sse"
	serverSessionsMu.Lock()
	serverSessions[id] = &mcpServerSession{userID: "user", lastActivity: time.Now().Add(-mcpSessionTTL - time.Second)}
	serverSessionsMu.Unlock()
	stream := registerMCPSSEStream(id)
	defer unregisterMCPSSEStream(id, stream)
	sweepExpiredMCPSessions()
	select {
	case <-stream.done:
	default:
		t.Fatal("idle connected stream retained its session")
	}
}
