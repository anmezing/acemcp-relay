package main

import (
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestPruneExpiredMCPSessions(t *testing.T) {
	now := time.Date(2026, 7, 26, 3, 0, 0, 0, time.UTC)
	sessions := map[string]*mcpServerSession{
		"expired": {userID: "one", lastActivity: now.Add(-31 * time.Minute)},
		"edge":    {userID: "one", lastActivity: now.Add(-30 * time.Minute)},
		"active":  {userID: "two", lastActivity: now.Add(-time.Minute)},
	}

	expired := pruneExpiredMCPSessions(sessions, now, 30*time.Minute)
	sort.Strings(expired)

	if !reflect.DeepEqual(expired, []string{"expired"}) {
		t.Fatalf("unexpected expired sessions: %#v", expired)
	}
	if _, ok := sessions["edge"]; !ok {
		t.Fatal("session exactly at the TTL must remain active")
	}
}

func TestEvictOldestMCPSessionForUser(t *testing.T) {
	now := time.Date(2026, 7, 26, 3, 0, 0, 0, time.UTC)
	sessions := map[string]*mcpServerSession{
		"one-new": {userID: "one", lastActivity: now.Add(-time.Minute)},
		"one-old": {userID: "one", lastActivity: now.Add(-2 * time.Minute)},
		"two-old": {userID: "two", lastActivity: now.Add(-3 * time.Minute)},
	}

	evicted, ok := evictOldestMCPSession(sessions, "one")
	if !ok || evicted != "one-old" {
		t.Fatalf("evicted (%q, %v), want one-old", evicted, ok)
	}
	if _, ok := sessions["two-old"]; !ok {
		t.Fatal("another user's session must not be evicted")
	}
}

func TestPrepareMCPSessionSlotEnforcesPerUserAndGlobalLimits(t *testing.T) {
	now := time.Date(2026, 7, 26, 3, 0, 0, 0, time.UTC)
	sessions := map[string]*mcpServerSession{
		"expired": {userID: "one", lastActivity: now.Add(-31 * time.Minute)},
		"one-old": {userID: "one", lastActivity: now.Add(-3 * time.Minute)},
		"one-new": {userID: "one", lastActivity: now.Add(-time.Minute)},
		"two-old": {userID: "two", lastActivity: now.Add(-4 * time.Minute)},
	}

	expired, evicted := prepareMCPSessionSlot(sessions, "one", now, 30*time.Minute, 2, 2)

	if !reflect.DeepEqual(expired, []string{"expired"}) {
		t.Fatalf("unexpected expired sessions: %#v", expired)
	}
	if !reflect.DeepEqual(evicted, []string{"one-old", "two-old"}) {
		t.Fatalf("unexpected evicted sessions: %#v", evicted)
	}
	if got := len(sessions); got != 1 {
		t.Fatalf("expected one existing session before insert, got %d", got)
	}
	if _, ok := sessions["one-new"]; !ok {
		t.Fatal("newest session should remain")
	}
}

func TestTouchMCPSessionRequiresOwner(t *testing.T) {
	now := time.Date(2026, 7, 26, 3, 0, 0, 0, time.UTC)
	previous := now.Add(-time.Minute)
	sessions := map[string]*mcpServerSession{
		"session": {userID: "owner", lastActivity: previous},
	}

	if touchMCPSession(sessions, "session", "other", now) {
		t.Fatal("another user must not be able to touch the session")
	}
	if !sessions["session"].lastActivity.Equal(previous) {
		t.Fatal("failed ownership check must not update last activity")
	}
	if !touchMCPSession(sessions, "session", "owner", now) {
		t.Fatal("owner should be able to touch the session")
	}
	if !sessions["session"].lastActivity.Equal(now) {
		t.Fatal("owner touch must update last activity")
	}
}
