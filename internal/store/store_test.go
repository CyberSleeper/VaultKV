package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// BEGIN AI SECTION

// setupStore creates a completely isolated new Store instance using a temporary file path
func setupStore(t *testing.T, prefix string) (*Store, func()) {
	nodeID := prefix + "_testnode"
	dir := t.TempDir()

	s, err := NewStore(dir, nodeID)
	if err != nil {
		t.Fatalf("failed to initialize store: %v", err)
	}

	// We no longer need to manually delete the file because t.TempDir() handles it
	// However, we MUST close the store to release file descriptors on Windows
	cleanup := func() {
		_ = s.Close()
	}

	return s, cleanup
}

func TestStore_SetGetDelete(t *testing.T) {
	s, cleanup := setupStore(t, "basic")
	defer cleanup()

	// Test Set
	if err := s.Set("hero", "batman"); err != nil {
		t.Fatalf("Expected nil err on Set, got: %v", err)
	}

	// Test Get
	val, ok := s.Get("hero")
	if !ok || val != "batman" {
		t.Errorf("Expected batman, got %s (ok: %v)", val, ok)
	}

	// Test Delete
	if err := s.Delete("hero"); err != nil {
		t.Fatalf("Expected nil err on Delete, got: %v", err)
	}

	val, ok = s.Get("hero")
	if ok || val != "" {
		t.Errorf("Expected hero to be deleted, got %s (ok: %v)", val, ok)
	}
}

func TestStore_RecoveryFromWAL(t *testing.T) {
	nodeID := "recovery_testnode"
	dir := t.TempDir()

	// 1. Initialize a generic store and write some data
	s1, err := NewStore(dir, nodeID)
	if err != nil {
		t.Fatalf("failed to init first store: %v", err)
	}
	s1.Set("persisted_key", "survives_crash")
	s1.Set("deleted_key", "will_be_gone")
	s1.Delete("deleted_key")
	s1.wal.Close() // Simulate a crash / shutdown

	// 2. Start a brand new Store instance pointing to the exact same directory
	s2, err := NewStore(dir, nodeID)
	if err != nil {
		t.Fatalf("failed to init recovered store: %v", err)
	}
	defer func() {
		s2.wal.Close()
	}()

	// 3. Verify the MemTable was accurately rebuilt from the WAL entries (Puts and Tombstones)
	val, ok := s2.Get("persisted_key")
	if !ok || val != "survives_crash" {
		t.Errorf("Expected 'survives_crash' from recovered store, got %s (ok: %v)", val, ok)
	}

	val, ok = s2.Get("deleted_key")
	if ok || val != "" {
		t.Errorf("Expected 'deleted_key' to still be a Tombstone after recovery, got %s (ok: %v)", val, ok)
	}
}

func TestStore_ConcurrentSetGet(t *testing.T) {
	s, cleanup := setupStore(t, "concurrent")
	defer cleanup()

	var wg sync.WaitGroup
	workers := 20

	// Blast the Store with concurrent WAL Appends + MemTable Sets
	for i := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("k-%d", id)
			val := fmt.Sprintf("v-%d", id)

			if err := s.Set(key, val); err != nil {
				t.Errorf("Concurrent set failed for worker %d: %v", id, err)
			}

			readVal, ok := s.Get(key)
			if !ok || readVal != val {
				t.Errorf("Concurrent get failed for worker %d. Expected %s, got %s (ok: %v)", id, val, readVal, ok)
			}
		}(i)
	}

	wg.Wait()
}

func TestStore_GracefulShutdown_Idempotent(t *testing.T) {
	s, cleanup := setupStore(t, "close_idempotent")
	defer cleanup()

	// Calling Close() multiple times should not error or panic
	err1 := s.Close()
	err2 := s.Close()
	err3 := s.Close()

	if err1 != nil || err2 != nil || err3 != nil {
		t.Fatalf("Expected nil on multiple closes, got: %v, %v, %v", err1, err2, err3)
	}
}

func TestStore_GracefulShutdown_PanicProtection(t *testing.T) {
	s, cleanup := setupStore(t, "close_panic")
	defer cleanup()

	var wg sync.WaitGroup

	// Use a very large payload (1MB) to force the MemTable to fill up
	// and trigger the `s.flushChan <- task` branch quickly
	largeVal := make([]byte, 1024*1024)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_ = s.Set(fmt.Sprintf("k%d", id), string(largeVal))
			_ = s.Delete(fmt.Sprintf("k%d", id))
		}(i)
	}

	// Wait some time to let some goroutines start their work
	// before we brutally close the store on them
	time.Sleep(50 * time.Millisecond)
	s.Close()

	wg.Wait()
}

// waitForFlush polls until at least minSSTs SSTables exist AND the frozen
// memtable queue has drained. Fails the test if it does not happen by deadline.
func waitForFlush(t *testing.T, s *Store, minSSTs int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.RLock()
		nSST := len(s.sstables)
		nFrozen := len(s.frozenMemTs)
		s.mu.RUnlock()
		if nSST >= minSSTs && nFrozen == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	t.Fatalf("flush did not complete: ssts=%d (want >=%d), frozen=%d (want 0)",
		len(s.sstables), minSSTs, len(s.frozenMemTs))
}

// forceFlush writes enough 1MB blobs to exceed the 4MB memtable threshold and
// trigger a background flush. Returns the keys it wrote.
func forceFlush(t *testing.T, s *Store, keyPrefix string) []string {
	t.Helper()
	big := strings.Repeat("x", 1024*1024) // 1MB
	const blobs = 5                       // 5MB > 4MB threshold
	keys := make([]string, blobs)
	for i := 0; i < blobs; i++ {
		k := fmt.Sprintf("%s_%d", keyPrefix, i)
		keys[i] = k
		if err := s.Set(k, big); err != nil {
			t.Fatalf("Set %s failed: %v", k, err)
		}
	}
	return keys
}

func TestStore_SSTRoundTrip(t *testing.T) {
	s, cleanup := setupStore(t, "sst_roundtrip")
	defer cleanup()

	keys := forceFlush(t, s, "blob")
	waitForFlush(t, s, 1)

	// Every flushed key must still be readable through the SST path
	want := strings.Repeat("x", 1024*1024)
	for _, k := range keys {
		val, ok := s.Get(k)
		if !ok {
			t.Errorf("expected %s to be readable after flush, got not-found", k)
			continue
		}
		if val != want {
			t.Errorf("value mismatch for %s: len(got)=%d, len(want)=%d", k, len(val), len(want))
		}
	}
}

func TestStore_RecoveryFromSST(t *testing.T) {
	nodeID := "sst_recovery_testnode"
	dir := t.TempDir()

	s1, err := NewStore(dir, nodeID)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	keys := forceFlush(t, s1, "persisted")
	waitForFlush(t, s1, 1)

	if err := s1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen pointing at the same directory — data must come back from SST
	s2, err := NewStore(dir, nodeID)
	if err != nil {
		t.Fatalf("recovered NewStore failed: %v", err)
	}
	defer s2.Close()

	if len(s2.sstables) == 0 {
		t.Fatalf("expected recovered store to load at least 1 SSTable, got 0")
	}

	want := strings.Repeat("x", 1024*1024)
	for _, k := range keys {
		val, ok := s2.Get(k)
		if !ok || val != want {
			t.Errorf("key %s not recovered from SST (ok=%v, len=%d)", k, ok, len(val))
		}
	}
}

func TestStore_TombstoneShadowsSST(t *testing.T) {
	s, cleanup := setupStore(t, "tombstone_sst")
	defer cleanup()

	keys := forceFlush(t, s, "doomed")
	waitForFlush(t, s, 1)

	// Sanity: keys live in SST now
	if _, ok := s.Get(keys[0]); !ok {
		t.Fatalf("precondition failed: %s should be readable from SST", keys[0])
	}

	// Delete one of the flushed keys via the active memtable tombstone
	if err := s.Delete(keys[0]); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	val, ok := s.Get(keys[0])
	if ok || val != "" {
		t.Errorf("expected tombstone in memtable to shadow SST entry; got val=%q ok=%v", val, ok)
	}
}

func TestStore_SetAfterCloseDoesNotPanic(t *testing.T) {
	s, _ := setupStore(t, "set_after_close")
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Set after Close panicked: %v", r)
		}
	}()

	// Should return an error (WAL closed) and absolutely must not panic
	if err := s.Set("k", "v"); err == nil {
		t.Errorf("expected Set after Close to return an error, got nil")
	}
}

func TestStore_InvalidNodeIDRejected(t *testing.T) {
	dir := t.TempDir()
	badIDs := []string{"", "has space", "bad/slash", "bang!", "dot.sep"}
	for _, id := range badIDs {
		if _, err := NewStore(dir, id); err == nil {
			t.Errorf("expected invalid nodeID %q to be rejected", id)
		}
	}
}

func TestStore_CorruptedSSTOnStartupErrors(t *testing.T) {
	dir := t.TempDir()
	nodeID := "corrupt_sst_testnode"

	// Drop a malformed file matching the SST glob
	badPath := filepath.Join(dir, fmt.Sprintf("vault_%s_1234567890.sst", nodeID))
	if err := os.WriteFile(badPath, []byte("definitely not a real sst"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	if _, err := NewStore(dir, nodeID); err == nil {
		t.Errorf("expected NewStore to fail on corrupted SST, got nil err")
	}
}

func TestStore_GetFromFrozenMemTable(t *testing.T) {
	s, cleanup := setupStore(t, "frozen_get")
	defer cleanup()

	// Write a small canary first (will live in the active memtable)
	if err := s.Set("canary", "alive"); err != nil {
		t.Fatalf("Set canary failed: %v", err)
	}

	// Now force a flush. The canary is part of the skiplist that just got
	// frozen — it should remain visible regardless of whether the flush
	// worker has finished writing the SST yet.
	forceFlush(t, s, "filler")

	// Immediately query the canary. It must be readable from either:
	//   (a) the frozen memtable (flush still in progress), or
	//   (b) the new SST (flush already finished).
	val, ok := s.Get("canary")
	if !ok || val != "alive" {
		t.Errorf("canary lost across flush boundary: val=%q ok=%v", val, ok)
	}

	// And again after the flush has definitely landed
	waitForFlush(t, s, 1)
	val, ok = s.Get("canary")
	if !ok || val != "alive" {
		t.Errorf("canary lost after flush completion: val=%q ok=%v", val, ok)
	}
}

// END AI SECTION
