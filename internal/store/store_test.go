package store

import (
	"fmt"
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

	// We no longer need to manually delete the file because t.TempDir() handles it!
	cleanup := func() {
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

// END AI SECTION
