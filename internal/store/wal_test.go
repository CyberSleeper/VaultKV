package store

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// BEGIN AI SECTION

func newTestWAL(t *testing.T) (*WAL, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := NewWAL(path)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	return w, path
}

func TestWAL_AppendReadAllRoundTrip(t *testing.T) {
	w, path := newTestWAL(t)

	entries := []*LogEntry{
		NewLogEntry("alpha", "1"),
		NewLogEntry("beta", "two"),
		NewLogEntry("gamma", "iii"),
	}

	for _, e := range entries {
		if err := w.Append(e); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and ReadAll
	w2, err := NewWAL(path)
	if err != nil {
		t.Fatalf("reopen NewWAL failed: %v", err)
	}
	defer w2.Close()

	got, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if len(got) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(got))
	}
	for i, e := range entries {
		if got[i].Key != e.Key || got[i].Value != e.Value {
			t.Errorf("entry %d mismatch: expected {%s:%s}, got {%s:%s}",
				i, e.Key, e.Value, got[i].Key, got[i].Value)
		}
	}
}

func TestWAL_EmptyReadAll(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()

	got, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll on empty WAL returned err: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected 0 entries from empty WAL, got %d", len(got))
	}
}

func TestWAL_ChecksumCorruptionDetected(t *testing.T) {
	w, path := newTestWAL(t)

	if err := w.Append(NewLogEntry("k", "v")); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	w.Close()

	// Flip a byte in the payload (skip the 4-byte checksum at the head)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if len(raw) < 6 {
		t.Fatalf("WAL file unexpectedly small: %d bytes", len(raw))
	}
	raw[len(raw)-1] ^= 0xFF
	if err := os.WriteFile(path, raw, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	w2, err := NewWAL(path)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer w2.Close()

	if _, err := w2.ReadAll(); err == nil {
		t.Errorf("expected ReadAll to detect checksum corruption, got nil err")
	}
}

func TestWAL_AppendAfterCloseErrors(t *testing.T) {
	w, _ := newTestWAL(t)
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	err := w.Append(NewLogEntry("k", "v"))
	if err == nil {
		t.Errorf("expected error appending to closed WAL, got nil")
	}
}

func TestWAL_ReadAllAfterCloseErrors(t *testing.T) {
	w, _ := newTestWAL(t)
	w.Close()

	if _, err := w.ReadAll(); err == nil {
		t.Errorf("expected ReadAll on closed WAL to error, got nil")
	}
}

func TestWAL_DeleteRemovesFile(t *testing.T) {
	w, path := newTestWAL(t)

	if err := w.Append(NewLogEntry("k", "v")); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	if err := w.Delete(); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("expected file to be removed, Stat err = %v", err)
	}
}

func TestWAL_DeleteIdempotent(t *testing.T) {
	w, _ := newTestWAL(t)

	if err := w.Delete(); err != nil {
		t.Fatalf("first Delete failed: %v", err)
	}
	if err := w.Delete(); err != nil {
		t.Errorf("second Delete should be a no-op, got: %v", err)
	}
}

func TestWAL_OversizedValueRejected(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()

	// Build a value with length > math.MaxUint32 is impossible to allocate in
	// a unit test, but we can verify the key-length guard instead by exceeding
	// math.MaxUint16 (65535) bytes.
	bigKey := strings.Repeat("x", 65536)

	err := w.Append(&LogEntry{Key: bigKey, Value: "v"})
	if err == nil {
		t.Errorf("expected oversized key to be rejected, got nil err")
	}
}

func TestWAL_NilEntryRejected(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()

	if err := w.Append(nil); err == nil {
		t.Errorf("expected nil-entry append to error, got nil")
	}
}

func TestWAL_ReadAllStopsAtTruncatedTail(t *testing.T) {
	// If the process crashes mid-write, ReadAll should return the entries it
	// could fully recover, treating ErrUnexpectedEOF as a clean stop.
	w, path := newTestWAL(t)

	if err := w.Append(NewLogEntry("complete", "entry")); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	w.Close()

	// Append a torn record: just a checksum + partial header, no payload
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("reopen for torn write failed: %v", err)
	}
	if err := binary.Write(f, binary.LittleEndian, uint32(0xDEADBEEF)); err != nil {
		t.Fatalf("torn write failed: %v", err)
	}
	// Write a partial keyLen (1 byte instead of 2) to guarantee unexpected EOF
	if _, err := f.Write([]byte{0x01}); err != nil {
		t.Fatalf("torn write failed: %v", err)
	}
	f.Close()

	w2, err := NewWAL(path)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer w2.Close()

	got, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll should treat torn tail as clean stop, got err: %v", err)
	}
	if len(got) != 1 || got[0].Key != "complete" {
		t.Errorf("expected 1 recovered entry 'complete', got %+v", got)
	}
}

// END AI SECTION
