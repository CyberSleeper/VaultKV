package store

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// BEGIN AI SECTION

func TestSSTable_BlockFormat(t *testing.T) {
	// Two small entries fit comfortably inside a single ~4KB data block, so the
	// sparse index should hold exactly one entry pointing at the start of that
	// block (offset 0). The footer layout is:
	//   [indexChecksum(4)][indexOffset(4)][magicNumber(4)]
	entry := NewSSTableEntry()
	entry.LogEntries = append(entry.LogEntries, NewLogEntry("apple", "red"))
	entry.LogEntries = append(entry.LogEntries, NewLogEntry("banana", "yellow"))

	var buf bytes.Buffer
	if err := entry.Encode(&buf); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	data := buf.Bytes()

	// Checkpoint 1: minimum size — at least one block + index + footer
	if len(data) < 12 {
		t.Fatalf("Encoded data too small: %d bytes", len(data))
	}

	// Checkpoint 2: the first data block begins with numEntries == 2
	blockNumEntries := binary.LittleEndian.Uint16(data[0:2])
	if blockNumEntries != 2 {
		t.Fatalf("expected first block to hold 2 entries, got %d", blockNumEntries)
	}

	// Checkpoint 3: magic number occupies the final 4 bytes
	footerStart := len(data) - 12
	magicNum := binary.LittleEndian.Uint32(data[footerStart+8 : footerStart+12])
	if magicNum != magicNumber {
		t.Fatalf("expected magic 0x%X at end of footer, got 0x%X", magicNumber, magicNum)
	}

	// Checkpoint 4: index offset is the middle 4 bytes of the footer and points
	// inside the file
	indexOffset := binary.LittleEndian.Uint32(data[footerStart+4 : footerStart+8])
	if int(indexOffset) > footerStart {
		t.Fatalf("index offset %d overlaps footer at %d", indexOffset, footerStart)
	}

	// Checkpoint 5: index checksum (first 4 footer bytes) verifies over the
	// index region AND the indexOffset footer field
	indexChecksum := binary.LittleEndian.Uint32(data[footerStart : footerStart+4])
	if indexBlockChecksum(data[indexOffset:footerStart], indexOffset) != indexChecksum {
		t.Fatalf("index checksum mismatch")
	}

	// Checkpoint 6: index has exactly one entry, pointing at block offset 0
	indexData := data[indexOffset:footerStart]
	numIndexEntries := binary.LittleEndian.Uint32(indexData[0:4])
	if numIndexEntries != 1 {
		t.Fatalf("expected 1 sparse index entry, got %d", numIndexEntries)
	}
	firstPtr := binary.LittleEndian.Uint32(indexData[4:8])
	if firstPtr != 0 {
		t.Fatalf("expected first block pointer at byte 0, got %d", firstPtr)
	}
}

func TestSSTable_DecodeRoundTrip(t *testing.T) {
	// 1. Create original source of truth
	original := NewSSTableEntry()
	original.LogEntries = append(original.LogEntries, NewLogEntry("hello", "world"))
	original.LogEntries = append(original.LogEntries, NewLogEntry("vault", "kv"))

	// 2. Encode using your Option 3 format
	var buf bytes.Buffer
	err := original.Encode(&buf)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	// 3. Setup a ReadSeeker for the Decoder
	rawBytes := buf.Bytes()
	reader := bytes.NewReader(rawBytes) // bytes.NewReader naturally implements io.ReadSeeker!

	// 4. Fire the Decode parsing!
	decoded := NewSSTableEntry()

	err = decoded.Decode(reader)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// 5. Verify perfect mathematical extraction
	if len(decoded.LogEntries) != len(original.LogEntries) {
		t.Fatalf("Decode returned %d entries, expected %d", len(decoded.LogEntries), len(original.LogEntries))
	}

	for i, entry := range original.LogEntries {
		if decoded.LogEntries[i].Key != entry.Key || decoded.LogEntries[i].Value != entry.Value {
			t.Errorf("Mismatch at index %d! Expected {%s: %s}, Got {%s: %s}",
				i, entry.Key, entry.Value, decoded.LogEntries[i].Key, decoded.LogEntries[i].Value)
		}
	}
	t.Log("✅ Decode completely successful! All entries matched perfectly.")
}

func TestSSTable_OnDiskRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "roundtrip.sst")

	// 1. Build a skiplist with a few known entries (in alphabetical order
	//    because SSTable point reads rely on sorted index).
	sl := NewSkiplist()
	sl.Set("apple", "red")
	sl.Set("banana", "yellow")
	sl.Set("cherry", "crimson")

	// 2. Create + flush the SSTable
	sst, err := NewSST(path)
	if err != nil {
		t.Fatalf("NewSSTable failed: %v", err)
	}
	if err := sst.Flush(sl); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	if err := sst.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 3. Reopen and load the index block
	reopened, err := NewSST(path)
	if err != nil {
		t.Fatalf("reopen NewSSTable failed: %v", err)
	}
	defer reopened.Close()

	if err := reopened.LoadIndexBlock(); err != nil {
		t.Fatalf("LoadIndexBlock failed: %v", err)
	}

	// 4. The sparse index holds one entry PER BLOCK, not per key. These three
	//    small entries fit in a single ~4KB block, so we expect exactly one
	//    index entry whose key is the first key in the block ("apple").
	if len(reopened.indexEntries) != 1 {
		t.Fatalf("expected 1 sparse index entry, got %d", len(reopened.indexEntries))
	}
	if got := string(reopened.indexEntries[0].KeyBytes); got != "apple" {
		t.Errorf("expected first-key 'apple' in sparse index, got %q", got)
	}

	// 5. Every key must be retrievable through the real lookup path
	//    (sparse-index search -> block read -> CRC verify -> scan).
	cases := map[string]string{"apple": "red", "banana": "yellow", "cherry": "crimson"}
	for k, want := range cases {
		val, found, err := reopened.lookup(k)
		if err != nil {
			t.Fatalf("lookup(%q) errored: %v", k, err)
		}
		if !found || val != want {
			t.Errorf("lookup(%q): got (%q, %v), want (%q, true)", k, val, found, want)
		}
	}

	// 6. A key that was never inserted must report not-found cleanly
	if _, found, err := reopened.lookup("durian"); err != nil || found {
		t.Errorf("lookup(durian): expected (_, false, nil), got (found=%v, err=%v)", found, err)
	}
}

func TestSSTable_LoadIndexBlock_BadMagic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad_magic.sst")

	sl := NewSkiplist()
	sl.Set("k", "v")

	sst, err := NewSST(path)
	if err != nil {
		t.Fatalf("NewSSTable failed: %v", err)
	}
	if err := sst.Flush(sl); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	sst.Close()

	// Corrupt the magic number, which now occupies the final 4 bytes of the
	// footer: [indexChecksum(4)][indexOffset(4)][magicNumber(4)]
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if len(raw) < 12 {
		t.Fatalf("SST file too small: %d", len(raw))
	}
	for i := len(raw) - 4; i < len(raw); i++ {
		raw[i] = 0x00
	}
	if err := os.WriteFile(path, raw, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	reopened, err := NewSST(path)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	if err := reopened.LoadIndexBlock(); err == nil {
		t.Errorf("expected LoadIndexBlock to reject bad magic number, got nil err")
	}
}

func TestSSTable_LoadIndexBlock_CorruptedIndexOffset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad_offset.sst")

	sl := NewSkiplist()
	sl.Set("k", "v")

	sst, err := NewSST(path)
	if err != nil {
		t.Fatalf("NewSST failed: %v", err)
	}
	if err := sst.Flush(sl); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	sst.Close()

	// indexOffset is the middle 4 bytes of the footer:
	// [indexChecksum(4)][indexOffset(4)][magicNumber(4)]. Flip its low byte so
	// it still points inside the file but no longer matches what the index
	// checksum was computed against.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if len(raw) < 12 {
		t.Fatalf("SST file too small: %d", len(raw))
	}
	raw[len(raw)-8] ^= 0x01
	if err := os.WriteFile(path, raw, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	reopened, err := NewSST(path)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	if err := reopened.LoadIndexBlock(); err == nil {
		t.Errorf("expected LoadIndexBlock to detect corrupted indexOffset, got nil err")
	}
}

func TestSSTable_LoadIndexBlock_TruncatedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "truncated.sst")

	if err := os.WriteFile(path, []byte("short"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	sst, err := NewSST(path)
	if err != nil {
		t.Fatalf("NewSSTable failed: %v", err)
	}
	defer sst.Close()

	if err := sst.LoadIndexBlock(); err == nil {
		t.Errorf("expected LoadIndexBlock to fail on truncated file, got nil err")
	}
}

func TestSSTable_FlushEmptySkiplist(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.sst")

	sl := NewSkiplist()

	sst, err := NewSST(path)
	if err != nil {
		t.Fatalf("NewSSTable failed: %v", err)
	}
	if err := sst.Flush(sl); err != nil {
		t.Fatalf("Flush of empty skiplist failed: %v", err)
	}
	sst.Close()

	reopened, err := NewSST(path)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	if err := reopened.LoadIndexBlock(); err != nil {
		t.Fatalf("LoadIndexBlock on empty-skiplist SST failed: %v", err)
	}
	if len(reopened.indexEntries) != 0 {
		t.Errorf("expected 0 index entries for empty skiplist, got %d", len(reopened.indexEntries))
	}
}

// END AI SECTION
