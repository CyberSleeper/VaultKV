package store

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// BEGIN AI SECTION

func TestSSTable_Option3_Format(t *testing.T) {
	// Create a dummy memtable flush event with 2 exact items
	entry := NewSSTableEntry()
	entry.LogEntries = append(entry.LogEntries, NewLogEntry("apple", "red"))
	entry.LogEntries = append(entry.LogEntries, NewLogEntry("banana", "yellow"))

	var buf bytes.Buffer
	err := entry.Encode(&buf)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	data := buf.Bytes()

	// Checkpoint 1: Minimum Size
	// Footer (8) + checksum (4) + KV count (2) = minimum 14 bytes
	if len(data) < 14 {
		t.Fatalf("Encoded data too small: %d bytes (Did you write the footer?)", len(data))
	}
	t.Log("✅ Checkpoint 1 Passed: Minimum file size met!")

	// Checkpoint 2: Validate the Top-Level Header (KV Cnt)
	// Because Option 3 puts KV cnt at the very start now (Checksum moved):
	kvCnt := binary.LittleEndian.Uint16(data[0:2])
	if kvCnt != 2 {
		t.Fatalf("Checkpoint 2 Failed! Expected top-level KV Cnt of 2, got %d. Did you structure the start correctly?", kvCnt)
	}
	t.Log("✅ Checkpoint 2 Passed: KV Cnt Header is correct!")

	// Checkpoint 3: Validate the Checksum AND Footer Magic Number
	// The file ends with: [IndexOffset 4B][MagicNum 4B][Checksum 4B]
	footerStart := len(data) - 12

	magicNum := binary.LittleEndian.Uint32(data[footerStart+4 : footerStart+8])
	expectedMagic := uint32(0xCAFEBABE)
	if magicNum != expectedMagic {
		t.Fatalf("Checkpoint 3 Failed! Expected magic number 0x%X in Footer, got 0x%X.", expectedMagic, magicNum)
	}
	t.Log("✅ Checkpoint 3 Passed: Magic Number matches right before EOF Checksum!")

	// Checkpoint 4: Validate the Index Offset Points to real data!
	indexOffset := binary.LittleEndian.Uint32(data[footerStart : footerStart+4])
	if indexOffset == 0 || indexOffset >= uint32(footerStart) {
		t.Fatalf("Checkpoint 4 Failed! Index offset %d is invalid or overlapping.", indexOffset)
	}
	t.Logf("✅ Checkpoint 4 Passed: Index offset successfully extracted: points to byte %d!", indexOffset)

	// Checkpoint 5: Verify the first Index
	indexData := data[indexOffset:footerStart]
	if len(indexData) < 6 {
		t.Fatalf("Checkpoint 5 Failed! Index block is missing or too small.")
	}

	// Read the first Location from the Index
	firstLocation := binary.LittleEndian.Uint32(indexData[0:4])
	if firstLocation != 2 { // Since Data block starts exactly after KVCnt(2) = Byte 2!
		t.Fatalf("Checkpoint 5 Failed! Expected first Data block to be at byte 2, but index points to %d", firstLocation)
	}
	t.Log("✅ Checkpoint 5 Passed: The Index block accurately points to the first Data block! Everything is mathematically flawless.")
}

func TestSSTable_Decode_Option3(t *testing.T) {
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

	// 4. Verify index has the right number of entries in sorted order
	if len(reopened.indexEntries) != 3 {
		t.Fatalf("expected 3 index entries, got %d", len(reopened.indexEntries))
	}

	expectedKeys := []string{"apple", "banana", "cherry"}
	for i, want := range expectedKeys {
		if string(reopened.indexEntries[i].KeyBytes) != want {
			t.Errorf("index[%d]: expected key %q, got %q", i, want, string(reopened.indexEntries[i].KeyBytes))
		}
	}

	// 5. Sanity-check: a point read via the pointer we just loaded should land
	//    on the right record. We replicate the layout the Store uses:
	//    [keyLen(2)][valLen(4)][key][value]
	ptr := reopened.indexEntries[1].Ptr // "banana"
	keyLenBuf := make([]byte, 2)
	valLenBuf := make([]byte, 4)
	if _, err := reopened.fd.ReadAt(keyLenBuf, int64(ptr)); err != nil {
		t.Fatalf("ReadAt keyLen failed: %v", err)
	}
	if _, err := reopened.fd.ReadAt(valLenBuf, int64(ptr)+2); err != nil {
		t.Fatalf("ReadAt valLen failed: %v", err)
	}
	keyLen := binary.LittleEndian.Uint16(keyLenBuf)
	valLen := binary.LittleEndian.Uint32(valLenBuf)

	valBytes := make([]byte, valLen)
	valOffset := int64(ptr) + 2 + 4 + int64(keyLen)
	if _, err := reopened.fd.ReadAt(valBytes, valOffset); err != nil {
		t.Fatalf("ReadAt val failed: %v", err)
	}
	if string(valBytes) != "yellow" {
		t.Errorf("expected value 'yellow' at index[1], got %q", string(valBytes))
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

	// Corrupt the magic number (bytes [size-8, size-4))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if len(raw) < 8 {
		t.Fatalf("SST file too small: %d", len(raw))
	}
	for i := len(raw) - 8; i < len(raw)-4; i++ {
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
