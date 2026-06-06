package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
)

const maxEntryCntBytes = math.MaxUint16

const magicNumber uint32 = 0xCAFEBABE

type SSTLevel struct {
	LevelNum int
	Tables   []*SST
}

// SST represents a single .sst file
type SST struct {
	fd           *os.File
	filename     string
	indexEntries []*IndexBlockEntry
}

type SSTEntry struct {
	LogEntries []*LogEntry
}

type IndexBlockEntry struct {
	Ptr      uint32
	KeyBytes []byte
}

func closeAllSSTables(levels []*SSTLevel) {
	for _, lvl := range levels {
		if lvl != nil {
			lvl.Close()
		}
	}
}

func (s *SSTLevel) Close() error {
	var firstErr error
	for _, v := range s.Tables {
		if v != nil {
			if err := v.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (s *SST) Close() error {
	var firstErr error

	if s.fd != nil {
		if err := s.fd.Close(); err != nil {
			firstErr = err
		}
	}
	return firstErr
}

type indexEntry struct {
	pointer uint32
	keyLen  uint16
	key     string
}

func NewSST(path string) (*SST, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	sstable := &SST{
		fd:           file,
		filename:     path,
		indexEntries: make([]*IndexBlockEntry, 0),
	}
	return sstable, nil
}

func NewSSTableEntry() *SSTEntry {
	return &SSTEntry{
		LogEntries: make([]*LogEntry, 0),
	}
}

func (s *SST) MergeSkiplist(skiplist *Skiplist) *SSTEntry {
	sstableEntry := NewSSTableEntry()

	// Skip the dummy header node and start at the first real data node
	curNode := skiplist.BeginNode.Next[0]

	for curNode != nil {
		sstableEntry.LogEntries = append(sstableEntry.LogEntries, NewLogEntry(curNode.Key, curNode.Value))
		curNode = curNode.Next[0]
	}

	return sstableEntry
}

func (s *SST) Append(entry *SSTEntry) error {
	if entry == nil {
		return fmt.Errorf("nil sst entry")
	}
	if s.fd == nil {
		return fmt.Errorf("sst is nil")
	}

	if err := entry.Encode(s.fd); err != nil {
		return err
	}
	if err := s.fd.Sync(); err != nil {
		return err
	}

	return nil
}

func (s *SST) Flush(skiplist *Skiplist) error {
	sstEntry := s.MergeSkiplist(skiplist)

	if err := s.Append(sstEntry); err != nil {
		return err
	}
	return nil
}

func (e *SSTEntry) Encode(w io.Writer) error {
	if len(e.LogEntries) > int(maxEntryCntBytes) {
		return fmt.Errorf("entry length too large: %d, limit=%d", len(e.LogEntries), maxEntryCntBytes)
	}

	var dataLen uint16
	dataLen = uint16(len(e.LogEntries))

	var buf bytes.Buffer

	if err := binary.Write(&buf, binary.LittleEndian, dataLen); err != nil {
		return err
	}

	indexSlice := make([]indexEntry, dataLen)

	for i, v := range e.LogEntries {
		if len(v.Key) > math.MaxUint16 {
			return fmt.Errorf("key too long: %d bytes, max %d", len(v.Key), math.MaxUint16)
		}
		if len(v.Value) > math.MaxUint32 {
			return fmt.Errorf("value too long: %d bytes, max %d", len(v.Value), math.MaxUint32)
		}
		keyLen := uint16(len(v.Key))
		valLen := uint32(len(v.Value))

		indexSlice[i] = indexEntry{
			pointer: uint32(buf.Len()),
			keyLen:  keyLen,
			key:     v.Key,
		}

		if err := binary.Write(&buf, binary.LittleEndian, keyLen); err != nil {
			return err
		}
		if err := binary.Write(&buf, binary.LittleEndian, valLen); err != nil {
			return err
		}
		if _, err := buf.Write([]byte(v.Key)); err != nil {
			return err
		}
		if _, err := buf.Write([]byte(v.Value)); err != nil {
			return err
		}
	}

	// The Trailer Index Block starts from buf.Len()
	indexOffset := uint32(buf.Len())

	// Index Block
	for _, v := range indexSlice {
		if err := binary.Write(&buf, binary.LittleEndian, v.pointer); err != nil {
			return err
		}
		if err := binary.Write(&buf, binary.LittleEndian, v.keyLen); err != nil {
			return err
		}
		if _, err := buf.Write([]byte(v.key)); err != nil {
			return err
		}
	}

	// Write Footer directly into buf!
	if err := binary.Write(&buf, binary.LittleEndian, indexOffset); err != nil {
		return err
	}
	if err := binary.Write(&buf, binary.LittleEndian, magicNumber); err != nil {
		return err
	}

	data := buf.Bytes()

	// TODO(Future Improvement):
	// Currently, this writes a single Master Checksum over the entire SSTable file at the EOF
	// For production-grade durability against single-bit flips, consider rotating to
	// Block-Based Checksums (calculating and inserting a checksum every 4KB of data chunked)
	// to prevent a 1-byte corruption from invalidating the entire file
	checksum := crc32.ChecksumIEEE(data)

	// Write everything to disk, and Checksum at the absolute end
	if _, err := w.Write(data); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, checksum); err != nil {
		return err
	}

	return nil
}

func (e *SSTEntry) Decode(r io.Reader) error {
	var dataLen uint16

	var buf bytes.Buffer

	if err := binary.Read(r, binary.LittleEndian, &dataLen); err != nil {
		return err
	}

	if err := binary.Write(&buf, binary.LittleEndian, dataLen); err != nil {
		return err
	}

	logEntries := make([]*LogEntry, dataLen)

	for i := range dataLen {
		var keyLen uint16
		var valLen uint32

		if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
			return err
		}
		if err := binary.Read(r, binary.LittleEndian, &valLen); err != nil {
			return err
		}

		keyBytes := make([]byte, keyLen)
		valBytes := make([]byte, valLen)

		if _, err := io.ReadFull(r, keyBytes); err != nil {
			return err
		}
		if _, err := io.ReadFull(r, valBytes); err != nil {
			return err
		}

		// Wrtie to buf for the checksum later
		if err := binary.Write(&buf, binary.LittleEndian, keyLen); err != nil {
			return err
		}
		if err := binary.Write(&buf, binary.LittleEndian, valLen); err != nil {
			return err
		}
		if _, err := buf.Write(keyBytes); err != nil {
			return err
		}
		if _, err := buf.Write(valBytes); err != nil {
			return err
		}

		logEntries[i] = &LogEntry{
			Key:   string(keyBytes),
			Value: string(valBytes),
		}
	}

	for range dataLen {
		var curIndex indexEntry

		if err := binary.Read(r, binary.LittleEndian, &curIndex.pointer); err != nil {
			return err
		}
		if err := binary.Read(r, binary.LittleEndian, &curIndex.keyLen); err != nil {
			return err
		}

		keyBytes := make([]byte, curIndex.keyLen)
		if _, err := io.ReadFull(r, keyBytes); err != nil {
			return err
		}

		if err := binary.Write(&buf, binary.LittleEndian, curIndex.pointer); err != nil {
			return err
		}
		if err := binary.Write(&buf, binary.LittleEndian, curIndex.keyLen); err != nil {
			return err
		}
		if _, err := buf.Write(keyBytes); err != nil {
			return err
		}
	}

	var curIndexOffset uint32
	var curMagicNumber uint32

	if err := binary.Read(r, binary.LittleEndian, &curIndexOffset); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &curMagicNumber); err != nil {
		return err
	}

	if curMagicNumber != magicNumber {
		return fmt.Errorf("invalid magic number: got 0x%X, expected 0x%X", curMagicNumber, magicNumber)
	}

	if err := binary.Write(&buf, binary.LittleEndian, curIndexOffset); err != nil {
		return err
	}
	if err := binary.Write(&buf, binary.LittleEndian, curMagicNumber); err != nil {
		return err
	}

	var checksum uint32
	if err := binary.Read(r, binary.LittleEndian, &checksum); err != nil {
		return err
	}

	if crc32.ChecksumIEEE(buf.Bytes()) != checksum {
		return fmt.Errorf("corrupted SST entry: expected checksum %d", checksum)
	}

	e.LogEntries = logEntries

	return nil
}

func (e *SST) LoadIndexBlock() error {
	// 1. Jump straight to the Footer (last 12 bytes of the file)
	// (IndexOffset: 4 bytes, MagicNumber: 4 bytes, Checksum: 4 bytes)

	fd := e.fd

	footerStart, err := fd.Seek(-12, io.SeekEnd)
	if err != nil {
		return err
	}

	var indexOffset uint32
	var magic uint32
	var checksum uint32

	if err := binary.Read(fd, binary.LittleEndian, &indexOffset); err != nil {
		return err
	}
	if err := binary.Read(fd, binary.LittleEndian, &magic); err != nil {
		return err
	}

	// Currently we don't check the data integrity as it would take a lot of time to validate a whole SST file chunks
	// Instead of having a single checksum per file, it is better to have a checksum per small chunk (ex. 4KB)
	// But yes, let's put this as TODO for now since it is quite high effort and deserves its own issue
	if err := binary.Read(fd, binary.LittleEndian, &checksum); err != nil {
		return err
	}

	if magic != magicNumber {
		return fmt.Errorf("invalid magic number: got 0x%X, expected 0x%X", magic, magicNumber)
	}

	// 2. Jump to the exact byte where the Index Block starts
	if _, err := fd.Seek(int64(indexOffset), io.SeekStart); err != nil {
		return err
	}

	// 3. Read everything between IndexOffset and FooterStart
	indexSize := footerStart - int64(indexOffset)
	if indexSize < 0 {
		return fmt.Errorf("invalid index offset %d exceeds footer position %d", indexOffset, footerStart)
	}
	limitReader := io.LimitReader(fd, indexSize)

	var indices []*IndexBlockEntry

	for {
		var entry IndexBlockEntry
		err := binary.Read(limitReader, binary.LittleEndian, &entry.Ptr)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		var keyLen uint16
		if err := binary.Read(limitReader, binary.LittleEndian, &keyLen); err != nil {
			return err
		}

		entry.KeyBytes = make([]byte, keyLen)
		if _, err := io.ReadFull(limitReader, entry.KeyBytes); err != nil {
			return err
		}

		indices = append(indices, &entry)
	}

	e.indexEntries = indices

	return nil
}
