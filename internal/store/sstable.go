package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sort"
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
	// indexOffset is the byte offset where the index region begins, i.e. the
	// end of the last data block. Needed to compute the boundary of the final
	// data block during a point lookup. Populated by LoadIndexBlock.
	indexOffset uint32
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

// indexBlockChecksum computes the CRC over the sparse index region together
// with the indexOffset footer field. Folding indexOffset in makes its
// integrity explicit: a corrupted offset changes the checksum input and is
// rejected on read, rather than relying only on the offset happening to shift
// the checksummed byte range. magicNumber is omitted because it is a known
// constant validated by direct equality.
func indexBlockChecksum(indexData []byte, indexOffset uint32) uint32 {
	buf := make([]byte, len(indexData)+4)
	copy(buf, indexData)
	binary.LittleEndian.PutUint32(buf[len(indexData):], indexOffset)
	return crc32.ChecksumIEEE(buf)
}

func (e *SSTEntry) Encode(w io.Writer) error {
	var indexSlice []indexEntry
	var currentBlockBuf bytes.Buffer
	var fileOffset uint32

	targetBlockSize := 4096 // 4KB target block size

	// Keep track of the first key in the current block for the sparse index
	var firstKeyInBlock string
	var entriesInBlock uint16

	for _, v := range e.LogEntries {
		if len(v.Key) > math.MaxUint16 {
			return fmt.Errorf("key too long: %d bytes, max %d", len(v.Key), math.MaxUint16)
		}
		if len(v.Value) > math.MaxUint32 {
			return fmt.Errorf("value too long: %d bytes, max %d", len(v.Value), math.MaxUint32)
		}

		if entriesInBlock == 0 {
			firstKeyInBlock = v.Key
		}

		keyLen := uint16(len(v.Key))
		valLen := uint32(len(v.Value))

		// Temporarily write the entry to the current block buffer
		if err := binary.Write(&currentBlockBuf, binary.LittleEndian, keyLen); err != nil {
			return err
		}
		if err := binary.Write(&currentBlockBuf, binary.LittleEndian, valLen); err != nil {
			return err
		}
		if _, err := currentBlockBuf.Write([]byte(v.Key)); err != nil {
			return err
		}
		if _, err := currentBlockBuf.Write([]byte(v.Value)); err != nil {
			return err
		}

		entriesInBlock++

		// If the block has reached the target size, flush it to disk
		if currentBlockBuf.Len() >= targetBlockSize {
			if err := flushBlock(&currentBlockBuf, w, &fileOffset, &indexSlice, firstKeyInBlock, entriesInBlock); err != nil {
				return err
			}
			entriesInBlock = 0
		}
	}

	// Flush any remaining entries in the last block
	if currentBlockBuf.Len() > 0 {
		if err := flushBlock(&currentBlockBuf, w, &fileOffset, &indexSlice, firstKeyInBlock, entriesInBlock); err != nil {
			return err
		}
	}

	// The Trailer Index Block starts here
	indexOffset := fileOffset

	var indexBuf bytes.Buffer

	// Write the number of index entries
	numIndexEntries := uint32(len(indexSlice))
	if err := binary.Write(&indexBuf, binary.LittleEndian, numIndexEntries); err != nil {
		return err
	}

	// Write Index Block (Sparse Index)
	for _, v := range indexSlice {
		if err := binary.Write(&indexBuf, binary.LittleEndian, v.pointer); err != nil {
			return err
		}
		if err := binary.Write(&indexBuf, binary.LittleEndian, v.keyLen); err != nil {
			return err
		}
		if _, err := indexBuf.Write([]byte(v.key)); err != nil {
			return err
		}
	}

	indexData := indexBuf.Bytes()
	indexChecksum := indexBlockChecksum(indexData, indexOffset)

	// Write Index Data
	if _, err := w.Write(indexData); err != nil {
		return err
	}

	// Write Index Checksum
	if err := binary.Write(w, binary.LittleEndian, indexChecksum); err != nil {
		return err
	}

	// Write Footer (IndexOffset, MagicNumber)
	if err := binary.Write(w, binary.LittleEndian, indexOffset); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, magicNumber); err != nil {
		return err
	}

	return nil
}

func flushBlock(buf *bytes.Buffer, w io.Writer, fileOffset *uint32, indexSlice *[]indexEntry, firstKey string, numEntries uint16) error {
	blockData := buf.Bytes()

	// Calculate block checksum (covering numEntries + data)
	// We prepend numEntries to the buffer logically for checksum, but write it separately
	var checksumBuf bytes.Buffer
	binary.Write(&checksumBuf, binary.LittleEndian, numEntries)
	checksumBuf.Write(blockData)
	checksum := crc32.ChecksumIEEE(checksumBuf.Bytes())

	// Write numEntries, Data, and Checksum to disk
	if err := binary.Write(w, binary.LittleEndian, numEntries); err != nil {
		return err
	}
	if _, err := w.Write(blockData); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, checksum); err != nil {
		return err
	}

	// Add sparse index entry pointing to the START of this block
	*indexSlice = append(*indexSlice, indexEntry{
		pointer: *fileOffset,
		keyLen:  uint16(len(firstKey)),
		key:     firstKey,
	})

	// Advance fileOffset: 2 bytes (numEntries) + len(data) + 4 bytes (checksum)
	*fileOffset += 2 + uint32(len(blockData)) + 4

	// Reset buffer for the next block
	buf.Reset()
	return nil
}

// Decode reads an entire SST stream and reconstructs every LogEntry in order.
// It verifies the index checksum and each per-block checksum along the way.
// Intended for whole-file consumers such as compaction's K-way merge.
func (e *SSTEntry) Decode(r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if len(data) < 12 {
		return fmt.Errorf("sst too small to contain footer: %d bytes", len(data))
	}

	footerStart := len(data) - 12
	indexChecksum := binary.LittleEndian.Uint32(data[footerStart : footerStart+4])
	indexOffset := binary.LittleEndian.Uint32(data[footerStart+4 : footerStart+8])
	magic := binary.LittleEndian.Uint32(data[footerStart+8 : footerStart+12])

	if magic != magicNumber {
		return fmt.Errorf("invalid magic number: got 0x%X, expected 0x%X", magic, magicNumber)
	}
	if int(indexOffset) > footerStart {
		return fmt.Errorf("invalid index offset %d exceeds footer position %d", indexOffset, footerStart)
	}

	if indexBlockChecksum(data[indexOffset:footerStart], indexOffset) != indexChecksum {
		return fmt.Errorf("corrupted SST index block: checksum mismatch")
	}

	// Walk every data block in [0, indexOffset).
	// Block layout: [numEntries(2)][entries...][checksum(4)]
	var logEntries []*LogEntry
	cursor := 0
	for cursor < int(indexOffset) {
		blockStart := cursor
		if cursor+2 > int(indexOffset) {
			return fmt.Errorf("malformed block header at offset %d", cursor)
		}
		numEntries := binary.LittleEndian.Uint16(data[cursor : cursor+2])
		cursor += 2

		for range numEntries {
			if cursor+6 > len(data) {
				return fmt.Errorf("malformed entry header at offset %d", cursor)
			}
			keyLen := int(binary.LittleEndian.Uint16(data[cursor : cursor+2]))
			valLen := int(binary.LittleEndian.Uint32(data[cursor+2 : cursor+6]))
			cursor += 6

			if cursor+keyLen+valLen > len(data) {
				return fmt.Errorf("malformed entry body at offset %d", cursor)
			}
			key := string(data[cursor : cursor+keyLen])
			cursor += keyLen
			val := string(data[cursor : cursor+valLen])
			cursor += valLen

			logEntries = append(logEntries, &LogEntry{Key: key, Value: val})
		}

		if cursor+4 > len(data) {
			return fmt.Errorf("missing block checksum at offset %d", cursor)
		}
		storedChecksum := binary.LittleEndian.Uint32(data[cursor : cursor+4])
		if crc32.ChecksumIEEE(data[blockStart:cursor]) != storedChecksum {
			return fmt.Errorf("corrupted SST data block at offset %d: checksum mismatch", blockStart)
		}
		cursor += 4
	}

	e.LogEntries = logEntries

	return nil
}

func (e *SST) LoadIndexBlock() error {
	// The file ends with a 12-byte footer:
	//   [indexChecksum(4)][indexOffset(4)][magicNumber(4)]
	// The index region itself spans [indexOffset, footerStart) and is laid out:
	//   [numIndexEntries(4)] [ (ptr(4) keyLen(2) key) ... ]
	fd := e.fd

	info, err := fd.Stat()
	if err != nil {
		return err
	}
	fileSize := info.Size()
	if fileSize < 12 {
		return fmt.Errorf("sst too small to contain footer: %d bytes", fileSize)
	}
	footerStart := fileSize - 12

	footer := make([]byte, 12)
	if _, err := fd.ReadAt(footer, footerStart); err != nil {
		return err
	}

	indexChecksum := binary.LittleEndian.Uint32(footer[0:4])
	indexOffset := binary.LittleEndian.Uint32(footer[4:8])
	magic := binary.LittleEndian.Uint32(footer[8:12])

	if magic != magicNumber {
		return fmt.Errorf("invalid magic number: got 0x%X, expected 0x%X", magic, magicNumber)
	}

	indexSize := footerStart - int64(indexOffset)
	if indexSize < 0 {
		return fmt.Errorf("invalid index offset %d exceeds footer position %d", indexOffset, footerStart)
	}

	indexData := make([]byte, indexSize)
	if _, err := fd.ReadAt(indexData, int64(indexOffset)); err != nil {
		return err
	}

	if indexBlockChecksum(indexData, indexOffset) != indexChecksum {
		return fmt.Errorf("corrupted SST index block in %s: checksum mismatch", e.filename)
	}

	reader := bytes.NewReader(indexData)

	var numIndexEntries uint32
	if err := binary.Read(reader, binary.LittleEndian, &numIndexEntries); err != nil {
		return err
	}

	indices := make([]*IndexBlockEntry, 0, numIndexEntries)
	for i := uint32(0); i < numIndexEntries; i++ {
		var entry IndexBlockEntry
		if err := binary.Read(reader, binary.LittleEndian, &entry.Ptr); err != nil {
			return err
		}

		var keyLen uint16
		if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
			return err
		}

		entry.KeyBytes = make([]byte, keyLen)
		if _, err := io.ReadFull(reader, entry.KeyBytes); err != nil {
			return err
		}

		indices = append(indices, &entry)
	}

	e.indexEntries = indices
	e.indexOffset = indexOffset

	return nil
}

// lookup finds targetKey within this SST using the sparse block index.
//
// The index stores the FIRST key of each ~4KB data block, so we binser
// for the block whose key range could contain targetKey, read that single
// block, verify its CRC, and scan it. Returns (value, true, nil) on a hit,
// ("", false, nil) if the key is not present, or a non-nil error if the block
// could not be read or failed its checksum.
func (s *SST) lookup(targetKey string) (string, bool, error) {
	if len(s.indexEntries) == 0 {
		return "", false, nil
	}

	target := []byte(targetKey)

	// First index entry whose first key is strictly greater than target, the
	// candidate block is the one immediately before it.
	hi := sort.Search(len(s.indexEntries), func(i int) bool {
		return bytes.Compare(s.indexEntries[i].KeyBytes, target) > 0
	})
	if hi == 0 {
		// target sorts before the first key of the first block
		return "", false, nil
	}
	blockIdx := hi - 1

	blockStart := s.indexEntries[blockIdx].Ptr
	var blockEnd uint32
	if blockIdx+1 < len(s.indexEntries) {
		blockEnd = s.indexEntries[blockIdx+1].Ptr
	} else {
		blockEnd = s.indexOffset
	}
	if blockEnd <= blockStart {
		return "", false, fmt.Errorf("invalid block boundaries: start=%d end=%d", blockStart, blockEnd)
	}

	// Block layout on disk: [numEntries(2)][entries...][checksum(4)]
	block := make([]byte, blockEnd-blockStart)
	if _, err := s.fd.ReadAt(block, int64(blockStart)); err != nil {
		return "", false, err
	}
	if len(block) < 6 {
		return "", false, fmt.Errorf("sst block at offset %d too small: %d bytes", blockStart, len(block))
	}

	payload := block[:len(block)-4] // numEntries + entries (checksum input)
	storedChecksum := binary.LittleEndian.Uint32(block[len(block)-4:])
	if crc32.ChecksumIEEE(payload) != storedChecksum {
		return "", false, fmt.Errorf("corrupted SST data block at offset %d: checksum mismatch", blockStart)
	}

	numEntries := binary.LittleEndian.Uint16(payload[0:2])
	cursor := 2
	for range numEntries {
		if cursor+6 > len(payload) {
			return "", false, fmt.Errorf("malformed entry header in block at offset %d", blockStart)
		}
		keyLen := int(binary.LittleEndian.Uint16(payload[cursor : cursor+2]))
		valLen := int(binary.LittleEndian.Uint32(payload[cursor+2 : cursor+6]))
		cursor += 6

		if cursor+keyLen+valLen > len(payload) {
			return "", false, fmt.Errorf("malformed entry body in block at offset %d", blockStart)
		}
		key := payload[cursor : cursor+keyLen]
		cursor += keyLen
		val := payload[cursor : cursor+valLen]
		cursor += valLen

		switch bytes.Compare(key, target) {
		case 0:
			return string(val), true, nil
		case 1:
			// entries are sorted; we've passed where target would be
			return "", false, nil
		}
	}

	return "", false, nil
}
