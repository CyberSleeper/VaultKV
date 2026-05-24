package store

// NOTE: Currently I don't really handle the data corruption case. What I've implemented:
// 	     [checksum(4)][keyLen(2)][valLen(4)][key][value]
//		 Each log-block will be appended to the same single file
//
// 	     For better reliability, we can add a partition to the wal bytes instead of
// 	     just a single file. Then in each file we occupy the first 4B with a checksum
// 	     for that entire file. This approach is already implemented in Cassandra

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
)

const (
	maxWALKeyBytes   = math.MaxUint16
	maxWALValueBytes = math.MaxUint32
)

type LogEntry struct {
	Key   string
	Value string
}

// WAL represents a Write-Ahead Log.
// Note: This struct is NOT thread-safe. Callers must synchronize
// access externally (e.g., via a Mutex in the Store) to prevent
// race conditions during Append, Close, and Delete operations.
type WAL struct {
	file *os.File
}

func NewLogEntry(k, v string) *LogEntry {
	return &LogEntry{
		Key:   k,
		Value: v,
	}
}

func NewWAL(filepath string) (*WAL, error) {
	file, err := os.OpenFile(filepath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	wal := &WAL{
		file: file,
	}
	return wal, nil
}

func (w *WAL) Close() error {
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

func (w *WAL) Append(entry *LogEntry) error {
	if entry == nil {
		return fmt.Errorf("nil log entry")
	}
	if w.file == nil {
		return fmt.Errorf("wal is closed")
	}

	if err := entry.Encode(w.file); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	return nil
}

func (w *WAL) ReadAll() ([]*LogEntry, error) {
	if w.file == nil {
		return nil, fmt.Errorf("wal is closed")
	}

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var entries []*LogEntry

	for {
		entry := &LogEntry{}
		err := entry.Decode(w.file)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (w *WAL) Delete() error {
	if w.file == nil {
		return nil
	}

	filename := w.file.Name()

	if err := w.file.Close(); err != nil {
		return err
	}
	w.file = nil

	return os.Remove(filename)
}

func BatchBinaryWrite(w io.Writer, values ...any) error {
	for _, v := range values {
		if err := binary.Write(w, binary.LittleEndian, v); err != nil {
			return err
		}
	}
	return nil
}

func BatchBinaryRead(r io.Reader, values ...any) error {
	for _, v := range values {
		if err := binary.Read(r, binary.LittleEndian, v); err != nil {
			return err
		}
	}
	return nil
}

func (e *LogEntry) Encode(w io.Writer) error {
	if uint64(len(e.Key)) > maxWALKeyBytes || uint64(len(e.Value)) > maxWALValueBytes {
		return fmt.Errorf("wal entry too large: key=%d value=%d", len(e.Key), len(e.Value))
	}

	keyLen := uint16(len(e.Key))
	valLen := uint32(len(e.Value))
	var buf bytes.Buffer

	if err := BatchBinaryWrite(&buf, keyLen, valLen); err != nil {
		return err
	}

	if _, err := buf.Write([]byte(e.Key)); err != nil {
		return err
	}
	if _, err := buf.Write([]byte(e.Value)); err != nil {
		return err
	}

	data := buf.Bytes()
	checksum := crc32.ChecksumIEEE(data)

	if err := binary.Write(w, binary.LittleEndian, checksum); err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return err
	}

	return nil
}

func (e *LogEntry) Decode(r io.Reader) error {
	var checksum uint32
	var keyLen uint16
	var valLen uint32

	if err := BatchBinaryRead(r, &checksum, &keyLen, &valLen); err != nil {
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

	var buf bytes.Buffer
	if err := BatchBinaryWrite(&buf, keyLen, valLen, keyBytes, valBytes); err != nil {
		return err
	}

	if crc32.ChecksumIEEE(buf.Bytes()) != checksum {
		return fmt.Errorf("corrupted log entry: expected checksum %d", checksum)
	}

	e.Key = string(keyBytes)
	e.Value = string(valBytes)

	return nil
}
