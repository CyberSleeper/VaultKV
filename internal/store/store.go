package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"sync"
	"time"
)

const memTableSizeThreshold = 4 * 1024 // 4MB

var validNodeID = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

type Engine interface {
	Set(key, value string) error
	Get(key string) (string, bool)
}

type flushTask struct {
	data    *Skiplist
	wal     *WAL
	sstName string
}

type Store struct {
	dir         string
	nodeId      string
	data        *Skiplist
	frozenMemTs []*Skiplist
	sstables    []*SSTable
	flushChan   chan *flushTask
	mu          sync.RWMutex
	wal         *WAL
	flushWg     sync.WaitGroup
	isClosed    bool
	OnFlushErr  func(error) // Callback for background flush errors
}

func NewStore(dir, nodeID string) (*Store, error) {
	if !validNodeID.MatchString(nodeID) {
		return nil, fmt.Errorf("invalid nodeID: %q", nodeID)
	}

	data := NewSkiplist()

	// Load existing SSTs
	sstPattern := filepath.Join(dir, fmt.Sprintf("vault_%s_*.sst", nodeID))
	sstFiles, err := filepath.Glob(sstPattern)
	if err != nil {
		return nil, fmt.Errorf("failed to scan for old SSTs: %w", err)
	}

	existingSstables := make([]*SSTable, len(sstFiles))

	sort.Strings(sstFiles)

	for i, file := range sstFiles {
		curSst, err := NewSSTable(file)
		if err != nil {
			return nil, fmt.Errorf("failed to open old SST %s: %w", file, err)
		}

		if err := curSst.LoadIndexBlock(); err != nil {
			curSst.Close()
			// Cleanup previously opened files to prevent FD leaks
			for j := range i {
				existingSstables[j].Close()
			}
			return nil, fmt.Errorf("corrupted SST detected in %s: %w", file, err)
		}

		existingSstables[i] = curSst
	}

	// Load data from the existing WALs
	walPattern := filepath.Join(dir, fmt.Sprintf("vault_%s_*.wal", nodeID))
	walFiles, err := filepath.Glob(walPattern)
	if err != nil {
		for _, sst := range existingSstables {
			sst.Close()
		}
		return nil, fmt.Errorf("failed to scan for old WALs: %w", err)
	}

	sort.Strings(walFiles)

	// Note: Currently if there exist several frozen WALs, they all gonna be
	// dumped into a single active MemTable. Ex: if there are three 4MB WALs,
	// the MemT will be 12MB. As per 30 April 2026 this is perfectly fine as
	// it will just trigger the IsFull() == true then get flushed as a massive
	// 12MB block. But still, we need to be cautious and maintain carefully

	for _, file := range walFiles {
		oldWal, err := NewWAL(file)
		if err != nil {
			for _, sst := range existingSstables {
				sst.Close()
			}
			return nil, fmt.Errorf("failed to open old WAL %s: %w", file, err)
		}

		entries, err := oldWal.ReadAll()
		if err != nil {
			oldWal.Close()
			for _, sst := range existingSstables {
				sst.Close()
			}
			return nil, fmt.Errorf("corrupted WAL detected in %s: %w", file, err)
		}

		for _, v := range entries {
			if v.Value == tombstone {
				data.Delete(v.Key)
			} else {
				data.Set(v.Key, v.Value)
			}
		}
		oldWal.Close()
	}

	newWalName := fmt.Sprintf("vault_%s_%d.wal", nodeID, time.Now().UnixNano())

	wal, err := NewWAL(filepath.Join(dir, newWalName))
	if err != nil {
		return nil, fmt.Errorf("initializing WAL: %w", err)
	}

	storeObj := &Store{
		dir:         dir,
		nodeId:      nodeID,
		data:        data,
		frozenMemTs: make([]*Skiplist, 0),
		sstables:    existingSstables,
		flushChan:   make(chan *flushTask, 10),
		wal:         wal,
	}

	// TODO: implement graceful shutdown
	storeObj.flushWg.Add(1)
	go storeObj.flushWorker()

	return storeObj, nil
}

func (s *Store) Set(key, value string) error {
	if s.wal == nil {
		return errors.New("WAL is not initialized")
	}

	s.mu.Lock()

	err := s.wal.Append(&LogEntry{
		Key:   key,
		Value: value,
	})
	if err != nil {
		s.mu.Unlock()
		return err
	}

	s.data.Set(key, value)

	var task *flushTask
	if s.data.IsFull() {
		task, err = s.flushMemTable()
		if err != nil {
			s.mu.Unlock()
			return err
		}
	}
	s.mu.Unlock()

	// Push to the background worker strictly OUTSIDE the lock
	// This prevents a Channel-Mutex Deadlock if the flushChan is full
	if task != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if fmt.Sprint(r) == "send on closed channel" {
						_ = task.wal.Close()
						return
					}
					panic(r)
				}
			}()
			s.flushChan <- task
		}()
	}

	return nil
}

func (s *Store) Get(targetKey string) (string, bool) {
	val, found := s.getInternal(targetKey)

	if found && val == tombstone {
		return "", false
	}

	return val, found
}

func (s *Store) getInternal(targetKey string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// TODO: query in this exact order:
	// 1. Active MemTable
	// 2. Frozen MemTables in order from newest to oldest
	// 3. SSTables on disk in order from newest to oldest

	// 1. Check Active MemTable
	if val, exists := s.data.Get(targetKey); exists {
		return val, true
	}

	// 2. Check Frozen MemTables (newest to oldest)
	for i := len(s.frozenMemTs) - 1; i >= 0; i-- {
		if val, exists := s.frozenMemTs[i].Get(targetKey); exists {
			return val, true
		}
	}

	// 3. SSTables (newest to oldest)
	for i := len(s.sstables) - 1; i >= 0; i-- {
		curSst := s.sstables[i]
		targetBytes := []byte(targetKey)
		idx, found := slices.BinarySearchFunc(curSst.indexEntries, targetBytes, func(entry *IndexBlockEntry, target []byte) int {
			return bytes.Compare(entry.keyBytes, target)
		})

		if found {
			exactPtr := curSst.indexEntries[idx].ptr
			keyLen := len(targetKey)

			// Note: Do not use fd.Seek as it will change the global fd, which will ofc
			// mess with the concurrency as we are using Rlock here. Use ReadAt instead
			// The implementation at 05 May 2026 is already tested and checked thoroughtly
			// So supposedly it is already correct UwU (hopefully)

			// Trivia: ReadAt maps to pread in Unix system, so it is atomic read

			// exactPtr points to the beginning of the record (the 2-byte keyLen).
			// The 4-byte valLen is located right after keyLen (exactPtr + 2).
			valLenBytes := make([]byte, 4)
			if _, err := curSst.fd.ReadAt(valLenBytes, int64(exactPtr+2)); err != nil {
				return "", false
			}
			valLen := binary.LittleEndian.Uint32(valLenBytes)

			// The actual Value bytes are located after keyLen, valLen, and the Key string.
			// Offset = exactPtr + 2 (keyLen) + 4 (valLen) + keyLen (the actual key bytes)
			valOffset := int64(exactPtr) + 2 + 4 + int64(keyLen)

			valBytes := make([]byte, valLen)
			if _, err := curSst.fd.ReadAt(valBytes, valOffset); err != nil {
				return "", false
			}

			return string(valBytes), true
		}
	}

	return "", false
}

func (s *Store) Delete(key string) error {
	// A Delete is just a Set with the TOMBSTONE value
	return s.Set(key, tombstone)
}

func (s *Store) flushMemTable() (*flushTask, error) {

	timestamp := time.Now().UnixNano()
	newWalName := fmt.Sprintf("vault_%s_%d.wal", s.nodeId, timestamp)
	newSstName := fmt.Sprintf("vault_%s_%d.sst", s.nodeId, timestamp)

	newWal, err := NewWAL(filepath.Join(s.dir, newWalName))
	if err != nil {
		return nil, fmt.Errorf("failed to create new WAL: %w", err)
	}

	frozenData := s.data
	frozenWal := s.wal

	// Freeze the memtable so it can still be queried during the flush
	s.frozenMemTs = append(s.frozenMemTs, frozenData)
	s.data = NewSkiplist()
	s.wal = newWal

	return &flushTask{
		data:    frozenData,
		wal:     frozenWal,
		sstName: newSstName,
	}, nil
}

func (s *Store) flushWorker() {
	// This signals the wg that this function has done its job
	defer s.flushWg.Done()

	for task := range s.flushChan {
		handleErr := func(err error) {
			if s.OnFlushErr != nil {
				s.OnFlushErr(err)
			} else {
				fmt.Printf("Background flush error: %v\n", err)
			}
		}

		var newSst *SSTable
		var err error

		// We cannot use 'continue' to skip a failed task because it will permanently breaks the
		// FIFO 1:1 alignment with 's.frozenMemTs', causing the wrong MemTable to be deleted from RAM
		// If a disk failure occurs, our current solution is to retry until it succeeds to prevent silent data loss

		// U might think ofa solution like DLQ, but then we need to think on how to scan through this
		// DLQ for the GET query. It will be an anti-pattern to manage this as it's no differ than another Frozen MemTs

		// RocksDB use "Wrtie Stalls" and "ReadOnly Mode" btw, so the current implementation of infinite retry is basically
		// the step 1 of the canon sol

		for {
			// Cleanup any partial file from previous attempt
			_ = os.Remove(filepath.Join(s.dir, task.sstName))

			newSst, err = NewSSTable(filepath.Join(s.dir, task.sstName))
			if err != nil {
				handleErr(fmt.Errorf("failed to create new SSTable: %w", err))
				time.Sleep(1 * time.Second)
				continue
			}

			err = newSst.Flush(task.data)
			if err != nil {
				newSst.Close()
				handleErr(fmt.Errorf("failed to flush SSTable: %w", err))
				time.Sleep(1 * time.Second)
				continue
			}

			if err := newSst.LoadIndexBlock(); err != nil {
				newSst.Close()
				handleErr(fmt.Errorf("failed to load index block for new SSTable: %w", err))
				time.Sleep(1 * time.Second)
				continue
			}

			// If we get here, the flush was completely successful!
			break
		}

		if err := task.wal.Delete(); err != nil {
			handleErr(fmt.Errorf("failed to delete obsolete WAL: %w", err))
		}

		// Success, then remove the flushed Skiplist from the front of frozenMemTs
		// We must lock here as we are about to "write" (deleting a Frozen MemT)
		s.mu.Lock()
		s.frozenMemTs[0] = nil // Avoid memory leak, tell GC to sweep it
		s.frozenMemTs = s.frozenMemTs[1:]
		s.sstables = append(s.sstables, newSst)
		s.mu.Unlock()
	}
}

func (s *Store) Close() error {
	s.mu.Lock()
	if s.isClosed {
		s.mu.Unlock()
		return nil
	}
	s.isClosed = true
	s.mu.Unlock()

	close(s.flushChan)

	s.flushWg.Wait()

	s.wal.Close()
	for _, sst := range s.sstables {
		if err := sst.Close(); err != nil {
			return fmt.Errorf("failed to close SSTable: %w", err)
		}
	}

	return nil
}
