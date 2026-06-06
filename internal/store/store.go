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
	"strconv"
	"strings"
	"sync"
	"time"
	"vault-kv/internal/worker"
)

const compactionInterval = 1 * time.Hour
const memTableSizeThreshold = 4 * 1024 * 1024 // 4MB

// Compaction thresholds (Size-Tiered)
const maxLevel0Files = 4
const maxLevelFilesMultiplier = 10

// tombstone is the sentinel value the Store writes to mark a key as deleted.
// It propagates through the active memtable, frozen memtables, and SSTs so
// that Store.Get can suppress older copies of the key found at lower levels.
// Skiplist is intentionally unaware of this sentinel — it stores tombstones
// like any other string value, and the Store layer alone interprets them.
const tombstone = "0:^_#TOMBSTONE#_^:0"

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
	dir              string
	nodeId           string
	data             *Skiplist
	frozenMemTs      []*Skiplist
	SSTLevels        []*SSTLevel
	flushChan        chan *flushTask
	mu               sync.RWMutex
	wal              *WAL
	flushWg          sync.WaitGroup
	isClosed         bool
	OnFlushErr       func(error) // Callback for background flush errors
	compactionWorker *worker.CompactionWorker
	compactChan      chan int
}

func NewStore(dir, nodeID string) (*Store, error) {
	if !validNodeID.MatchString(nodeID) {
		return nil, fmt.Errorf("invalid nodeID: %q", nodeID)
	}

	data := NewSkiplist()

	// Load existing SSTs
	// vault_<nodeID>_L<level>_<timestamp>.sst
	sstPattern := filepath.Join(dir, fmt.Sprintf("vault_%s_*.sst", nodeID))
	sstFiles, err := filepath.Glob(sstPattern)
	if err != nil {
		return nil, fmt.Errorf("failed to scan for old SSTs: %w", err)
	}

	existingSstables := make([]*SSTLevel, 0)
	sort.Strings(sstFiles)

	for _, file := range sstFiles {
		levelStr, err := extractLevelFromPath(file)
		if err != nil {
			closeAllSSTables(existingSstables)
			return nil, fmt.Errorf("failed to parse level from %s: %w", file, err)
		}
		levelNum, err := strconv.Atoi(levelStr)

		if err != nil {
			closeAllSSTables(existingSstables)
			return nil, fmt.Errorf("invalid level number in %s: %w", file, err)
		}

		for len(existingSstables) <= levelNum {
			existingSstables = append(existingSstables, &SSTLevel{
				Tables: make([]*SST, 0),
			})
		}

		curSst, err := NewSST(file)
		if err != nil {
			closeAllSSTables(existingSstables)
			return nil, fmt.Errorf("failed to open old SST %s: %w", file, err)
		}

		if err := curSst.LoadIndexBlock(); err != nil {
			curSst.Close()
			closeAllSSTables(existingSstables)
			return nil, fmt.Errorf("corrupted SST detected in %s: %w", file, err)
		}

		existingSstables[levelNum].Tables = append(existingSstables[levelNum].Tables, curSst)
	}

	for _, level := range existingSstables {
		if level != nil && len(level.Tables) > 0 {
			sort.Slice(level.Tables, func(i, j int) bool {
				return level.Tables[i].filename < level.Tables[j].filename
			})
		}
	}

	// Load data from the existing WALs
	walPattern := filepath.Join(dir, fmt.Sprintf("vault_%s_*.wal", nodeID))
	walFiles, err := filepath.Glob(walPattern)
	if err != nil {
		closeAllSSTables(existingSstables)
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
			closeAllSSTables(existingSstables)
			return nil, fmt.Errorf("failed to open old WAL %s: %w", file, err)
		}

		entries, err := oldWal.ReadAll()
		if err != nil {
			oldWal.Close()
			closeAllSSTables(existingSstables)
			return nil, fmt.Errorf("corrupted WAL detected in %s: %w", file, err)
		}

		for _, v := range entries {
			// Tombstones are stored verbatim in the memtable, the Store
			// layer interprets them at Get time.
			data.Set(v.Key, v.Value)
		}
		oldWal.Close()
	}

	newWalName := fmt.Sprintf("vault_%s_%d.wal", nodeID, time.Now().UnixNano())

	wal, err := NewWAL(filepath.Join(dir, newWalName))
	if err != nil {
		return nil, fmt.Errorf("initializing WAL: %w", err)
	}

	compactChan := make(chan int, 100)

	storeObj := &Store{
		dir:         dir,
		nodeId:      nodeID,
		data:        data,
		frozenMemTs: make([]*Skiplist, 0),
		SSTLevels:   existingSstables,
		flushChan:   make(chan *flushTask, 10),
		compactChan: compactChan,
		wal:         wal,
	}

	// Initialize workers
	compactionWorker := worker.NewCompactionWorker(compactionInterval, compactChan, storeObj)
	compactionWorker.Run()
	storeObj.compactionWorker = compactionWorker

	storeObj.flushWg.Add(1)
	go storeObj.flushWorker()

	return storeObj, nil
}

// extractLevelFromPath parses strings like "sst/vault_node_east_01_L2_1717670400.sst"
// and safely returns the level ("2") without using slow regex.
func extractLevelFromPath(path string) (string, error) {
	parts := strings.Split(path, "_")

	// A valid path must have at least: sst/vault, nodeID, L<level>, timestamp.sst (4 parts)
	if len(parts) < 3 {
		return "", fmt.Errorf("invalid sst file path layout")
	}

	levelPart := parts[len(parts)-2]

	if len(levelPart) > 1 && levelPart[0] == 'L' {
		return levelPart[1:], nil
	}

	return "", fmt.Errorf("could not find level marker 'L' in path: %s", path)
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
	var foundVal string
	var isFound bool

	s.ForEachSST(func(lvl int, curSst *SST) bool {
		targetBytes := []byte(targetKey)
		idx, found := slices.BinarySearchFunc(curSst.indexEntries, targetBytes, func(entry *IndexBlockEntry, target []byte) int {
			return bytes.Compare(entry.KeyBytes, target)
		})

		if found {
			exactPtr := curSst.indexEntries[idx].Ptr
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
				return true
			}
			valLen := binary.LittleEndian.Uint32(valLenBytes)

			// The actual Value bytes are located after keyLen, valLen, and the Key string.
			// Offset = exactPtr + 2 (keyLen) + 4 (valLen) + keyLen (the actual key bytes)
			valOffset := int64(exactPtr) + 2 + 4 + int64(keyLen)

			valBytes := make([]byte, valLen)
			if _, err := curSst.fd.ReadAt(valBytes, valOffset); err != nil {
				return true
			}

			foundVal = string(valBytes)
			isFound = true
			return false
		}

		return true
	})

	return foundVal, isFound
}

// ForEachSST iterates over all SSTables in the store.
// It iterates from the lowest level (L0) to the highest, and within each level,
// from the newest SSTable to the oldest.
// The callback returns false to stop the iteration.
// Note: The caller must hold the RLock on the Store's mutex.
func (s *Store) ForEachSST(cb func(lvl int, sst *SST) bool) {
	for _, level := range s.SSTLevels {
		if level == nil {
			continue
		}
		for i := len(level.Tables) - 1; i >= 0; i-- {
			if !cb(level.LevelNum, level.Tables[i]) {
				return
			}
		}
	}
}

func (s *Store) Delete(key string) error {
	// A Delete is just a Set with the TOMBSTONE value
	return s.Set(key, tombstone)
}

// flushMemTable freezes the active memtable and its corresponding WAL, appending
// the memtable to the frozen list so it remains queryable. It then initializes a
// new active memtable and WAL to accept incoming writes. It returns a flushTask
// containing the frozen data to be processed by the background flush worker.
// The caller must hold the Store's write lock before invoking this function.
func (s *Store) flushMemTable() (*flushTask, error) {

	timestamp := time.Now().UnixNano()
	newWalName := fmt.Sprintf("vault_%s_%d.wal", s.nodeId, timestamp)
	newSstName := fmt.Sprintf("vault_%s_L0_%d.sst", s.nodeId, timestamp)

	newWal, err := NewWAL(filepath.Join(s.dir, newWalName))
	if err != nil {
		return nil, fmt.Errorf("failed to create new WAL: %w", err)
	}

	frozenData := s.data
	frozenWal := s.wal

	// Freeze the memtable so it can still be queried during the flush
	s.frozenMemTs = append(s.frozenMemTs, frozenData)

	// Replcae the current memt n wal with the new one
	s.data = NewSkiplist()
	s.wal = newWal

	return &flushTask{
		data:    frozenData,
		wal:     frozenWal,
		sstName: newSstName,
	}, nil
}

// flushWorker is a long-running background goroutine that sequentially processes
// flushTasks from the Store's flush channel. For each task, it writes the frozen
// memtable's data to a new SSTable on disk, loads the SSTable's index, and cleans up
// the obsolete WAL file. In the event of a disk or write error, it will infinitely
// retry the flush operation to prevent silent data loss and maintain the FIFO
// sequence of frozen memtables. Upon success, it locks the Store to append the new
// SSTable to the active list and safely removes the flushed memtable from memory.
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

		var newSst *SST
		var err error

		// We cannot use 'continue' to skip a failed task because it will permanently breaks the
		// FIFO 1:1 alignment with 's.frozenMemTs', causing the wrong MemTable to be deleted from RAM
		// If a disk failure occurs, our current solution is to retry until it succeeds to prevent silent data loss

		// U might think of a solution like DLQ, but then we need to think on how to scan through this
		// DLQ for the GET query. It will be an anti-pattern to manage this as it's no differ than another Frozen MemTs

		// RocksDB use "Wrtie Stalls" and "ReadOnly Mode" btw, so the current implementation of infinite retry is basically
		// the step 1 of the canon sol

		for {
			// Cleanup any partial file from previous attempt
			_ = os.Remove(filepath.Join(s.dir, task.sstName))

			newSst, err = NewSST(filepath.Join(s.dir, task.sstName))
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

		curLvl := 0
		
		for len(s.SSTLevels) <= curLvl {
			s.SSTLevels = append(s.SSTLevels, &SSTLevel{
				LevelNum: len(s.SSTLevels),
				Tables:   make([]*SST, 0),
			})
		}
		
		s.SSTLevels[curLvl].Tables = append(s.SSTLevels[curLvl].Tables, newSst)
		s.compactChan <- curLvl
		s.mu.Unlock()
	}
}

func (s *Store) CheckCompaction(level int) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if level >= len(s.SSTLevels) {
		return
	}

	threshold := maxLevel0Files
	for i := 0; i < level; i++ {
		threshold *= maxLevelFilesMultiplier
	}

	if len(s.SSTLevels[level].Tables) >= threshold {
		// Do not execute compaction directly inside the lock to avoid blocking other operations
		go s.ExecuteCompaction(level)
	}
}

func (s *Store) ExecuteCompaction(level int) {
	// TODO: Trigger actual compaction worker logic here.
	// For now, we will just print a log or delegate it to compactionWorker.
	// The CompactionWorker currently has a Compact() method. We should probably
	// notify the CompactionWorker to start compacting a specific level instead.
	fmt.Printf("Triggering compaction for level %d\n", level)
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

	s.compactionWorker.Stop()

	var errs []error
	if err := s.wal.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close WAL: %w", err))
	}
	s.ForEachSST(func(lvl int, sst *SST) bool {
		if err := sst.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close SSTable: %w", err))
		}
		return true
	})

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}
