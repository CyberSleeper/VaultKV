package store

import (
	"container/heap"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
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
	// compactChan is a coalescing "doorbell": it carries no level information.
	// A single buffered slot means many flushes collapse into one pending
	// wake-up, and the worker re-scans every level on each wake, so dropping a
	// duplicate signal can never lose a level-specific trigger.
	compactChan chan struct{}
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

	compactChan := make(chan struct{}, 1)

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

	// 3. SSTables (newest to oldest). The sparse index + block read/scan and
	// CRC verification all live in SST.lookup; ReadAt keeps reads atomic and
	// cursor-free so they are safe under the RLock held here.
	var foundVal string
	var isFound bool

	s.ForEachSST(func(lvl int, curSst *SST) bool {
		val, found, err := curSst.lookup(targetKey)
		if err != nil {
			// Treat a read/checksum failure as "not in this SST" and keep
			// scanning older SSTs rather than crashing the read path.
			return true
		}
		if found {
			foundVal = val
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

		// Flush will only flush to lvl 0
		const curLvl = 0

		for len(s.SSTLevels) <= curLvl {
			s.SSTLevels = append(s.SSTLevels, &SSTLevel{
				LevelNum: len(s.SSTLevels),
				Tables:   make([]*SST, 0),
			})
		}

		s.SSTLevels[curLvl].Tables = append(s.SSTLevels[curLvl].Tables, newSst)
		s.mu.Unlock()

		// Ring the compaction doorbell strictly OUTSIDE the lock to avoid a
		// channel-mutex deadlock: the worker drains this by calling
		// CheckCompaction, which needs the RLock.
		s.signalCompaction()
	}
}

// signalCompaction rings the compaction doorbell without blocking. The bell
// carries no level info, so if a wake-up is already pending we simply drop the
// duplicate — the worker re-scans all levels on its next pass regardless.
func (s *Store) signalCompaction() {
	select {
	case s.compactChan <- struct{}{}:
	default:
	}
}

// CheckCompaction scans every level and compacts those that have reched their
// size-tiered threshold. It is invoked by the compaction worker on each
// doorbell ring and on each periodic tick. The threshold grows geometrically:
// L0 = maxLevel0Files, and each deeper level multiplies by
// maxLevelFilesMultiplier.
func (s *Store) CheckCompaction() {
	s.mu.RLock()
	var overThreshold []int
	threshold := maxLevel0Files
	for level := range s.SSTLevels {
		if s.SSTLevels[level] != nil && len(s.SSTLevels[level].Tables) >= threshold {
			overThreshold = append(overThreshold, level)
		}
		threshold *= maxLevelFilesMultiplier
	}
	s.mu.RUnlock()

	// Run compactions outside the read lock; ExecuteCompaction takes its own
	// write lock when it swaps SSTs in. A level pushed over threshold by this
	// pass (e.g. L1 growing after an L0 merge) is caught on the next doorbell.
	//
	// Call synchronously, NOT `go s.ExecuteCompaction(...)`: serial invocation
	// is what keeps compactions from racing each other and keeps them inside
	// the worker's WaitGroup for graceful shutdown (see ExecuteCompaction doc).
	for _, level := range overThreshold {
		s.ExecuteCompaction(level)
	}
}

// ExecuteCompaction merges every SST currently in `level` into a single new
// SST at level+1 (a simplified size-tiered scheme).
//
// MUST be called synchronously (never `go s.ExecuteCompaction(...)`). It relies
// on two invariants that only hold under serial invocation by the single
// compaction worker goroutine:
//   - No two compactions run at once, so no in-progress guard is needed and the
//     locked swap in step 5 cannot race another compaction.
//   - It runs inside CompactionWorker.Compact, whose WaitGroup lets Close()
//     block until an in-flight compaction finishes. Spawning it on a new
//     goroutine would escape that WaitGroup and break graceful shutdown.
//
// Crash safety: the merged SST is written to a ".tmp" file and renamed into
// place only once fully written and synced. The NewStore glob (`*.sst`) ignores
// ".tmp", so a crash mid-write leaves a partial temp that is simply ignored on
// restart rather than bricking startup.
func (s *Store) ExecuteCompaction(level int) {
	handleErr := func(err error) {
		if s.OnFlushErr != nil {
			s.OnFlushErr(err)
		} else {
			fmt.Printf("Background compaction error: %v\n", err)
		}
	}

	// 1. Snapshot the inputs and decide whether targetLevel is the bottom level
	//    (no SSTs exist at any deeper level). Both facts are read under the same
	//    RLock so they are consistent. isBottom is stable: only this worker
	//    goroutine ever compacts (serial-invocation invariant), so no concurrent
	//    compaction can add a deeper level between here and the merge.
	s.mu.RLock()
	if level >= len(s.SSTLevels) || s.SSTLevels[level] == nil || len(s.SSTLevels[level].Tables) == 0 {
		s.mu.RUnlock()
		return
	}
	inputs := make([]*SST, len(s.SSTLevels[level].Tables))
	copy(inputs, s.SSTLevels[level].Tables)
	targetLevel := level + 1
	isBottom := true
	for lvl := targetLevel + 1; lvl < len(s.SSTLevels); lvl++ {
		if s.SSTLevels[lvl] != nil && len(s.SSTLevels[lvl].Tables) > 0 {
			isBottom = false
			break
		}
	}
	s.mu.RUnlock()

	// 2. Merge with newest-wins semantics. At the bottom level tombstones are
	//    dropped; at intermediate levels they are propagated so they keep
	//    shadowing older copies of the key in deeper levels.
	merged, err := mergeSSTs(inputs, isBottom)
	if err != nil {
		handleErr(fmt.Errorf("compaction merge (L%d): %w", level, err))
		return
	}

	// 3+4. Write the merged SST to a temp file, rename it in, then open the
	//      live handle. Skipped entirely when the merge produced no entries
	//      (all entries were tombstones dropped at the bottom level) — there is
	//      nothing to write and nothing to add to targetLevel.
	var newSst *SST
	if len(merged.LogEntries) > 0 {
		finalName := fmt.Sprintf("vault_%s_L%d_%d.sst", s.nodeId, targetLevel, time.Now().UnixNano())
		finalPath := filepath.Join(s.dir, finalName)
		tmpPath := finalPath + ".tmp"

		if err := writeSSTFile(tmpPath, merged); err != nil {
			_ = os.Remove(tmpPath)
			handleErr(fmt.Errorf("compaction write (L%d): %w", level, err))
			return
		}
		if err := os.Rename(tmpPath, finalPath); err != nil {
			_ = os.Remove(tmpPath)
			handleErr(fmt.Errorf("compaction rename (L%d): %w", level, err))
			return
		}

		newSst, err = NewSST(finalPath)
		if err != nil {
			handleErr(fmt.Errorf("compaction open new SST (L%d): %w", level, err))
			return
		}
		if err := newSst.LoadIndexBlock(); err != nil {
			newSst.Close()
			_ = os.Remove(finalPath)
			handleErr(fmt.Errorf("compaction index new SST (L%d): %w", level, err))
			return
		}
	}

	// 5. Atomically swap: drop exactly the compacted inputs from `level` and,
	//    if there is a merged SST, append it to targetLevel. Filtering by
	//    pointer identity preserves any SST a concurrent flush added to L0
	//    since the snapshot was taken.
	s.mu.Lock()
	if newSst != nil {
		for len(s.SSTLevels) <= targetLevel {
			s.SSTLevels = append(s.SSTLevels, &SSTLevel{
				LevelNum: len(s.SSTLevels),
				Tables:   make([]*SST, 0),
			})
		}
	}
	inputSet := make(map[*SST]struct{}, len(inputs))
	for _, in := range inputs {
		inputSet[in] = struct{}{}
	}
	remaining := make([]*SST, 0, len(s.SSTLevels[level].Tables))
	for _, t := range s.SSTLevels[level].Tables {
		if _, compacted := inputSet[t]; !compacted {
			remaining = append(remaining, t)
		}
	}
	s.SSTLevels[level].Tables = remaining
	if newSst != nil {
		s.SSTLevels[targetLevel].Tables = append(s.SSTLevels[targetLevel].Tables, newSst)
	}
	s.mu.Unlock()

	// 6. Delete the old files. Safe after the unlock: the inputs are no longer
	//    referenced, and any reader either finished before the swap or started
	//    after it (Get holds RLock for its whole duration, mutually exclusive
	//    with the swap's Lock).
	for _, old := range inputs {
		if err := old.Delete(); err != nil {
			handleErr(fmt.Errorf("removing compacted SST %s: %w", old.filename, err))
		}
	}
}

// mergeItem is one element in the k-way merge heap: the current key/value
// being considered from a single input SST, plus the iterator to advance it.
type mergeItem struct {
	key    string
	val    string
	sstIdx int      // position in the inputs slice; higher index = newer SST
	iter   *sstIter // live cursor into this SST's data blocks
}

// mergeHeap implements container/heap over mergeItems.
//
// Ordering: key ASC, then sstIdx DESC for ties. The DESC tie-break ensures
// that, when the same key exists in multiple SSTs, the newest SST's copy
// surfaces first so the loop in mergeSSTs can emit it and discard the rest
// without ever seeing them in the output.
type mergeHeap []mergeItem

func (h mergeHeap) Len() int      { return len(h) }
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h mergeHeap) Less(i, j int) bool {
	if h[i].key != h[j].key {
		return h[i].key < h[j].key
	}
	return h[i].sstIdx > h[j].sstIdx // newer SST wins the tie
}
func (h *mergeHeap) Push(x any) { *h = append(*h, x.(mergeItem)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// mergeSSTs performs a k-way heap merge over the input SSTables and returns
// their entries as a single sorted SSTEntry. Inputs must be ordered
// oldest→newest; the Store maintains this invariant by sorting each level's
// Tables by filename, which embeds the creation timestamp.
//
// On a duplicate key the newest value wins (O(N log K) merge, no re-sort).
//
// When dropTombstones is true (caller is compacting into the bottom-most level)
// tombstone entries are omitted from the output: no deeper level exists that
// could resurrect the key, so the tombstone has done its job and can be freed.
// At any intermediate level dropTombstones must be false so that the tombstone
// keeps shadowing older copies of the key that still live in deeper levels.
func mergeSSTs(inputs []*SST, dropTombstones bool) (*SSTEntry, error) {
	if len(inputs) == 0 {
		return NewSSTableEntry(), nil
	}

	h := make(mergeHeap, 0, len(inputs))
	heap.Init(&h)

	// Open a streaming iterator for each input SST and seed the heap with each
	// SST's first entry. sstIter reads the file once via os.ReadFile so it
	// never disturbs the live fd used by concurrent point lookups.
	for i, in := range inputs {
		it, err := newSSTIter(in.filename)
		if err != nil {
			return nil, err
		}
		k, v, ok, err := it.Next()
		if err != nil {
			return nil, err
		}
		if ok {
			heap.Push(&h, mergeItem{key: k, val: v, sstIdx: i, iter: it})
		}
	}

	merged := NewSSTableEntry()
	lastEmitted := ""
	hasEmitted := false

	for h.Len() > 0 {
		item := heap.Pop(&h).(mergeItem)

		// The heap's sstIdx DESC tie-break guarantees that for a duplicated key,
		// the newest SST's copy is popped first. Subsequent pops of the same key
		// come from older SSTs and must be discarded.
		if !hasEmitted || item.key != lastEmitted {
			// Always advance lastEmitted even when we skip the entry — this
			// ensures the older-SST copies of the same key (popped next due to
			// the tie-break order) are still correctly discarded.
			lastEmitted = item.key
			hasEmitted = true

			if !(dropTombstones && item.val == tombstone) {
				merged.LogEntries = append(merged.LogEntries, NewLogEntry(item.key, item.val))
			}
		}

		// Advance this SST's iterator and re-insert its next entry.
		k, v, ok, err := item.iter.Next()
		if err != nil {
			return nil, err
		}
		if ok {
			heap.Push(&h, mergeItem{key: k, val: v, sstIdx: item.sstIdx, iter: item.iter})
		}
	}

	return merged, nil
}

// writeSSTFile creates a new SST at path, writes the entry (Encode + fsync via
// Append), and closes the handle. The handle is closed before the caller
// renames the file so the rename is safe on Windows.
func writeSSTFile(path string, entry *SSTEntry) error {
	sst, err := NewSST(path)
	if err != nil {
		return err
	}

	if err := sst.Append(entry); err != nil {
		sst.Close()
		return err
	}

	return sst.Close()
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
