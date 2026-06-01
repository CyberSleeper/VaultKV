package worker

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type CompactionWorker struct {
	Interval     time.Duration
	cancelFunc   context.CancelFunc
	wgCompaction sync.WaitGroup
	ticker       *time.Ticker
	ctx          context.Context
}

func NewCompactionWorker(interval time.Duration) *CompactionWorker {
	ticker := time.NewTicker(interval)
	ctx, cancel := context.WithCancel(context.Background())

	return &CompactionWorker{
		Interval:   interval,
		ticker:     ticker,
		ctx:        ctx,
		cancelFunc: cancel,
	}
}

func (c *CompactionWorker) Run() {
	if c.ticker == nil || c.ctx == nil {
		panic("Compaction worker not initialized")
	}
	go func() {
		defer c.ticker.Stop()
		for {
			select {
			case <-c.ctx.Done():
				fmt.Println("Compaction worker stopped")
				return
			case <-c.ticker.C:
				c.Compact()
			}
		}
	}()
}

func (c *CompactionWorker) Stop() {
	if c.cancelFunc == nil {
		return
	}
	c.cancelFunc()
	c.wgCompaction.Wait()
}

func (c *CompactionWorker) Compact() {
	c.wgCompaction.Add(1)
	defer c.wgCompaction.Done()

	// TODO: implement size-tiered compaction
	// Current idea
	// 1. Pass the filenames for those SST we want to compact
	// 2. Do the K-way merge, it's just like a normal merging but we need to put
	// all elements from topmost to a heap, this way the complexity is O(N log K)
	// instead of O(NK) where N is the len of longest SST entries.
	// 3. Create the compacted SST
	// 4. Lock the SST and remove the references and files of old SSTs then insert
	// the resulting SST
	// 5. Unlock
}
