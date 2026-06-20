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
	compactChan  chan struct{}
	storeManager StoreManager
}

type StoreManager interface {
	CheckCompaction()
	ExecuteCompaction(level int)
}

func NewCompactionWorker(interval time.Duration, compactChan chan struct{}, storeManager StoreManager) *CompactionWorker {
	ticker := time.NewTicker(interval)
	ctx, cancel := context.WithCancel(context.Background())

	return &CompactionWorker{
		Interval:     interval,
		ticker:       ticker,
		ctx:          ctx,
		cancelFunc:   cancel,
		compactChan:  compactChan,
		storeManager: storeManager,
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
			// Both the doorbell (rung after a flush) and the periodic tick (a
			// safety-net backstop) funnel into the same scan-all-levels pass,
			// so there is exactly one compaction path.
			case <-c.compactChan:
				c.Compact()
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

// Compact runs one scan-all-levels compaction pass. The WaitGroup lets Stop()
// block until an in-flight pass finishes rather than tearing it down mid-merge.
// The actual size-tiered merge lives in the store's CheckCompaction ->
// ExecuteCompaction path.
func (c *CompactionWorker) Compact() {
	c.wgCompaction.Add(1)
	defer c.wgCompaction.Done()

	c.storeManager.CheckCompaction()
}
