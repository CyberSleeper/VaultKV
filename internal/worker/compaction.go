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
	return &CompactionWorker{
		Interval: interval,
	}
}

func (c *CompactionWorker) Init() {
	ticker := time.NewTicker(c.Interval)
	ctx, cancel := context.WithCancel(context.Background())

	c.ticker = ticker
	c.ctx = ctx
	c.cancelFunc = cancel
}

func (c *CompactionWorker) Run() {
	go func() {
		for {
			select {
			case <-c.ticker.C:
				c.Compact()
			case <-c.ctx.Done():
				fmt.Println("Compaction worker stopped")
				c.ticker.Stop()
				return
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
}
