package storage

import (
	"sync/atomic"
	"time"
)

type task [T any] struct {
    item T
    err chan error
}

type BulkProcessor [T any] struct {
    queue chan task[T]
    maxBatchSize int
    maxWait time.Duration
    fn func([]T) error

    statusId int32
    done chan struct{}
}

func NewBulkProcessor[T any](
    maxQueueSize, 
    maxBatchSize int, 
    maxWait time.Duration,
    batchFn func([]T) error,
) *BulkProcessor[T] {
    b := &BulkProcessor[T]{
        queue: make(chan task[T], maxQueueSize),
        maxBatchSize: maxBatchSize,
        maxWait: maxWait,
        fn: batchFn,
        statusId: 0,
        done: make(chan struct{}),
    }
    go b.start()
    return b
}

func (b *BulkProcessor[T]) Add(item T) error {
    err := make(chan error)
    b.queue <- task[T]{item, err}
    return <-err
}

func (b *BulkProcessor[T]) start() {
    for atomic.LoadInt32(&b.statusId) == 0 {
        batch, errs := b.createBatch()
        err := b.fn(batch)
        for i := 0; i < len(errs); i++ {
            errs[i] <- err
        }
    }
    b.done<-struct{}{}
}

func (b *BulkProcessor[T]) createBatch() ([]T, []chan error) {
    timeout := time.NewTimer(b.maxWait)
    defer timeout.Stop()

    items := make([]T, 0)
    errs := make([]chan error, 0)
    for i := 0; i < b.maxBatchSize; i++ {
        select {
        case it := <-b.queue:
            items = append(items, it.item)
            errs = append(errs, it.err)
        case <-timeout.C:
            return items, errs
        }
    }
    return items, errs
}

func (b *BulkProcessor[T]) Shutdown() error {
    for len(b.queue) > 0 {
        time.Sleep(time.Second)
    }
    atomic.StoreInt32(&b.statusId, 1)
    <-b.done
    return nil
}
