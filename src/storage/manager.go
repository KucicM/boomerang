package storage

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

var storageHistogram = prometheus.NewHistogramVec(
    prometheus.HistogramOpts{Name: "db_manager_ops", Help: "Operations from database managment"},
    []string{"type", "op"},
)

var storageQueueSize = prometheus.NewGaugeVec(
    prometheus.GaugeOpts{Name: "db_manager_queue", Help: "Size of database queues"},
    []string{"op"},
)

func init() {
    prometheus.MustRegister(
        storageHistogram,
        storageQueueSize,
    )
}

type StorageManager struct {
    queue *persistentQueue
    blobs *blobStorage
    bulkSave *BulkProcessor[StorageItem]
    bulkDelete *BulkProcessor[string]
    bulkUpdate *BulkProcessor[queueItem]
    stop int32
    wg *sync.WaitGroup
}

func NewStorageManager() (*StorageManager, error) {
    queueCfg := PersistentQueueCfg{DbURL: "queue.db"}
    queue, err := newQueue(queueCfg)
    if err != nil {
        return nil, err
    }

    blobCfg := BlobStorageCfg{DbURL: "blobs.db"}
    blobs, err := newBlobStorage(blobCfg)
    if err != nil {
        return nil, err
    }

    m := &StorageManager{queue: queue, blobs: blobs, stop: 0, wg: &sync.WaitGroup{}}
    m.bulkSave = NewBulkProcessor(
        1000,
        100,
        time.Millisecond * 10,
        m.save,
    )

    m.bulkDelete = NewBulkProcessor(
        1000,
        100,
        time.Millisecond * 10,
        m.delete,
    )

    m.bulkUpdate = NewBulkProcessor(
        1000,
        100,
        time.Millisecond * 10,
        m.update,
    )

    return m, nil
}

func (s *StorageManager) Save(item StorageItem) error {
    defer func(start time.Time) {
        storageHistogram.WithLabelValues("single", "save").Observe(float64(time.Since(start)))
        storageQueueSize.WithLabelValues("save").Inc()
    }(time.Now())

    item.Id = uuid.New().String()
    return s.bulkSave.Add(item)
}

func (s *StorageManager) save(items []StorageItem) error {
    defer func(start time.Time) {
        storageHistogram.WithLabelValues("bulk", "save").Observe(float64(time.Since(start)))
        storageQueueSize.WithLabelValues("save").Sub(float64(len(items)))
    }(time.Now())

    blobItems := make([]blobItem, len(items))
    queueItems := make([]queueItem, len(items))
    for i, item := range items {
        blobItems[i] = toBlobItem(item)
        queueItems[i] = toQueueItem(item)
    }

    if err := s.blobs.save(blobItems); err != nil {
        log.Printf("error on bulk blob save %s\n", err)
        return err
    }

    if err := s.queue.save(queueItems); err != nil {
        log.Printf("error on push many to queue %s\n", err)
        return err
    }

    return nil
}

func (s *StorageManager) GetReqQueue(maxSize int) (chan StorageItem) {
    q := make(chan StorageItem, maxSize)
    s.wg.Add(1)
    go func() {
        defer s.wg.Done()
        for atomic.LoadInt32(&s.stop) == 0 {
            items, err := s.load(maxSize)
            if err != nil {
                log.Printf("error loading items from database %s\n", err)
            }

            for _, item := range items {
                q <- item
            }

            if len(items) == 0 {
                // probably could wait for request and stop wating?
                time.Sleep(time.Second)
            }
        }
        close(q)
    }()
    return q
}

func (s *StorageManager) StopLoadQueue() {
    log.Println("stopping adding items to req queue")
    atomic.StoreInt32(&s.stop, 1)
    s.wg.Wait()
    log.Println("stopped adding items to req queue")
}

func (s *StorageManager) load(maxSize int) ([]StorageItem, error) {
    defer func(start time.Time) {
        storageHistogram.WithLabelValues("bulk", "load").Observe(float64(time.Since(start)))
    }(time.Now())

    queueItems, err := s.queue.load(maxSize)
    if err != nil {
        log.Printf("error poping from queue %s\n", err)
        return nil, err
    }

    ids := make([]string, len(queueItems))
    for i := 0; i < len(queueItems); i++ {
        ids[i] = queueItems[i].id
    }

    blobItems, err := s.blobs.load(ids)
    if err != nil {
        log.Printf("error loading blobs %s\n", err)
        return nil, err
    }

    ret := make([]StorageItem, len(blobItems))
    for i := 0; i < len(blobItems); i++ {
        ret[i] = toStorageItem(queueItems[i], blobItems[i])
    }

    return ret, nil
}

func (s *StorageManager) Update(item StorageItem) error {
    defer func(start time.Time) {
        storageHistogram.WithLabelValues("single", "update").Observe(float64(time.Since(start)))
        storageQueueSize.WithLabelValues("update").Inc()
    }(time.Now())

    return s.bulkUpdate.Add(toQueueItem(item))
}

func (s *StorageManager) update(items []queueItem) error {
    defer func(start time.Time) {
        storageHistogram.WithLabelValues("bulk", "update").Observe(float64(time.Since(start)))
        storageQueueSize.WithLabelValues("update").Sub(float64(len(items)))
    }(time.Now())

    if err := s.queue.update(items); err != nil {
        log.Printf("failed to update queue %s\n", err)
        return err
    }
    return nil
}

func (s *StorageManager) Delete(item StorageItem) {
    defer func(start time.Time) {
        storageHistogram.WithLabelValues("single", "delete").Observe(float64(time.Since(start)))
        storageQueueSize.WithLabelValues("delete").Inc()
    }(time.Now())

    s.bulkDelete.Add(item.Id)
}

// todo retry delete
func (s *StorageManager) delete(ids []string) error {
    defer func(start time.Time) {
        storageHistogram.WithLabelValues("bulk", "delete").Observe(float64(time.Since(start)))
        storageQueueSize.WithLabelValues("delete").Sub(float64(len(ids)))
    }(time.Now())

    if err := s.queue.delete(ids); err != nil {
        log.Printf("Delete from queue failed %v\n", err)
    }
    if err := s.blobs.delete(ids); err != nil {
        log.Printf("Delete from blobs failed %v\n", err)
    }
    return  nil
}

func (s *StorageManager) Shutdown() error {
    log.Println("store shutdown...")

    log.Println("shutdown bulk save")
    s.bulkSave.Shutdown()

    log.Println("shutdown bulk update")
    s.bulkUpdate.Shutdown()

    log.Println("shutdown bulk delete")
    s.bulkDelete.Shutdown()


    var ret error
    if err := s.queue.Shutdown(); err != nil {
        log.Printf("error shutdown queue %s\n", err)
        ret = err
    }

    if err := s.blobs.Shutdown(); err != nil {
        log.Printf("error shutdown blobs %s\n", err)
        ret = err
    }
    return ret
}
