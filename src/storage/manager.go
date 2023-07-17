package storage

import (
	"log"
	"time"

	"github.com/google/uuid"
)

type StorageManager struct {
    queue *persistentQueue
    blobs *blobStorage
    bulkSave *BulkProcessor[StorageItem]
    bulkDelete *BulkProcessor[string]
    bulkUpdate *BulkProcessor[queueItem]
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

    m := &StorageManager{queue: queue, blobs: blobs}
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
    item.Id = uuid.New().String()
    return s.bulkSave.Add(item)
}

func (s *StorageManager) save(items []StorageItem) error {
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

func (s *StorageManager) Load(maxSize int) ([]StorageItem, error) {
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
    return s.bulkUpdate.Add(toQueueItem(item))
}

func (s *StorageManager) update(items []queueItem) error {
    if err := s.queue.update(items); err != nil {
        log.Printf("failed to update queue %s\n", err)
        return err
    }
    return nil
}

func (s *StorageManager) Delete(item StorageItem) {
    s.bulkDelete.Add(item.Id)
}

// todo retry delete
func (s *StorageManager) delete(ids []string) error {
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
