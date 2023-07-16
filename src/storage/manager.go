package storage

import (
	"log"
	"time"

	"github.com/google/uuid"
)

type StorageRequest struct {
    Id string
    Endpoint string
    Payload string
    SendAfter uint64
}

type StorageManager struct {
    queue *PersistentQueue
    blobs *BlobStorage
    bulkSave *BulkProcessor[StorageRequest]
    bulkDelete *BulkProcessor[string]
}

func NewStorageManager() (*StorageManager, error) {
    queueCfg := PersistentQueueCfg{DbURL: "queue.db"}
    queue, err := NewQueue(queueCfg)
    if err != nil {
        return nil, err
    }

    blobCfg := BlobStorageCfg{DbURL: "blobs.db"}
    blobs, err := NewBlobStorage(blobCfg)
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

    return m, nil
}

func (s *StorageManager) Save(req StorageRequest) error {
    req.Id = uuid.New().String()
    return s.bulkSave.Add(req)
}

func (s *StorageManager) save(reqs []StorageRequest) error {
    blobReqs := make([]BlobStorageRequest, len(reqs))
    queueReqs := make([]QueueRequest, len(reqs))
    for i, req := range reqs {
        blobReqs[i] = BlobStorageRequest{Id: req.Id, Payload: req.Payload}
        queueReqs[i] = QueueRequest{Id: req.Id, Endpoint: req.Endpoint, SendAfter: req.SendAfter}
    }

    if err := s.blobs.BulkSave(blobReqs); err != nil {
        log.Printf("error on bulk blob save %s\n", err)
        return err
    }

    if err := s.queue.PushMany(queueReqs); err != nil {
        log.Printf("error on push many to queue %s\n", err)
        return err
    }

    return nil
}

func (s *StorageManager) Load(maxSize int) ([]StorageRequest, error) {
    items, err := s.queue.Pop(maxSize)
    if err != nil {
        log.Printf("error poping from queue %s\n", err)
        return nil, err
    }

    ids := make([]string, len(items))
    for i := 0; i < len(items); i++ {
        ids[i] = items[i].Id
    }

    payloads, err := s.blobs.Load(ids)
    if err != nil {
        log.Printf("error loading blobs %s\n", err)
        return nil, err
    }

    ret := make([]StorageRequest, len(payloads))
    for i := 0; i < len(payloads); i++ {
        ret[i] = StorageRequest{
            Id: items[i].Id,
            Endpoint: items[i].Endpoint,
            Payload: payloads[i],
            SendAfter: items[i].SendAfter,
        }
    }

    return ret, nil
}

func (s *StorageManager) Delete(id string) {
    s.bulkDelete.Add(id)
}

func (s *StorageManager) delete(ids []string) error {
    if err := s.queue.DeleteMany(ids); err != nil {
        log.Printf("Delete from queue failed %v\n", err)
    }

    if err := s.blobs.DeleteMany(ids); err != nil {
        log.Printf("Delete from blobs failed %v\n", err)
    }

    return  nil
}
