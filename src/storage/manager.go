package storage

import (
	"log"

	"github.com/google/uuid"
)

type StorageRequest struct {
    Endpoint string
    Payload string
    SendAfter uint64
}

type StorageManager struct {
    queue *PersistentQueue
    blobs *BlobStorage
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

    return &StorageManager{queue: queue, blobs: blobs}, nil
}

func (s *StorageManager) Save(req StorageRequest) error {
    id := uuid.New().String()

    qReq := QueueRequest{
        Id: id,
        Endpoint: req.Endpoint,
        SendAfter: req.SendAfter,
    }
    if err := s.queue.Push(qReq); err != nil {
        return err
    }

    bReq := BlobStorageRequest{
        Id: id,
        Payload: req.Payload,
    }

    if err := s.blobs.Save(bReq); err != nil {
        // TODO delete from queue
        return err
    }

    return nil
}

func (s *StorageManager) Load() ([]StorageRequest, error) {
    items, err := s.queue.Pop()
    if err != nil {
        return nil, err
    }

    var ret []StorageRequest
    for _, item := range items {
        payload, err := s.blobs.Load(item.Id)
        if err != nil {
            // TODO item should be put back to queue
            log.Println(err)
            continue
        }

        req := StorageRequest{
            Endpoint: item.Endpoint,
            Payload: payload,
            SendAfter: item.SendAfter,
        }
        ret = append(ret, req)
    }

    return ret, nil
}
