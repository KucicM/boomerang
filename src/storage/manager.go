package storage

import (
	"log"

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

    bReq := BlobStorageRequest{
        Id: id,
        Payload: req.Payload,
    }

    if err := s.blobs.Save(bReq); err != nil {
        return err
    }

    qReq := QueueRequest{
        Id: id,
        Endpoint: req.Endpoint,
        SendAfter: req.SendAfter,
    }
    if err := s.queue.Push(qReq); err != nil {
        _ = s.blobs.Delete(id) // todo handle error
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

    var ret []StorageRequest
    for _, item := range items {
        payload, err := s.blobs.Load(item.Id)
        if err != nil {
            // TODO item should be put back to queue
            log.Printf("error loading from blobs %s\n", err)
            continue
        }

        req := StorageRequest{
            Id: item.Id,
            Endpoint: item.Endpoint,
            Payload: payload,
            SendAfter: item.SendAfter,
        }
        ret = append(ret, req)
    }

    return ret, nil
}

func (s *StorageManager) Delete(id string) {
    if err := s.queue.Delete(id); err != nil {
        log.Printf("Delete from queue failed %v\n", err)
    }

    if err := s.blobs.Delete(id); err != nil {
        log.Printf("Delete from blobs failed %v\n", err)
    }
}
