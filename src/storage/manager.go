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
    if err := s.queue.Delete(id); err != nil {
        log.Printf("Delete from queue failed %v\n", err)
    }

    if err := s.blobs.Delete(id); err != nil {
        log.Printf("Delete from blobs failed %v\n", err)
    }
}
