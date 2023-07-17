package storage

type StorageItem struct {
    Id string
    Endpoint string
    SendAfter uint64
    MaxRetry int
    BackOffMs uint64
    StatusId int
    Payload string
}

func toStorageItem(q queueItem, b blobItem) StorageItem {
    return StorageItem{q.id, q.endpoint, q.sendAfter, q.leftAttempts, q.backOffMs, q.statusId, b.payload}
}

type queueItem struct {
    id string
    endpoint string
    sendAfter uint64
    leftAttempts int
    backOffMs uint64
    statusId int
}

func toQueueItem(s StorageItem) queueItem {
    return queueItem{s.Id, s.Endpoint, s.SendAfter, s.MaxRetry, s.BackOffMs, s.StatusId}
}

type blobItem struct {
    id string
    payload string
}

func toBlobItem(s StorageItem) blobItem {
    return blobItem{s.Id, s.Payload}
}
