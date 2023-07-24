package storage

type StorageItem struct {
    Id string
    Endpoint string
    SendAfter uint64
    MaxRetry int
    BackOffMs uint64
    Status Status
    Payload string
    Headers string
}

func toStorageItem(q queueItem, b blobItem) StorageItem {
    return StorageItem{q.id, q.endpoint, q.sendAfter, q.leftAttempts, q.backOffMs, q.status, b.payload, b.headers}
}

type queueItem struct {
    id string
    endpoint string
    sendAfter uint64
    leftAttempts int
    backOffMs uint64
    status Status
}

func toQueueItem(s StorageItem) queueItem {
    return queueItem{s.Id, s.Endpoint, s.SendAfter, s.MaxRetry, s.BackOffMs, s.Status}
}

type blobItem struct {
    id string
    payload string
    headers string
}

func toBlobItem(s StorageItem) blobItem {
    return blobItem{s.Id, s.Payload, s.Headers}
}

type Status int
const (
    Initial Status = iota
    Running = iota
    Retry = iota
    Recovered = iota
)
