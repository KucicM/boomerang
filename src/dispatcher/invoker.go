package dispatcher

import (
	"bytes"
	"log"
	"net/http"
	"time"

	"github.com/kucicm/boomerang/src/storage"
)

const PERIOD = 5 * time.Second

type Invoker struct {
    store *storage.StorageManager
    retry *Retrier
    maxBatchSize int
    workerPoolSize int
}

func NewInvoker(store *storage.StorageManager) *Invoker {
    retry := NewRetrier(store)
    invoker := &Invoker{store, retry, 1000, 100}
    go invoker.start()
    return invoker
}

func (inv *Invoker) start() {
    semaphore := make(chan struct{}, inv.workerPoolSize)
    for {
        dbReqs, err := inv.store.Load(inv.maxBatchSize)
        if err != nil {
            log.Printf("Cannot load items %v\n", err)
            continue
        }

        for _, dbReq := range dbReqs {
            semaphore <- struct{}{}
            go inv.invoke(dbReq, semaphore)
        }
    }
}


func (inv *Invoker) invoke(dbItem storage.StorageItem, semaphore chan struct{}) {
    req, err := http.NewRequest(http.MethodPost, dbItem.Endpoint, bytes.NewBufferString(dbItem.Payload))
    if err != nil {
        inv.retry.Retry(dbItem)
        log.Printf("Failed to create http request %v\n", err)
        return
    }

    client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        inv.retry.Retry(dbItem)
        log.Printf("Error on call %v\n", err)
        return
    }
    defer resp.Body.Close()

    inv.store.Delete(dbItem)

    <- semaphore
}
