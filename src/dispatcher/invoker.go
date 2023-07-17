package dispatcher

import (
	"bytes"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/kucicm/boomerang/src/storage"
)

const PERIOD = 5 * time.Second

type Invoker struct {
    store *storage.StorageManager
    retry *Retrier
    maxBatchSize int
    workerPoolSize int

    status int32
    done chan struct{}
}

func NewInvoker(store *storage.StorageManager) *Invoker {
    retry := NewRetrier(store)
    invoker := &Invoker{store, retry, 1000, 100, 0, make(chan struct{})}
    go invoker.start()
    return invoker
}

func (inv *Invoker) start() {
    semaphore := make(chan struct{}, inv.workerPoolSize)

    for atomic.LoadInt32(&inv.status) == 0 {
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

    close(semaphore)
    for len(semaphore) > 0 {
        <-semaphore
    }

    inv.done <-struct{}{}
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

func (inv *Invoker) Shutdown() error {
    log.Println("Invoker shutdown...")
    atomic.StoreInt32(&inv.status, 1)
    <-inv.done
    return nil
}
