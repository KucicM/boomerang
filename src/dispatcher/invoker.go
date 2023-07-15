package dispatcher

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/kucicm/boomerang/src/storage"
)

const PERIOD = 5 * time.Second

type Invoker struct {
    store *storage.StorageManager
    maxBatchSize int
}

func NewInvoker(store *storage.StorageManager) *Invoker {
    invoker := &Invoker{store, 1000}
    go invoker.start()
    return  invoker
}

func (inv *Invoker) start() {
    for {
        dbReqs, err := inv.store.Load(inv.maxBatchSize)
        if err != nil {
            log.Printf("Cannot load items %v\n", err)
            continue
        }

        for _, dbReq := range dbReqs {
            go inv.invoke(dbReq)
        }

        time.Sleep(PERIOD)
    }
}


func (inv *Invoker) invoke(dbReq storage.StorageRequest) {
    req, err := http.NewRequest(http.MethodPost, dbReq.Endpoint, bytes.NewBufferString(dbReq.Payload))
    if err != nil {
        log.Printf("Failed to create http request %v\n", err)
        return
    }

    client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        log.Printf("Error on call %v\n", err)
        return
    }
    defer resp.Body.Close()

    body, err := io.ReadAll(resp.Body)
    if err != nil {
        log.Printf("Error reading body %v\n", err)
        return
    }

    log.Println(string(body))

    inv.store.Delete(dbReq.Id)
}
