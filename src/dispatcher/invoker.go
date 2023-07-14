package dispatcher

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/kucicm/boomerang/src/storage"
)

type Invoker struct {
    store *storage.StorageManager
}

func NewInvoker(store *storage.StorageManager) *Invoker {
    invoker := &Invoker{store}
    go invoker.start()

    return  invoker
}

func (i *Invoker) start() {
    for {
        dbReqs, err := i.store.Load()
        if err != nil {
            log.Printf("Cannot load items %v\n", err)
            continue
        }
        for _, dbReq := range dbReqs {
            req, err := http.NewRequest(http.MethodPost, dbReq.Endpoint, bytes.NewBufferString(dbReq.Payload))
            if err != nil {
                log.Printf("Failed to create http request %v\n", err)
                continue
            }

            client := &http.Client{}
            resp, err := client.Do(req)
            if err != nil {
                log.Printf("Error on call %v\n", err)
                continue
            }
            defer resp.Body.Close()

            body, err := io.ReadAll(resp.Body)
            if err != nil {
                log.Printf("Error reading body %v\n", err)
                continue
            }

            log.Println(string(body))

            i.store.Delete(dbReq.Id)
        }

        time.Sleep(5 * time.Second)
    }
}

func (i *Invoker) Stop() {
}

