package server

import (
	"bytes"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type sendResult struct {
    req ScheduleRequest
    success bool
    timeTaken int64 // ns
}

type storage interface {
    Load(bs uint) ([]ScheduleRequest, error)
    Update(req ScheduleRequest) error
    Delete(req ScheduleRequest) error
}

type dispatcher struct {
    store storage
    stopSingal int32
    wg sync.WaitGroup
}

func NewDispatcher(store storage) *dispatcher {
    return &dispatcher{
    	store: store,
    }
}

func (d *dispatcher) Start() {
    d.wg.Add(1)
    go func() {
        defer d.wg.Done()
        for atomic.LoadInt32(&d.stopSingal) == 0 {
            batch := d.loadBatch()
            results := d.sendBatch(batch)
            d.finalizeCall(results)
        }
    }()
}

func (d *dispatcher) loadBatch() []ScheduleRequest {
    reqs, err := d.store.Load(10) // todo batch should be from config
    if err != nil {
        log.Printf("cannot get batch of requests %s\n", err)
        return []ScheduleRequest{}
    }
    return reqs
}

func (d *dispatcher) sendBatch(batch []ScheduleRequest) <-chan sendResult {
    ret := make(chan sendResult, len(batch))
    defer close(ret)

    wg := &sync.WaitGroup{}
    wg.Add(len(batch))

    for _, req := range batch {
        go func(r ScheduleRequest, c chan sendResult, w *sync.WaitGroup) {
            defer w.Done()
            c <- sendResult{req: r}
        }(req, ret, wg)
    }

    go func(w *sync.WaitGroup, c chan sendResult) {
        w.Wait()
        close(c)
    }(wg, ret)

    return ret
}

func (d *dispatcher) doCall(req ScheduleRequest, res chan sendResult, wg *sync.WaitGroup) {
    defer wg.Done()
    var success bool

    defer func(success bool, start time.Time) {
        res <- sendResult{req: req, success: success, timeTaken: time.Since(start).Nanoseconds()}
    }(success, time.Now())

    httpReq, err := http.NewRequest(http.MethodPost, req.Endpoint, bytes.NewBufferString(req.Payload))
    if err != nil {
        log.Printf("Failed to create http request %v\n", err)
        return
    }

    for k, v := range req.Headers {
        httpReq.Header.Add(k, v)
    }

    client := &http.Client{}
    resp, err := client.Do(httpReq)
    if err != nil {
        log.Printf("error calling %s %s", req.Endpoint, err)
        return
    }
    defer resp.Body.Close()
    success = true
}

func (d *dispatcher) finalizeCall(results <-chan sendResult) {
    for res := range results {
        req := res.req
        if res.success {
            _ = d.store.Delete(req) // todo handle error
        } else {
            // validate should retry
            // if not delete
            // if yes
            req.SendAfter += req.BackOffMs
            req.MaxRetry -= 1
            _ = d.store.Update(req) // todo handle error
        }
    }
}

func (d *dispatcher) Shutdown() error {
    log.Println("Shutdown dispatcher...")
    atomic.StoreInt32(&d.stopSingal, 1)
    log.Println("Wait for dispathcer shutdown")
    d.wg.Wait()
    log.Println("dispatcher shutdown done")
    return nil
}
