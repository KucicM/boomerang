package server

import (
	"bytes"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type DispatcherCfg struct {
    LoadBatchSize uint
    MaxConcurrency uint
}

type sendResult struct {
    req ScheduleRequest
    success bool
    timeTaken int64 // ns
}

type storage interface {
    Load(bs uint) []ScheduleRequest
    Update(req ScheduleRequest)
    Delete(req ScheduleRequest)
}

type dispatcher struct {
    cfg DispatcherCfg
    store storage
    stopSingal int32
    wg sync.WaitGroup
    semaphore chan struct{}
}

var dispathcer *dispatcher
var onceDispathcer sync.Once
func NewDispatcher(cfg DispatcherCfg,store storage) *dispatcher {
    onceDispathcer.Do(func() {
        dispathcer = &dispatcher{
            cfg: cfg,
            store: store,
            stopSingal: 0,
            wg: sync.WaitGroup{},
            semaphore: make(chan struct{}, cfg.MaxConcurrency),
        }
    })
    return dispathcer
}

var startOnlyOnce sync.Once
func (d *dispatcher) Start() {
    startOnlyOnce.Do(func() {
        d.wg.Add(1)
        go func() {
            defer d.wg.Done()
            for atomic.LoadInt32(&d.stopSingal) == 0 {
                batch := d.loadBatch()
                results := d.sendBatch(batch)
                d.finalizeCall(results)
            }
        }()
    })
}

func (d *dispatcher) loadBatch() []ScheduleRequest {
    return d.store.Load(d.cfg.LoadBatchSize)
}

func (d *dispatcher) sendBatch(batch []ScheduleRequest) <-chan sendResult {
    ret := make(chan sendResult, len(batch))

    if len(batch) == 0 {
        time.Sleep(time.Second)
        close(ret)
        return ret
    }

    wg := &sync.WaitGroup{}
    wg.Add(len(batch))

    for _, req := range batch {
        d.semaphore <- struct{}{}
        go d.doCall(req, ret, wg)
    }

    go func(w *sync.WaitGroup, c chan sendResult) {
        w.Wait()
        close(c)
    }(wg, ret)

    return ret
}

func (d *dispatcher) doCall(req ScheduleRequest, res chan sendResult, wg *sync.WaitGroup) {
    var success bool
    defer func(success bool, start time.Time) {
        res <- sendResult{req: req, success: success, timeTaken: time.Since(start).Nanoseconds()}
        <- d.semaphore
        wg.Done()
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
        log.Printf("error calling %s %s\n", req.Endpoint, err)
        return
    }
    defer resp.Body.Close()

    if resp.StatusCode < 500 {
        success = true
    }
}

func (d *dispatcher) finalizeCall(results <-chan sendResult) {
    for res := range results {
        req := res.req
        if res.success || !isValidForRetry(req){
            d.store.Delete(req)
        } else {
            req.SendAfter += req.BackOffMs
            req.MaxRetry -= 1
            d.store.Update(req)
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
