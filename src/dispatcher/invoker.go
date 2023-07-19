package dispatcher

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/kucicm/boomerang/src/storage"
	"github.com/prometheus/client_golang/prometheus"
)

const PERIOD = 5 * time.Second

var activeCallsGuage = prometheus.NewGauge(prometheus.GaugeOpts{Name: "active_invoke_calls", Help: "Current active calls to other service from invoker"})
var callHistogram = prometheus.NewHistogramVec(
    prometheus.HistogramOpts{Name: "invoker_call", Help: "Every call made by invoker"},
    []string{"success", "http_status"},
)
var retryCounter = prometheus.NewCounter(prometheus.CounterOpts{Name: "invoker_retry", Help: "Number of retries in invoker"})

func init() {
    prometheus.MustRegister(
        activeCallsGuage,
        callHistogram,
        retryCounter,
    )
}

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
        activeCallsGuage.Set(float64(len(semaphore)))
    }

    close(semaphore)
    for len(semaphore) > 0 {
        <-semaphore
        activeCallsGuage.Set(float64(len(semaphore)))
    }

    inv.done <-struct{}{}
}


func (inv *Invoker) invoke(dbItem storage.StorageItem, semaphore chan struct{}) {
    start := time.Now()

    req, err := http.NewRequest(http.MethodPost, dbItem.Endpoint, bytes.NewBufferString(dbItem.Payload))
    if err != nil {
        retryCounter.Inc()
        inv.retry.Retry(dbItem)
        log.Printf("Failed to create http request %v\n", err)
        return
    }

    var headers map[string]string
    if err = json.Unmarshal([]byte(dbItem.Headers), &headers); err != nil && len(dbItem.Headers) > 0 {
        retryCounter.Inc()
        inv.retry.Retry(dbItem)
        log.Printf("Failed to add headers %v\n", err)
        return
    }

    for k, v := range headers {
        req.Header.Add(k, v)
    }

    client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        callHistogram.WithLabelValues(strconv.FormatBool(false), resp.Status).Observe(float64(time.Since(start)))
        retryCounter.Inc()
        inv.retry.Retry(dbItem)
        log.Printf("Error on call %v\n", err)
        return
    }
    defer resp.Body.Close()

    callHistogram.WithLabelValues(strconv.FormatBool(true), resp.Status).Observe(float64(time.Since(start)))

    inv.store.Delete(dbItem)

    <- semaphore
}

func (inv *Invoker) Shutdown() error {
    log.Println("Invoker shutdown...")
    atomic.StoreInt32(&inv.status, 1)
    <-inv.done
    return nil
}
