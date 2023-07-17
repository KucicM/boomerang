package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"sync"
	"time"
)

type Request struct {
    ExpectedAfter int64 `json:"expected"`
}

type service struct {
    endpoint string

    rps int

    loadInterval time.Duration
    waitInterval time.Duration

    callbackTime time.Duration
    fixedCallbackTime bool

    repeat uint

    mu *sync.Mutex

    sendStats *statistics
    receiveStats *statistics
    msgStats *statistics
}

type statistics struct {
    lock *sync.Mutex
    min int64
    max int64
    avg int64
    cnt int64
}

func newStats() *statistics {
    return &statistics{&sync.Mutex{}, math.MaxInt64, math.MinInt64, 0, 0}
}

func (s *statistics) update(start, end int64) {
    diff := end - start
    s.lock.Lock()
    defer s.lock.Unlock()

    if diff < s.min {
        s.min = diff
    } else if diff > s.max {
        s.max = diff
    }

    s.avg = (s.cnt * s.avg + diff) / (s.cnt + 1)
    s.cnt += 1
}

func (s *statistics) log() string {
    s.lock.Lock()
    defer s.lock.Unlock()
    return fmt.Sprintf("cnt: %d\tmin: %dms\tavg:%dms\tmax: %dms", 
                s.cnt, s.min / 1000000, s.avg / 1000000, s.max / 1000000)
 }

func main() {
    port := flag.Int("port", 9999, "Port to listen")
    endpoint := flag.String("endpoint", "", "bomerang addr")

    targetRPS := flag.Int("rps", 10000, "Target requests per second")

    loadInterval := flag.Duration("load-interval", 10 * time.Second, "Period duration with load in the interval")
    waitInterval := flag.Duration("wait-interval", 10 * time.Second, "Period duration without load in the interval")
    callbackTime := flag.Duration("callback-time", 10 * time.Second, "Duration after is expected to have callback") 
    fiexdCallbackTime := flag.Bool("fixed-callback-time", true, "Is callback time fixed per interval or always increasing") 
    repeat := flag.Uint("repeat", 0, "How many time should interval be repeated, 0 = inf")
    flag.Parse()

    srv := &service{
        endpoint: *endpoint,

        rps: *targetRPS,

        loadInterval: *loadInterval,
        waitInterval: *waitInterval,
        callbackTime: *callbackTime,
        fixedCallbackTime: *fiexdCallbackTime,
        repeat: *repeat,

        mu: &sync.Mutex{},
        sendStats: newStats(),
        receiveStats: newStats(),
        msgStats: newStats(),
    }

    go srv.startLoad(*port)
    go srv.report()

    http.HandleFunc("/", srv.handleReq)
    log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
}

func (s *service) startLoad(port int) {
    template := `{"endpoint": "http://localhost:%d/", "payload": "{\"expected\": %d}", "sendAfter": %d}`
    for i := 0; i < int(s.repeat) || s.repeat == 0; i++ {
        s.load(template, port, time.Now().UnixNano())
        s.wait()
    }
}

func (s *service) load(template string, port int, start int64) {
    ticker := time.NewTicker(time.Second / time.Duration(s.rps))

    timeout := time.NewTimer(s.loadInterval)
    defer timeout.Stop()

    for {
        select {
        case <- ticker.C:
            go s.send(template, port, start)
        case <-timeout.C:
            return
        }
    }
}

func (s *service) wait() {
    time.Sleep(s.waitInterval)
}

func (s *service) send(template string, port int, start int64) {
    funStart := time.Now()
    var sendAfter int64
    if s.fixedCallbackTime {
        sendAfter = start + s.callbackTime.Nanoseconds()
    } else {
        sendAfter = funStart.Add(s.callbackTime).UnixNano()
    }

    payload := fmt.Sprintf(template, port, sendAfter, sendAfter / 1000 / 1000)
    req, err := http.NewRequest(http.MethodPost, s.endpoint, bytes.NewBuffer([]byte(payload)))
    if err != nil {
        log.Println(err)
        return
    }

    if resp, err := http.DefaultClient.Do(req); err != nil {
        log.Println(err)
    } else {
        resp.Body.Close()
    }

    s.sendStats.update(funStart.UnixNano(), time.Now().UnixNano())
}

func (s *service) handleReq(w http.ResponseWriter, r *http.Request) {
    start := time.Now().UnixNano()

    body, _ := ioutil.ReadAll(r.Body)
    defer r.Body.Close()

    var req Request
    if err := json.Unmarshal(body, &req); err != nil {
        log.Println(err)
    }

    s.receiveStats.update(start, time.Now().UnixNano())
    s.msgStats.update(req.ExpectedAfter, start)
}

func (s *service) report() {
    for {
        time.Sleep(1 * time.Second)
        sendStats := s.sendStats.log()
        receiveStats := s.receiveStats.log()
        msgStats := s.msgStats.log()
        log.Printf("\nsend:\t%s\nrec:\t%s\nmsg:\t%s\n", sendStats, receiveStats, msgStats)
    }
}
