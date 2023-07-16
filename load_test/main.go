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
    Created int64 `json:"created"`
}

type service struct {
    rps int
    endpoint string
    mu *sync.Mutex
    stats *statistics

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
    targetRPS := flag.Int("rps", 280, "Target requests per second")
    endpoint := flag.String("endpoint", "", "bomerang addr")
    flag.Parse()

    srv := &service{
        rps: *targetRPS,
        endpoint: *endpoint,
        mu: &sync.Mutex{},
        sendStats: newStats(),
        receiveStats: newStats(),
        msgStats: newStats(),
    }

    go srv.spammy(*port)
    go srv.report()

    http.HandleFunc("/", srv.handleReq)
    log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
}

func (s *service) spammy(port int) {
    template := `{"endpoint": "http://localhost:%d/", "payload": "{\"created\": %d}", "sendAfter": %d}`
    ticker := time.NewTicker(time.Second / time.Duration(s.rps))
    for {
        select {
        case <- ticker.C:
            go func() {
                start := time.Now().UnixNano()

                now := time.Now()
                payload := fmt.Sprintf(template, port, now.UnixNano(), now.UnixMilli())
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

                s.sendStats.update(start, time.Now().UnixNano())
            }()
        }
    }
}

func (s *service) handleReq(w http.ResponseWriter, r *http.Request) {
    start := time.Now().UnixNano()

    body, _ := ioutil.ReadAll(r.Body)
    defer r.Body.Close()

    var req Request
    if err := json.Unmarshal(body, &req); err != nil {
        log.Println(err)
    }

    s.msgStats.update(req.Created, start)
    s.receiveStats.update(start, time.Now().UnixNano())
}

func (s *service) report() {
    for {
        time.Sleep(1 * time.Second)
        sendStats := s.sendStats.log()
        receiveStats := s.receiveStats.log()
        msgStats := s.msgStats.log()
        log.Printf("\nsend: %s\nrec: %s\nmsg: %s\n", sendStats, receiveStats, msgStats)
    }
}
