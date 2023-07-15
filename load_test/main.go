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
}

type statistics struct {
    min int64
    max int64
    avg int64
    cnt int64
}

func main() {
    port := flag.Int("port", 9999, "Port to listen")
    targetRPS := flag.Int("rps", 1000, "Target requests per second")
    endpoint := flag.String("endpoint", "", "bomerang addr")
    flag.Parse()

    srv := &service{
        rps: *targetRPS,
        endpoint: *endpoint,
        mu: &sync.Mutex{},
        stats: &statistics{math.MaxInt64, math.MinInt64, 0, 0},
    }

    go srv.spammy(*port)

    http.HandleFunc("/", srv.handleReq)
    log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
}

func (s *service) spammy(port int) {
    template := `{"endpoint": "http://localhost:%d/", "payload": "{\"created\": %d}", "sendAfter": %d}`
    ticker := time.NewTicker(time.Second / time.Duration(s.rps))
    for {
        select {
        case <- ticker.C:
            now := time.Now()
            payload := fmt.Sprintf(template, port, now.UnixMicro(), now.UnixMilli())
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
        }
    }
}

func (s *service) handleReq(w http.ResponseWriter, r *http.Request) {
    receivedTime := time.Now().UnixMicro()

    body, _ := ioutil.ReadAll(r.Body)
    defer r.Body.Close()

    var req Request
    if err := json.Unmarshal(body, &req); err != nil {
        log.Println(err)
    }

    s.log(req.Created, receivedTime)
}

func (s *service) log(created, received int64) {
    diff := received - created
    s.mu.Lock()
    defer s.mu.Unlock()

    if s.stats.max < diff {
        s.stats.max = diff
    }

    if s.stats.min > diff {
        s.stats.min = diff
    }

    totalTime := s.stats.avg * s.stats.cnt
    s.stats.avg = (totalTime + diff) / (s.stats.cnt + 1)
    s.stats.cnt += 1

    if s.stats.cnt % 1000 == 0{
        log.Printf("%+v\n", s.stats)
    }
}
