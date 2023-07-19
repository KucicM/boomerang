package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/kucicm/boomerang/src/dispatcher"
	"github.com/kucicm/boomerang/src/storage"
	"github.com/prometheus/client_golang/prometheus"
)

var submitHistogram = prometheus.NewHistogramVec(
    prometheus.HistogramOpts{
        Name: "submit_requests", 
        Help: "Every call on endpoint /submnit",
    },
    []string{"success"},
)

func init() {
    prometheus.MustRegister(submitHistogram)
}

type Request struct {
    Endpoint string `json:"endpoint"`
    Payload string `json:"payload"`
    SendAfter uint64 `json:"sendAfter"`
    MaxRetry int `json:"maxRetry"`
    BackOffMs uint64 `json:"backOffMs"`
    Headers string `json:"headers"`
}

type Server struct {
    store *storage.StorageManager
    inv *dispatcher.Invoker
}

func NewServer() *Server {
    mng, err := storage.NewStorageManager()
    if err != nil {
        log.Fatal(err)
    }

    inv := dispatcher.NewInvoker(mng)

    return &Server{
        store: mng,
        inv: inv,
    }
}

func (s *Server) AcceptRequest(w http.ResponseWriter, r *http.Request) {
    var success = false
    defer func(start time.Time) {
        submitHistogram.WithLabelValues(strconv.FormatBool(success)).Observe(float64(time.Since(start).Milliseconds()))
    }(time.Now())

    if r.Method != http.MethodPost {
        w.WriteHeader(http.StatusMethodNotAllowed)
        return
    }

    body, err := ioutil.ReadAll(r.Body)
    if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        fmt.Fprintf(w, "Error reading request body %v", err)
        log.Printf("Error in submit %v\n", err)
        return
    }
    defer r.Body.Close()

    var req Request
    if err = json.Unmarshal(body, &req); err != nil {
        w.WriteHeader(http.StatusBadRequest)
        fmt.Fprintf(w, "Cannot parse request body %v", err)
        log.Printf("Cannot parse request body %v\n", err)
        return
    }

    storeReq := storage.StorageItem{
        Endpoint: req.Endpoint,
        Payload: req.Payload,
        SendAfter: req.SendAfter,
        MaxRetry: req.MaxRetry,
        BackOffMs: req.BackOffMs,
        Headers: req.Headers,
    }
    if err := s.store.Save(storeReq); err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        fmt.Fprintf(w, "Cannot save request to database %v", err)
        return
    }

    fmt.Fprintf(w, "%s", body)
    success = true
}

func (s *Server) Shutdown() error {
    log.Println("Server shutdown")

    var ret error
    if err := s.inv.Shutdown(); err != nil {
        log.Printf("error shutting down invoker %s\n", err)
        ret = err
    }

    if err := s.store.Shutdown(); err != nil {
        log.Printf("error shutting down store manager %s\n", err)
        ret = err
    }
    return ret
}
