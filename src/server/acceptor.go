package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var ( 
    summary = promauto.NewSummaryVec(
        prometheus.SummaryOpts{
            Name: "boomerang_submit_request",
            Help: "boomerang_submit_request",
        },
        []string{"status"},
    )
)

type ScheduleRequest struct {
    Id uint64
    Endpoint string `json:"endpoint"`
    Headers map[string]string `json:"headers"`
    Payload string `json:"payload"`
    SendAfter uint64 `json:"sendAfter"`
    MaxRetry int `json:"maxRetry"`
    BackOffMs uint64 `json:"backOffMs"`
    TimeToLive uint64 `json:"TimeToLive"`
}

type store interface {
    Save(ScheduleRequest) error
}

type accepter struct {
    store store
}

func NewAccepter(store store) *accepter {
    log.Println("Accepter init")
    return &accepter{store}
}

func (a *accepter) SubmitHandler(w http.ResponseWriter, r *http.Request) {
    var status = "ok"
    defer func(start time.Time) {
        summary.WithLabelValues(status).Observe(float64(time.Since(start).Nanoseconds()))
    }(time.Now())

    if r.Method != http.MethodPost { // TODO replace with new stuff
        w.WriteHeader(http.StatusMethodNotAllowed)
        status = "invalid method"
        return
    }

    body, err := io.ReadAll(r.Body)
    if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        fmt.Fprintf(w, "Error reading request body %v", err)
        log.Printf("Error in submit handler %v\n", err)
        status = "invalid body"
        return
    }
    defer r.Body.Close()

    var req ScheduleRequest
    if err = json.Unmarshal(body, &req); err != nil {
        w.WriteHeader(http.StatusBadRequest)
        fmt.Fprintf(w, "Cannot parse request body %v", err)
        log.Printf("Cannot parse schedule request body %v\n", err)
        status = "invalid request"
        return
    }

    if err := a.store.Save(req); err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        fmt.Fprintf(w, "Cannot save schadule request %v", err)
        status = "save fail"
        return
    }

    fmt.Fprintf(w, "%s", body) // debug
}

func (a *accepter) Shutdown() error {
    log.Println("Accepter shutdown")
    return nil
}
