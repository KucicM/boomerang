package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/kucicm/boomerang/src/storage"

    _ "github.com/mattn/go-sqlite3"
)

type Request struct {
    Endpoint string `json:"endpoint"`
    Payload string `json:"pyaload"`
    SendAfter uint64 `json:"sendAfter"`
}

type Server struct {
    queue *storage.PersistentQueue
}

func NewServer() *Server {

    queueCfg := storage.PersistentQueueCfg{DbURL: "test_data.db"}
    q, err := storage.NewQueue(queueCfg)
    if err != nil {
        log.Fatal(err)
    }

    go func() {
        for {
            log.Println("sleep")
            time.Sleep(5 * time.Second)
            vals, err := q.Pop()
            if err != nil {
                log.Println(err)
                continue
            } 

            for _, val := range vals {
                log.Printf("%+v\n", val)
            }
        }
    }()

    return &Server{
        queue: q,
    }
}

func (s *Server) AcceptRequest(w http.ResponseWriter, r *http.Request) {
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
        return
    }

    queueReq := storage.QueueRequest{
        Endpoint: req.Endpoint,
        Payload: req.Payload,
        SendAfter: req.SendAfter,
    }
    if _, err := s.queue.Push(queueReq); err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        fmt.Fprintf(w, "Cannot save request to database %v", err)
        return
    }

    fmt.Fprintf(w, "%s", body)
}

func main() {
    srv := NewServer()
    log.Println("servise started")
    http.HandleFunc("/submit", srv.AcceptRequest)
    log.Fatal(http.ListenAndServe(":8888", nil))
}


