package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/kucicm/boomerang/src/dispatcher"
	"github.com/kucicm/boomerang/src/storage"

	_ "github.com/mattn/go-sqlite3"
)

type Request struct {
    Endpoint string `json:"endpoint"`
    Payload string `json:"payload"`
    SendAfter uint64 `json:"sendAfter"`
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

    log.Printf("got %+v\n", req)
    storeReq := storage.StorageRequest{
        Endpoint: req.Endpoint,
        Payload: req.Payload,
        SendAfter: req.SendAfter,
    }
    if err := s.store.Save(storeReq); err != nil {
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


