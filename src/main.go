package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

type Server struct {

}

func NewServer() *Server {
    return &Server{}
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

    fmt.Fprintf(w, "%s", body)
}

func main() {
    srv := NewServer()

    http.HandleFunc("/submit", srv.AcceptRequest)
    log.Fatal(http.ListenAndServe(":8888", nil))
}


