package main

import (
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/kucicm/boomerang/src/server"
	_ "github.com/mattn/go-sqlite3"
)


func main() {
    srv := server.NewServer()
    http.HandleFunc("/submit", srv.AcceptRequest)

    go func() {
        if err := http.ListenAndServe(":8888", nil); !errors.Is(err, http.ErrServerClosed) {
            log.Fatalf("Server failed %s\n", err)
        }
        log.Println("Server stopping...")
    }()


    // wait for shutdown
    ch := make(chan os.Signal, 1)
    signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
    <-ch

    if err := srv.Shutdown(); err != nil {
        log.Fatalf("Failed to shutdown server %s", err)
    }
}


