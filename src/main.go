package main

import (
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/kucicm/boomerang/src/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)


func main() {
    srv := server.NewAccepter(nil)
    http.HandleFunc("/submit", srv.SubmitHandler)
    http.Handle("/metrics", promhttp.Handler())

    go func() {
        log.Println("Server started")
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


