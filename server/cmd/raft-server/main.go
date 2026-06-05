package main

import (
	"log"
	"net/http"
	"os"

	"github.com/raft/raft/server/internal/sync"
)

func main() {
	addr := env("RAFT_ADDR", ":8080")
	hub := sync.NewHub()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/rooms/", hub.ServeRoom)

	log.Printf("raft sync server listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func env(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
