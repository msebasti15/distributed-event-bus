package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/yourusername/distributed-event-bus/broker"
	"github.com/yourusername/distributed-event-bus/logging"
	"github.com/yourusername/distributed-event-bus/transport/tcp"
)

func main() {
	address := flag.String("listen", ":9000", "TCP address to listen on")
	queueSize := flag.Int("queue-size", 128, "per-consumer queue size")
	cacheSize := flag.Int("cache-size", 1024, "per-topic cache size for events published without consumers")
	deduplicationSize := flag.Int("deduplication-size", 4096, "number of recent message IDs retained for deduplication")
	drop := flag.Bool("drop", false, "drop events when a consumer queue is full")
	httpAddress := flag.String("http-listen", ":9001", "HTTP control address")
	shutdownTimeout := flag.Duration("shutdown-timeout", 30*time.Second, "maximum graceful shutdown duration")
	redirect := flag.String("redirect", "", "replacement broker address announced during shutdown")
	logLevel := flag.String("log-level", "info", "log level: error, warn, info, debug")
	flag.Parse()
	level, validLevel := logging.ParseLevel(*logLevel)
	if !validLevel {
		panic("invalid -log-level; use error, warn, info, or debug")
	}
	logger := logging.New(level, os.Stderr)

	policy := broker.Block
	if *drop {
		policy = broker.Drop
	}
	bus := broker.New(broker.Config{QueueSize: *queueSize, CacheSize: *cacheSize, DeduplicationSize: *deduplicationSize, Backpressure: policy})
	server := tcp.NewServer(bus)
	server.SetLogger(logger)
	admin := &http.Server{Addr: *httpAddress}
	mux := http.NewServeMux()
	mux.HandleFunc("/state", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"server": server.State(), "broker": bus.State()})
	})
	shutdownDone := make(chan struct{})
	var shutdownOnce sync.Once
	triggerShutdown := func(redirectAddress string) {
		shutdownOnce.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
			defer cancel()
			if err := server.GracefulShutdown(ctx, redirectAddress); err != nil {
				logger.Errorf("graceful shutdown: %v", err)
			}
			_ = bus.Close()
			_ = admin.Shutdown(context.Background())
			close(shutdownDone)
		})
	}
	mux.HandleFunc("/shutdown", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "graceful shutdown started"})
		redirectAddress := r.URL.Query().Get("redirect")
		if redirectAddress == "" {
			redirectAddress = *redirect
		}
		go triggerShutdown(redirectAddress)
	})
	admin.Handler = mux
	go func() {
		if err := admin.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Errorf("http control server: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-stop
		triggerShutdown(*redirect)
	}()

	logger.Infof("starting event bus tcp=%s http=%s queue_size=%d cache_size=%d deduplication_size=%d backpressure=%s log_level=%s", *address, *httpAddress, *queueSize, *cacheSize, *deduplicationSize, policyName(policy), *logLevel)
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.ListenAndServe(*address) }()
	if err := <-serverDone; err != nil {
		logger.Errorf("TCP server: %v", err)
		return
	}
	select {
	case <-shutdownDone:
	case <-time.After(*shutdownTimeout + time.Second):
		logger.Errorf("shutdown completion timeout")
	}
}

func policyName(policy broker.BackpressurePolicy) string {
	if policy == broker.Drop {
		return "drop"
	}
	return "block"
}
