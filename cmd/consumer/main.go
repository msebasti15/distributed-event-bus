package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/yourusername/distributed-event-bus/transport/tcp"
)

func main() {
	address := flag.String("address", "127.0.0.1:9000", "event bus TCP address")
	standby := flag.String("standby", "", "comma-separated standby broker addresses")
	topic := flag.String("topic", "events", "topic to consume")
	clientID := flag.String("client-id", "consumer-cli", "consumer client ID")
	flag.Parse()

	endpoints := []string{*address}
	if *standby != "" {
		endpoints = append(endpoints, strings.Split(*standby, ",")...)
	}
	client, err := tcp.NewConnectionManager(context.Background(), endpoints, *clientID)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	if err := client.Subscribe(*topic); err != nil {
		log.Fatal(err)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	log.Printf("subscribed client=%s topic=%s", *clientID, *topic)

	for {
		select {
		case event, ok := <-client.Events():
			if !ok {
				return
			}
			log.Printf("received topic=%s key=%s payload=%q", event.Topic, event.Key, event.Payload)
		case redirect, ok := <-client.Shutdowns():
			if !ok {
				return
			}
			if redirect != "" {
				log.Printf("broker is shutting down; reconnect to %s", redirect)
			} else {
				log.Printf("broker is shutting down")
			}
			// ConnectionManager handles reconnect and re-subscription.
		case <-stop:
			return
		}
	}
}
