package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/yourusername/distributed-event-bus/broker"
	"github.com/yourusername/distributed-event-bus/transport/tcp"
)

func main() {
	address := flag.String("address", "127.0.0.1:9000", "event bus TCP address")
	standby := flag.String("standby", "", "comma-separated standby broker addresses")
	topic := flag.String("topic", "events", "topic to publish to")
	key := flag.String("key", "", "event key")
	payload := flag.String("payload", "hello from producer", "event payload")
	count := flag.Int("count", 1, "number of events; use 0 to run forever")
	interval := flag.Duration("interval", time.Second, "delay between events")
	flag.Parse()

	endpoints := []string{*address}
	if *standby != "" {
		endpoints = append(endpoints, strings.Split(*standby, ",")...)
	}
	client, err := tcp.NewConnectionManager(context.Background(), endpoints, "producer-cli")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	go func() {
		for redirect := range client.Shutdowns() {
			if redirect == "" {
				log.Printf("broker is shutting down; producer must reconnect")
				continue
			}
			log.Printf("broker is shutting down; reconnect producer to %s", redirect)
		}
	}()

	for sequence := 1; *count == 0 || sequence <= *count; sequence++ {
		messageID, err := newMessageID()
		if err != nil {
			log.Fatal(err)
		}
		event := broker.Event{
			ID:      messageID,
			Topic:   *topic,
			Key:     *key,
			Payload: []byte(fmt.Sprintf("%s #%d", *payload, sequence)),
		}
		if err := client.Publish(event); err != nil {
			log.Fatal(err)
		}
		log.Printf("published (broker ACK received) id=%s topic=%s key=%s payload=%q", event.ID, event.Topic, event.Key, event.Payload)
		if *count == 0 || sequence < *count {
			time.Sleep(*interval)
		}
	}
}

func newMessageID() (string, error) {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(data[:]), nil
}
