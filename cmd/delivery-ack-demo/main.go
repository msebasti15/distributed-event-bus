package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/yourusername/distributed-event-bus/transport/tcp"
)

func main() {
	address := flag.String("address", "127.0.0.1:19500", "event bus TCP address")
	topic := flag.String("topic", "delivery-demo", "topic to consume")
	ackAfter := flag.Duration("ack-after", 2500*time.Millisecond, "delay before acknowledging the first delivery")
	flag.Parse()

	client, err := tcp.Dial(context.Background(), *address, "delivery-ack-demo")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if err := client.Subscribe(*topic); err != nil {
		log.Fatal(err)
	}
	log.Printf("subscribed topic=%s; waiting for an event", *topic)

	event, ok := <-client.Events()
	if !ok {
		log.Fatal("event stream closed before delivery")
	}
	log.Printf("received id=%s; intentionally waiting %s before ACK", event.ID, *ackAfter)
	time.Sleep(*ackAfter)

	select {
	case retry := <-client.Events():
		log.Printf("redelivery observed id=%s", retry.ID)
	case <-time.After(500 * time.Millisecond):
		log.Printf("no redelivery observed before ACK")
	}

	if err := client.Acknowledge(event.ID); err != nil {
		log.Fatal(err)
	}
	log.Printf("acknowledged id=%s", event.ID)
}
