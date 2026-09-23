package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/yourusername/distributed-event-bus/broker"
	"github.com/yourusername/distributed-event-bus/transport/tcp"
)

func main() {
	address := flag.String("address", "127.0.0.1:19400", "event bus TCP address")
	topic := flag.String("topic", "ack-demo", "topic to publish to")
	messageID := flag.String("message-id", "demo-message-1", "message ID to publish twice")
	flag.Parse()

	client, err := tcp.Dial(context.Background(), *address, "ack-demo")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	event := broker.Event{
		ID:             *messageID,
		IdempotencyKey: *messageID,
		Topic:          *topic,
		Payload:        []byte("ack demonstration"),
	}

	first, err := client.PublishWithResult(ctx, event)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("first publish: status=%s message_id=%s", statusName(first), event.ID)

	second, err := client.PublishWithResult(ctx, event)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("second publish: status=%s message_id=%s", statusName(second), event.ID)
}

func statusName(status tcp.PublishStatus) string {
	switch status {
	case tcp.PublishAccepted:
		return "published"
	case tcp.PublishDuplicate:
		return "duplicate"
	default:
		return "unknown"
	}
}
