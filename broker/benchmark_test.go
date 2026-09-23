package broker

import (
	"context"
	"strconv"
	"testing"
)

func BenchmarkPublishWithoutSubscribers(b *testing.B) {
	bus := New(Config{CacheSize: 0})
	event := Event{Topic: "events", Payload: []byte("payload")}
	b.SetBytes(int64(len(event.Payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := bus.Publish(context.Background(), event); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPublishToOneSubscriber(b *testing.B) {
	bus := New(Config{QueueSize: 128})
	sub, err := bus.Subscribe("events")
	if err != nil {
		b.Fatal(err)
	}
	stop := make(chan struct{})
	go drainSubscription(sub, stop)
	defer close(stop)
	defer sub.Close()

	event := Event{Topic: "events", Payload: []byte("payload")}
	b.SetBytes(int64(len(event.Payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := bus.Publish(context.Background(), event); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPublishFanoutTenSubscribers(b *testing.B) {
	bus := New(Config{QueueSize: 128})
	stop := make(chan struct{})
	for i := 0; i < 10; i++ {
		sub, err := bus.Subscribe("events")
		if err != nil {
			b.Fatal(err)
		}
		go drainSubscription(sub, stop)
		defer sub.Close()
	}
	defer close(stop)

	event := Event{Topic: "events", Payload: []byte("payload")}
	b.SetBytes(int64(len(event.Payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := bus.Publish(context.Background(), event); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDuplicateMessageID(b *testing.B) {
	bus := New(Config{DeduplicationSize: b.N + 1})
	event := Event{ID: "stable-message-id", Topic: "events", Payload: []byte("payload")}
	if _, err := bus.PublishWithResult(context.Background(), event); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := bus.PublishWithResult(context.Background(), event)
		if err != nil {
			b.Fatal(err)
		}
		if result != Duplicate {
			b.Fatalf("got %v, want Duplicate", result)
		}
	}
}

func BenchmarkUniqueMessageIDs(b *testing.B) {
	bus := New(Config{DeduplicationSize: b.N + 1})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		event := Event{ID: "message-" + strconv.Itoa(i), Topic: "events", Payload: []byte("payload")}
		if _, err := bus.PublishWithResult(context.Background(), event); err != nil {
			b.Fatal(err)
		}
	}
}

func drainSubscription(sub *Subscription, stop <-chan struct{}) {
	for {
		select {
		case _, ok := <-sub.Events():
			if !ok {
				return
			}
		case <-stop:
			return
		}
	}
}
