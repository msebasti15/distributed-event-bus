package broker

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPublishFansOutToSubscribers(t *testing.T) {
	b := New(Config{QueueSize: 2})
	defer b.Close()

	a, err := b.Subscribe("orders")
	if err != nil { t.Fatal(err) }
	c, err := b.Subscribe("orders")
	if err != nil { t.Fatal(err) }

	want := Event{Topic: "orders", Key: "order-1", Payload: []byte("created")}
	if err := b.Publish(context.Background(), want); err != nil { t.Fatal(err) }
	for _, sub := range []*Subscription{a, c} {
		select {
		case got := <-sub.Events():
			if got.Topic != want.Topic || got.Key != want.Key || string(got.Payload) != string(want.Payload) { t.Fatalf("got %+v, want %+v", got, want) }
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for event")
		}
	}
}

func TestDropPolicyReportsFullQueue(t *testing.T) {
	b := New(Config{QueueSize: 1, Backpressure: Drop})
	defer b.Close()
	sub, err := b.Subscribe("metrics")
	if err != nil { t.Fatal(err) }

	if err := b.Publish(context.Background(), Event{Topic: "metrics", Payload: []byte("one")}); err != nil { t.Fatal(err) }
	if err := b.Publish(context.Background(), Event{Topic: "metrics", Payload: []byte("two")}); !errors.Is(err, ErrQueueFull) { t.Fatalf("got %v, want ErrQueueFull", err) }
	<-sub.Events()
}

func TestBlockPolicyHonorsContext(t *testing.T) {
	b := New(Config{QueueSize: 1, Backpressure: Block})
	defer b.Close()
	_, err := b.Subscribe("events")
	if err != nil { t.Fatal(err) }
	if err := b.Publish(context.Background(), Event{Topic: "events"}); err != nil { t.Fatal(err) }

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := b.Publish(ctx, Event{Topic: "events"}); !errors.Is(err, context.DeadlineExceeded) { t.Fatalf("got %v, want deadline exceeded", err) }
}

func TestCloseClosesSubscriptions(t *testing.T) {
	b := New(Config{QueueSize: 1})
	sub, err := b.Subscribe("events")
	if err != nil { t.Fatal(err) }
	if err := b.Close(); err != nil { t.Fatal(err) }
	if _, ok := <-sub.Events(); ok { t.Fatal("subscription channel is still open") }
	if err := b.Publish(context.Background(), Event{Topic: "events"}); !errors.Is(err, ErrClosed) { t.Fatalf("got %v, want ErrClosed", err) }
}

func TestUnconsumedEventsAreReplayedFromTopicCache(t *testing.T) {
	b := New(Config{QueueSize: 4, CacheSize: 4})
	defer b.Close()

	want := Event{Topic: "orders", Key: "order-1", Payload: []byte("created")}
	if err := b.Publish(context.Background(), want); err != nil { t.Fatal(err) }
	sub, err := b.Subscribe("orders")
	if err != nil { t.Fatal(err) }

	select {
	case got := <-sub.Events():
		if got.Topic != want.Topic || got.Key != want.Key || string(got.Payload) != string(want.Payload) { t.Fatalf("got %#v, want %#v", got, want) }
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for cached event")
	}
}

func TestTopicCacheKeepsNewestEvents(t *testing.T) {
	b := New(Config{QueueSize: 4, CacheSize: 2})
	defer b.Close()
	for _, payload := range []string{"one", "two", "three"} {
		if err := b.Publish(context.Background(), Event{Topic: "events", Payload: []byte(payload)}); err != nil { t.Fatal(err) }
	}
	sub, err := b.Subscribe("events")
	if err != nil { t.Fatal(err) }

	first := <-sub.Events()
	second := <-sub.Events()
	if got := string(first.Payload); got != "two" { t.Fatalf("got %q, want %q", got, "two") }
	if got := string(second.Payload); got != "three" { t.Fatalf("got %q, want %q", got, "three") }
}
