package tcp

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/yourusername/distributed-event-bus/broker"
)

func TestPublishSubscribeOverTCP(t *testing.T) {
	bus := broker.New(broker.Config{QueueSize: 8})
	defer bus.Close()
	server := NewServer(bus)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan struct{})
	go func() {
		_ = server.Serve(listener)
		close(serveDone)
	}()
	defer func() {
		_ = server.Close()
		<-serveDone
	}()

	client, err := Dial(context.Background(), listener.Addr().String(), "integration-test")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Subscribe("orders"); err != nil {
		t.Fatal(err)
	}

	want := broker.Event{Topic: "orders", Key: "order-1", Payload: []byte("created")}
	if err := client.Publish(want); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-client.Events():
		if got.Topic != want.Topic || got.Key != want.Key || string(got.Payload) != string(want.Payload) {
			t.Fatalf("got %#v, want %#v", got, want)
		}
		if err := client.Acknowledge(got.ID); err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestDeliveryIsRetriedUntilAcknowledged(t *testing.T) {
	bus := broker.New(broker.Config{QueueSize: 8})
	defer bus.Close()
	server := NewServer(bus)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan struct{})
	go func() {
		_ = server.Serve(listener)
		close(serveDone)
	}()
	defer func() {
		_ = server.Close()
		<-serveDone
	}()

	client, err := Dial(context.Background(), listener.Addr().String(), "delivery-ack-test")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Subscribe("orders"); err != nil {
		t.Fatal(err)
	}

	event := broker.Event{ID: "delivery-1", Topic: "orders", Payload: []byte("created")}
	if err := client.Publish(event); err != nil {
		t.Fatal(err)
	}
	first := <-client.Events()
	if first.ID != event.ID {
		t.Fatalf("got event ID %q, want %q", first.ID, event.ID)
	}

	select {
	case duplicate := <-client.Events():
		if duplicate.ID != event.ID {
			t.Fatalf("got duplicate ID %q, want %q", duplicate.ID, event.ID)
		}
	case <-time.After(deliveryAckTimeout + time.Second):
		t.Fatal("timed out waiting for redelivery")
	}
	if err := client.Acknowledge(event.ID); err != nil {
		t.Fatal(err)
	}
}

func TestGracefulShutdownNotifiesClient(t *testing.T) {
	bus := broker.New(broker.Config{QueueSize: 8, CacheSize: 8})
	defer bus.Close()
	server := NewServer(bus)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan struct{})
	go func() { _ = server.Serve(listener); close(serveDone) }()

	client, err := Dial(context.Background(), listener.Addr().String(), "shutdown-test")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Subscribe("orders"); err != nil {
		t.Fatal(err)
	}

	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		shutdownDone <- server.GracefulShutdown(ctx, "127.0.0.1:29000")
	}()

	select {
	case redirect, ok := <-client.Shutdowns():
		if !ok {
			t.Fatal("shutdown notification channel closed before redirect")
		}
		if redirect != "127.0.0.1:29000" {
			t.Fatalf("got redirect %q", redirect)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for shutdown notification")
	}
	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
	<-serveDone
}

func TestConnectionManagerHandsOffToStandby(t *testing.T) {
	primaryBus := broker.New(broker.Config{QueueSize: 8})
	standbyBus := broker.New(broker.Config{QueueSize: 8})
	defer primaryBus.Close()
	defer standbyBus.Close()
	primary := NewServer(primaryBus)
	standby := NewServer(standbyBus)
	primaryListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	standbyListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	primaryDone := make(chan struct{})
	standbyDone := make(chan struct{})
	go func() { _ = primary.Serve(primaryListener); close(primaryDone) }()
	go func() { _ = standby.Serve(standbyListener); close(standbyDone) }()

	manager, err := NewConnectionManager(context.Background(), []string{primaryListener.Addr().String(), standbyListener.Addr().String()}, "handoff-test")
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	if err := manager.Subscribe("orders"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := primary.GracefulShutdown(ctx, standbyListener.Addr().String()); err != nil {
		t.Fatal(err)
	}
	select {
	case redirect := <-manager.Shutdowns():
		if redirect != standbyListener.Addr().String() {
			t.Fatalf("got redirect %q", redirect)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for connection handoff")
	}

	want := broker.Event{Topic: "orders", Key: "after-handoff", Payload: []byte("created")}
	if err := manager.Publish(want); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-manager.Events():
		if got.Topic != want.Topic || got.Key != want.Key || string(got.Payload) != string(want.Payload) {
			t.Fatalf("got %#v, want %#v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event after handoff")
	}
	_ = standby.Close()
	<-primaryDone
	<-standbyDone
}
