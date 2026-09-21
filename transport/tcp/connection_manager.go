package tcp

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/yourusername/distributed-event-bus/broker"
)

var ErrNoBrokerAvailable = errors.New("no broker is currently available")

// ConnectionManager reconnects across an ordered list of brokers. The first
// endpoint is preferred whenever it becomes healthy again.
type ConnectionManager struct {
	mu        sync.RWMutex
	endpoints []string
	clientID  string
	current   *Client
	topics    map[string]struct{}
	events    chan broker.Event
	shutdowns chan string
	done      chan struct{}
	once      sync.Once
}

// NewConnectionManager connects to the first available endpoint and starts recovery.
func NewConnectionManager(ctx context.Context, endpoints []string, clientID string) (*ConnectionManager, error) {
	if len(endpoints) == 0 {
		return nil, ErrNoBrokerAvailable
	}
	manager := &ConnectionManager{
		endpoints: append([]string(nil), endpoints...),
		clientID:  clientID,
		topics:    make(map[string]struct{}),
		events:    make(chan broker.Event, 64),
		shutdowns: make(chan string, 1),
		done:      make(chan struct{}),
	}
	if err := manager.connectUntilAvailable(ctx); err != nil {
		return nil, err
	}
	go manager.run()
	return manager, nil
}

func (c *ConnectionManager) Events() <-chan broker.Event { return c.events }
func (c *ConnectionManager) Shutdowns() <-chan string   { return c.shutdowns }

func (c *ConnectionManager) Subscribe(topic string) error {
	c.mu.Lock()
	c.topics[topic] = struct{}{}
	client := c.current
	c.mu.Unlock()
	if client == nil {
		return ErrNoBrokerAvailable
	}
	return client.Subscribe(topic)
}

func (c *ConnectionManager) Publish(event broker.Event) error {
	for attempt := 0; attempt < 50; attempt++ {
		c.mu.RLock()
		client := c.current
		c.mu.RUnlock()
		if client != nil {
			if err := client.Publish(event); err == nil {
				return nil
			}
			// A failed write can race with ConnectionManager replacing the
			// connection. Retry after the manager selects another endpoint.
		}
		select {
		case <-c.done:
			return ErrClientClosed
		case <-time.After(100 * time.Millisecond):
		}
	}
	return ErrNoBrokerAvailable
}

func (c *ConnectionManager) Close() error {
	c.once.Do(func() {
		close(c.done)
		c.mu.Lock()
		client := c.current
		c.current = nil
		c.mu.Unlock()
		if client != nil {
			_ = client.Close()
		}
	})
	return nil
}

func (c *ConnectionManager) connect(ctx context.Context) error {
	c.mu.RLock()
	endpoints := append([]string(nil), c.endpoints...)
	c.mu.RUnlock()
	for _, endpoint := range endpoints {
		client, err := Dial(ctx, endpoint, c.clientID)
		if err != nil {
			continue
		}
		c.mu.Lock()
		c.current = client
		topics := make([]string, 0, len(c.topics))
		for topic := range c.topics {
			topics = append(topics, topic)
		}
		c.mu.Unlock()
		subscribed := true
		for _, topic := range topics {
			if err := client.Subscribe(topic); err != nil {
				_ = client.Close()
				c.mu.Lock()
				c.current = nil
				c.mu.Unlock()
				subscribed = false
				break
			}
		}
		if subscribed { return nil }
	}
	return ErrNoBrokerAvailable
}

func (c *ConnectionManager) connectUntilAvailable(ctx context.Context) error {
	for {
		if err := c.connect(ctx); err == nil {
			return nil
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}

func (c *ConnectionManager) run() {
	defer close(c.events)
	defer close(c.shutdowns)
	backoff := 100 * time.Millisecond
	for {
		c.mu.RLock()
		client := c.current
		c.mu.RUnlock()
		if client == nil {
			if !c.reconnect(backoff) {
				return
			}
			backoff = 100 * time.Millisecond
			continue
		}

		redirect, graceful := c.watch(client)
		if graceful && redirect != "" {
			c.prefer(redirect)
			select {
			case c.shutdowns <- redirect:
			default:
			}
		}
		c.mu.Lock()
		if c.current == client {
			c.current = nil
		}
		c.mu.Unlock()
		_ = client.Close()
		if !c.reconnect(backoff) {
			return
		}
		backoff = 100 * time.Millisecond
	}
}

func (c *ConnectionManager) watch(client *Client) (string, bool) {
	for {
		select {
		case event, ok := <-client.Events():
			if !ok {
				select {
				case redirect, shutdownOK := <-client.Shutdowns():
					if shutdownOK { return redirect, true }
				default:
				}
				return "", false
			}
			select {
			case c.events <- event:
			case <-c.done: return "", false
			}
		case redirect, ok := <-client.Shutdowns():
			if ok { return redirect, true }
			return "", false
		case <-client.Done():
			// The client closes Done immediately after publishing SHUTDOWN.
			// Drain the buffered notification before treating this as an abrupt
			// failure, otherwise a controlled migration loses its redirect.
			select {
			case redirect, ok := <-client.Shutdowns():
				if ok { return redirect, true }
			default:
			}
			return "", false
		case <-c.done:
			return "", false
		}
	}
}

func (c *ConnectionManager) reconnect(initial time.Duration) bool {
	delay := initial
	for {
		select {
		case <-c.done:
			return false
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := c.connect(ctx)
		cancel()
		if err == nil { return true }
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-c.done:
			timer.Stop()
			return false
		}
		if delay < 5*time.Second { delay *= 2 }
	}
}

func (c *ConnectionManager) prefer(endpoint string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ordered := []string{endpoint}
	for _, candidate := range c.endpoints {
		if candidate != endpoint { ordered = append(ordered, candidate) }
	}
	c.endpoints = ordered
}
