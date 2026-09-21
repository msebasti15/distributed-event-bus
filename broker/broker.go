// Package broker implements the in-memory core of the event bus.
package broker

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrClosed       = errors.New("broker is closed")
	ErrDraining     = errors.New("broker is draining")
	ErrQueueFull    = errors.New("subscriber queue is full")
	ErrSubscription = errors.New("subscription is closed")
)

// BackpressurePolicy controls what Publish does when a subscriber queue is full.
type BackpressurePolicy int

const (
	// Block waits until the subscriber accepts the event or the context is cancelled.
	Block BackpressurePolicy = 0
	// Drop rejects delivery to a full subscriber queue and returns ErrQueueFull.
	Drop = 1
)

// Event is the unit transported by the broker. Payload is intentionally opaque.
type Event struct {
	Topic   string
	Key     string
	Payload []byte
}

// Config controls broker behavior.
type Config struct {
	QueueSize         int
	CacheSize         int
	Backpressure      BackpressurePolicy
}

// Broker routes events to subscriptions. It is safe for concurrent use.
type Broker struct {
	mu       sync.RWMutex
	closed   bool
	draining bool
	config   Config
	sequence uint64
	topics   map[string]map[uint64]*Subscription
	cache    map[string][]Event
}

// State is a snapshot of broker activity.
type State struct {
	Closed        bool `json:"closed"`
	Draining      bool `json:"draining"`
	Topics        int  `json:"topics"`
	Subscriptions int  `json:"subscriptions"`
	PendingEvents int  `json:"pending_events"`
	CachedEvents  int  `json:"cached_events"`
}

// State returns a snapshot suitable for operational endpoints.
func (b *Broker) State() State {
	b.mu.RLock()
	defer b.mu.RUnlock()
	state := State{Closed: b.closed, Draining: b.draining, Topics: len(b.topics)}
	for _, subscribers := range b.topics {
		state.Subscriptions += len(subscribers)
		for _, subscription := range subscribers {
			state.PendingEvents += len(subscription.events)
		}
	}
	for _, events := range b.cache {
		state.CachedEvents += len(events)
	}
	return state
}

// New creates a broker with the supplied configuration.
func New(config Config) *Broker {
	if config.QueueSize < 1 {
		config.QueueSize = 1
	}
	if config.CacheSize < 0 {
		config.CacheSize = 0
	}
	return &Broker{
		config: config,
		topics: make(map[string]map[uint64]*Subscription),
		cache:  make(map[string][]Event),
	}
}

// Subscribe registers a consumer for topic. Events are delivered in publish order
// for this subscription. The returned channel is closed when the subscription ends.
func (b *Broker) Subscribe(topic string) (*Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed || b.draining {
		return nil, ErrClosed
	}
	b.sequence++
	s := newSubscription(b, topic, b.sequence, b.config.QueueSize)
	if b.topics[topic] == nil {
		b.topics[topic] = make(map[uint64]*Subscription)
	}
	b.topics[topic][s.id] = s
	if cached := b.cache[topic]; len(cached) > 0 {
		replayCount := len(cached)
		if replayCount > cap(s.events) {
			replayCount = cap(s.events)
		}
		for _, event := range cached[:replayCount] {
			s.events <- event
		}
		if replayCount == len(cached) {
			delete(b.cache, topic)
		} else {
			b.cache[topic] = cached[replayCount:]
		}
	}
	return s, nil
}

// BeginShutdown stops new subscriptions and publishes while existing
// subscriptions remain available for draining.
func (b *Broker) BeginShutdown() {
	b.mu.Lock()
	b.draining = true
	b.mu.Unlock()
}

// Publish routes event to every current subscriber of event.Topic.
func (b *Broker) Publish(ctx context.Context, event Event) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrClosed
	}
	if b.draining {
		b.mu.Unlock()
		return ErrDraining
	}
	subscribers := make([]*Subscription, 0, len(b.topics[event.Topic]))
	for _, s := range b.topics[event.Topic] {
		subscribers = append(subscribers, s)
	}
	policy := b.config.Backpressure
	if len(subscribers) == 0 {
		b.cacheEvent(event)
		b.mu.Unlock()
		return nil
	}
	b.mu.Unlock()

	for _, s := range subscribers {
		if err := s.enqueue(ctx, event, policy); err != nil {
			return err
		}
	}
	return nil
}

// Drain waits until all currently queued events for the subscription are read,
// then closes it. No new events can be enqueued after Broker.BeginShutdown.
func (s *Subscription) Drain(ctx context.Context) error {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if len(s.events) == 0 {
			return s.Close()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (b *Broker) cacheEvent(event Event) {
	if b.config.CacheSize == 0 {
		return
	}
	cached := b.cache[event.Topic]
	if len(cached) >= b.config.CacheSize {
		copy(cached, cached[1:])
		cached = cached[:len(cached)-1]
	}
	b.cache[event.Topic] = append(cached, event)
}

// Close stops the broker and closes all subscription channels.
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	all := make([]*Subscription, 0)
	for _, subscribers := range b.topics {
		for _, s := range subscribers {
			all = append(all, s)
		}
	}
	b.topics = make(map[string]map[uint64]*Subscription)
	b.mu.Unlock()

	for _, s := range all {
		s.close(false)
	}
	return nil
}

// Subscription represents one ordered consumer queue.
type Subscription struct {
	broker *Broker
	topic  string
	id     uint64
	events chan Event
	done   chan struct{}
	once   sync.Once
	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
}

func newSubscription(b *Broker, topic string, id uint64, queueSize int) *Subscription {
	return &Subscription{broker: b, topic: topic, id: id, events: make(chan Event, queueSize), done: make(chan struct{})}
}

// Events returns the subscription's ordered event stream.
func (s *Subscription) Events() <-chan Event { return s.events }

// Close removes the subscription and closes its event stream.
func (s *Subscription) Close() error {
	s.broker.mu.Lock()
	if subscribers := s.broker.topics[s.topic]; subscribers != nil {
		delete(subscribers, s.id)
		if len(subscribers) == 0 {
			delete(s.broker.topics, s.topic)
		}
	}
	s.broker.mu.Unlock()
	s.close(true)
	return nil
}

func (s *Subscription) close(drain bool) {
	s.once.Do(func() {
		s.mu.Lock()
		s.closed = true
		close(s.done)
		s.mu.Unlock()
		s.wg.Wait()

		if !drain {
			for {
				select {
				case <-s.events:
				default:
					close(s.events)
					return
				}
			}
		}
		close(s.events)
	})
}

func (s *Subscription) enqueue(ctx context.Context, event Event, policy BackpressurePolicy) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrSubscription
	}
	s.wg.Add(1)
	s.mu.Unlock()
	defer s.wg.Done()

	select {
	case <-s.done:
		return ErrSubscription
	default:
	}

	if policy == Drop {
		select {
		case <-s.done:
			return ErrSubscription
		case s.events <- event:
			return nil
		default:
			return ErrQueueFull
		}
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return ErrSubscription
	case s.events <- event:
		return nil
	}
}
