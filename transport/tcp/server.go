// Package tcp provides the TCP transport for the event broker.
package tcp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/yourusername/distributed-event-bus/broker"
	"github.com/yourusername/distributed-event-bus/protocol"
)

var ErrNotConnected = errors.New("client must send CONNECT first")

// Server exposes a broker through the TCP wire protocol.
type Server struct {
	broker   *broker.Broker
	listener net.Listener
	mu       sync.Mutex
	closed   bool
	draining bool
	conns    map[*connection]struct{}
	wg       sync.WaitGroup
}

// NewServer creates a TCP server backed by b.
func NewServer(b *broker.Broker) *Server {
	return &Server{broker: b, conns: make(map[*connection]struct{})}
}

// ListenAndServe listens on address and handles clients until Close is called.
func (s *Server) ListenAndServe(address string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()
	return s.Serve(listener)
}

// Serve accepts connections from listener until it is closed.
func (s *Server) Serve(listener net.Listener) error {
	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()

	for {
		conn, err := listener.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}

		state := newConnection(s, conn)
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = conn.Close()
			continue
		}
		s.conns[state] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go func() {
			defer s.wg.Done()
			defer s.removeConnection(state)
			state.serve()
		}()
	}
}

// Close stops accepting new clients and closes active connections.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	listener := s.listener
	connections := make([]*connection, 0, len(s.conns))
	for conn := range s.conns {
		connections = append(connections, conn)
	}
	s.mu.Unlock()

	if listener != nil {
		_ = listener.Close()
	}
	for _, conn := range connections {
		conn.close()
	}
	s.wg.Wait()
	return nil
}

// GracefulShutdown stops accepting new connections and input messages, then
// drains every active subscription before notifying clients and closing them.
func (s *Server) GracefulShutdown(ctx context.Context, redirect string) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.draining = true
	listener := s.listener
	connections := make([]*connection, 0, len(s.conns))
	for conn := range s.conns {
		connections = append(connections, conn)
	}
	s.mu.Unlock()

	if listener != nil {
		_ = listener.Close()
	}
	s.broker.BeginShutdown()

	var drainWG sync.WaitGroup
	for _, conn := range connections {
		drainWG.Add(1)
		go func(conn *connection) {
			defer drainWG.Done()
			conn.gracefulClose(ctx, redirect)
		}(conn)
	}
	drainWG.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

// State is a snapshot suitable for operational endpoints.
type State struct {
	Closed            bool `json:"closed"`
	Draining          bool `json:"draining"`
	ActiveConnections int  `json:"active_connections"`
}

// State returns the current server lifecycle state.
func (s *Server) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return State{Closed: s.closed, Draining: s.draining, ActiveConnections: len(s.conns)}
}

func (s *Server) removeConnection(conn *connection) {
	s.mu.Lock()
	delete(s.conns, conn)
	s.mu.Unlock()
}

type connection struct {
	server *Server
	net    net.Conn
	write  sync.Mutex
	mu     sync.Mutex
	subs   map[string]*broker.Subscription
	closed bool
	draining bool
	deliveryWG sync.WaitGroup
}

func newConnection(server *Server, conn net.Conn) *connection {
	return &connection{server: server, net: conn, subs: make(map[string]*broker.Subscription)}
}

func (c *connection) serve() {
	defer c.abort()

	frame, err := protocol.ReadFrame(c.net)
	if err != nil {
		return
	}
	if frame.Type != protocol.TypeConnect {
		_ = c.sendError(ErrNotConnected)
		return
	}
	if _, err := decodeConnect(frame.Payload); err != nil {
		_ = c.sendError(fmt.Errorf("invalid CONNECT payload: %w", err))
		return
	}

	for {
		frame, err := protocol.ReadFrame(c.net)
		if err != nil {
			return
		}
		switch frame.Type {
		case protocol.TypeSubscribe:
			if err := c.subscribe(frame.Payload); err != nil {
				_ = c.sendError(err)
			}
		case protocol.TypePublish:
			if err := c.publish(frame.Payload); err != nil {
				_ = c.sendError(err)
			}
		case protocol.TypePing:
			_ = c.send(protocol.Frame{Type: protocol.TypePong})
		case protocol.TypeUnsubscribe:
			c.unsubscribe(frame.Payload)
		default:
			_ = c.sendError(fmt.Errorf("unsupported frame type: %d", frame.Type))
		}
	}
}

func (c *connection) subscribe(payload []byte) error {
	message, err := decodeTopic(payload)
	if err != nil || message.Topic == "" {
		return errors.New("invalid SUBSCRIBE payload")
	}
	sub, err := c.server.broker.Subscribe(message.Topic)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if old := c.subs[message.Topic]; old != nil {
		_ = old.Close()
	}
	c.subs[message.Topic] = sub
	c.mu.Unlock()

	c.deliveryWG.Add(1)
	go func() {
		defer c.deliveryWG.Done()
		for event := range sub.Events() {
			payload, err := encodeEvent(event)
			if err != nil || c.send(protocol.Frame{Type: protocol.TypeEvent, Payload: payload}) != nil {
				return
			}
		}
	}()
	return nil
}

func (c *connection) abort() {
	c.mu.Lock()
	draining := c.draining
	c.mu.Unlock()
	if !draining {
		c.close()
	}
}

func (c *connection) gracefulClose(ctx context.Context, redirect string) {
	c.mu.Lock()
	if c.closed || c.draining {
		c.mu.Unlock()
		return
	}
	c.draining = true
	subs := make([]*broker.Subscription, 0, len(c.subs))
	for _, sub := range c.subs {
		subs = append(subs, sub)
	}
	c.mu.Unlock()

	// Interrupt the input loop while keeping the socket available for outbound
	// events until the subscription queues have drained.
	_ = c.net.SetReadDeadline(time.Now())
	for _, sub := range subs {
		if err := sub.Drain(ctx); err != nil {
			c.close()
			return
		}
	}
	c.deliveryWG.Wait()
	payload, err := encodeShutdown(shutdownMessage{Redirect: redirect})
	if err == nil {
		_ = c.send(protocol.Frame{Type: protocol.TypeShutdown, Payload: payload})
	}
	c.closeTransport()
}

func (c *connection) publish(payload []byte) error {
	message, err := decodePublish(payload)
	if err != nil || message.Topic == "" {
		return errors.New("invalid PUBLISH payload")
	}
	return c.server.broker.Publish(context.Background(), broker.Event{Topic: message.Topic, Key: message.Key, Payload: message.Payload})
}

func (c *connection) unsubscribe(payload []byte) {
	message, err := decodeTopic(payload)
	if err != nil {
		return
	}
	c.mu.Lock()
	sub := c.subs[message.Topic]
	delete(c.subs, message.Topic)
	c.mu.Unlock()
	if sub != nil {
		_ = sub.Close()
	}
}

func (c *connection) send(frame protocol.Frame) error {
	c.write.Lock()
	defer c.write.Unlock()
	return protocol.WriteFrame(c.net, frame)
}

func (c *connection) sendError(err error) error {
	payload, marshalErr := encodeError(errorMessage{Error: err.Error()})
	if marshalErr != nil {
		return marshalErr
	}
	return c.send(protocol.Frame{Type: protocol.TypeError, Payload: payload})
}

func (c *connection) close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	subs := make([]*broker.Subscription, 0, len(c.subs))
	for _, sub := range c.subs {
		subs = append(subs, sub)
	}
	c.subs = make(map[string]*broker.Subscription)
	c.mu.Unlock()
	for _, sub := range subs {
		_ = sub.Close()
	}
	_ = c.net.Close()
}

func (c *connection) closeTransport() {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	_ = c.net.Close()
}

var _ io.Closer = (*Server)(nil)
