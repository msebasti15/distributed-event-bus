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
	"github.com/yourusername/distributed-event-bus/logging"
	"github.com/yourusername/distributed-event-bus/protocol"
)

var ErrNotConnected = errors.New("client must send CONNECT first")

const (
	deliveryAckTimeout  = 2 * time.Second
	deliveryMaxAttempts = 3
)

// Server exposes a broker through the TCP wire protocol.
type Server struct {
	broker   *broker.Broker
	listener net.Listener
	mu       sync.Mutex
	closed   bool
	draining bool
	conns    map[*connection]struct{}
	wg       sync.WaitGroup
	logger   *logging.Logger
}

// NewServer creates a TCP server backed by b.
func NewServer(b *broker.Broker) *Server {
	return &Server{broker: b, conns: make(map[*connection]struct{}), logger: logging.New(logging.LevelError, io.Discard)}
}

// SetLogger configures operational logging for the TCP server.
func (s *Server) SetLogger(logger *logging.Logger) { s.logger = logger }

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
		s.logger.Debugf("accepted TCP connection remote=%s", conn.RemoteAddr())
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
	s.logger.Infof("graceful shutdown started redirect=%q", redirect)
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
		conn.mu.Lock()
		if !conn.closed {
			conn.draining = true
		}
		conn.mu.Unlock()
		drainWG.Add(1)
		go func(conn *connection) {
			defer drainWG.Done()
			conn.gracefulClose(ctx, redirect)
		}(conn)
	}
	drainWG.Wait()
	if ctx.Err() != nil {
		s.logger.Warnf("graceful shutdown timed out: %v", ctx.Err())
		return ctx.Err()
	}
	s.logger.Infof("graceful shutdown completed")
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
	server     *Server
	net        net.Conn
	write      sync.Mutex
	mu         sync.Mutex
	subs       map[string]*broker.Subscription
	closed     bool
	draining   bool
	deliveryMu sync.Mutex
	deliveryWG sync.WaitGroup
	ackMu      sync.Mutex
	pendingAck map[string]chan byte
}

func newConnection(server *Server, conn net.Conn) *connection {
	return &connection{server: server, net: conn, subs: make(map[string]*broker.Subscription), pendingAck: make(map[string]chan byte)}
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
	c.server.logger.Infof("client connected remote=%s", c.net.RemoteAddr())

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
			if c.isDraining() {
				_ = c.sendError(broker.ErrDraining)
				continue
			}
			if err := c.publish(frame.Payload); err != nil {
				_ = c.sendError(err)
			}
		case protocol.TypeAck:
			if err := c.acknowledge(frame.Payload); err != nil {
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
	c.deliveryMu.Lock()
	defer c.deliveryMu.Unlock()
	if c.isDraining() {
		return broker.ErrDraining
	}
	sub, err := c.server.broker.Subscribe(message.Topic)
	if err != nil {
		return err
	}
	c.server.logger.Infof("subscription created topic=%s remote=%s", message.Topic, c.net.RemoteAddr())
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
			if !c.deliver(event) {
				return
			}
		}
	}()
	return nil
}

func (c *connection) abort() {
	c.server.mu.Lock()
	c.mu.Lock()
	draining := c.draining || c.server.draining
	c.mu.Unlock()
	c.server.mu.Unlock()
	if !draining {
		c.close()
	}
}

func (c *connection) gracefulClose(ctx context.Context, redirect string) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.draining = true
	subs := make([]*broker.Subscription, 0, len(c.subs))
	for _, sub := range c.subs {
		subs = append(subs, sub)
	}
	c.mu.Unlock()

	for _, sub := range subs {
		if err := sub.Drain(ctx); err != nil {
			c.close()
			return
		}
	}
	c.deliveryMu.Lock()
	c.deliveryWG.Wait()
	c.deliveryMu.Unlock()
	payload, err := encodeShutdown(shutdownMessage{Redirect: redirect})
	if err == nil {
		_ = c.send(protocol.Frame{Type: protocol.TypeShutdown, Payload: payload})
	}
	c.closeTransport()
}

func (c *connection) isDraining() bool {
	c.server.mu.Lock()
	serverDraining := c.server.draining
	c.server.mu.Unlock()
	c.mu.Lock()
	connectionDraining := c.draining
	c.mu.Unlock()
	return connectionDraining || serverDraining
}

func (c *connection) deliver(event broker.Event) bool {
	payload, err := encodeEvent(event)
	if err != nil {
		return false
	}
	acknowledged := make(chan byte, 1)
	c.ackMu.Lock()
	c.pendingAck[event.ID] = acknowledged
	c.ackMu.Unlock()
	defer func() {
		c.ackMu.Lock()
		delete(c.pendingAck, event.ID)
		c.ackMu.Unlock()
	}()

	for attempt := 1; attempt <= deliveryMaxAttempts; attempt++ {
		c.server.logger.Debugf("delivering event id=%s topic=%s attempt=%d/%d remote=%s", event.ID, event.Topic, attempt, deliveryMaxAttempts, c.net.RemoteAddr())
		if err := c.send(protocol.Frame{Type: protocol.TypeEvent, Payload: payload}); err != nil {
			c.server.logger.Warnf("delivery failed id=%s: %v", event.ID, err)
			return false
		}
		timer := time.NewTimer(deliveryAckTimeout)
		select {
		case status := <-acknowledged:
			timer.Stop()
			if status == ackDelivered {
				c.server.logger.Debugf("delivery ACK received id=%s remote=%s", event.ID, c.net.RemoteAddr())
				return true
			}
		case <-timer.C:
			c.server.logger.Debugf("delivery ACK timeout id=%s attempt=%d", event.ID, attempt)
		}
	}
	c.server.logger.Warnf("delivery abandoned after retries id=%s", event.ID)
	return false
}

func (c *connection) acknowledge(payload []byte) error {
	ack, err := decodeAck(payload)
	if err != nil {
		return err
	}
	c.ackMu.Lock()
	acknowledged := c.pendingAck[ack.ID]
	c.ackMu.Unlock()
	if acknowledged == nil {
		c.server.logger.Debugf("ACK ignored id=%s remote=%s", ack.ID, c.net.RemoteAddr())
		return nil
	}
	c.server.logger.Debugf("ACK received id=%s status=%d remote=%s", ack.ID, ack.Status, c.net.RemoteAddr())
	select {
	case acknowledged <- ack.Status:
	default:
	}
	return nil
}

func (c *connection) publish(payload []byte) error {
	message, err := decodePublish(payload)
	if err != nil || message.Topic == "" {
		return errors.New("invalid PUBLISH payload")
	}
	result, publishErr := c.server.broker.PublishWithResult(context.Background(), broker.Event{
		ID: message.ID, IdempotencyKey: message.IdempotencyKey,
		Topic: message.Topic, Key: message.Key, Payload: message.Payload,
	})
	ack := ackMessage{ID: message.ID, Status: ackPublished}
	if result == broker.Duplicate {
		ack.Status = ackDuplicate
	}
	if publishErr != nil {
		ack.Status = ackRejected
		ack.Error = publishErr.Error()
	}
	c.server.logger.Infof("publish handled id=%s topic=%s status=%d", message.ID, message.Topic, ack.Status)
	ackPayload, err := encodeAck(ack)
	if err != nil {
		return err
	}
	return c.send(protocol.Frame{Type: protocol.TypeAck, Payload: ackPayload})
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
		c.server.logger.Infof("subscription removed topic=%s remote=%s", message.Topic, c.net.RemoteAddr())
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
	c.server.logger.Debugf("connection closed remote=%s", c.net.RemoteAddr())
}

func (c *connection) closeTransport() {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	_ = c.net.Close()
}

var _ io.Closer = (*Server)(nil)
