package tcp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/yourusername/distributed-event-bus/broker"
	"github.com/yourusername/distributed-event-bus/protocol"
)

var ErrClientClosed = errors.New("tcp client is closed")

// PublishStatus describes the broker's acknowledgement for a message.
type PublishStatus byte

const (
	PublishAccepted PublishStatus = 1
	PublishDuplicate PublishStatus = 2
)

// Client is a TCP client for publishing and consuming broker events.
type Client struct {
	conn      net.Conn
	write     sync.Mutex
	events    chan broker.Event
	shutdowns chan string
	done      chan struct{}
	once      sync.Once
	pendingMu sync.Mutex
	pending   map[string]chan ackMessage
	ackedMu   sync.Mutex
	acked     map[string]struct{}
	ackedIDs  []string
}

const maxRememberedDeliveries = 4096

// Dial connects to address and sends the initial CONNECT frame.
func Dial(ctx context.Context, address, clientID string) (*Client, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}

	client := &Client{conn: conn, events: make(chan broker.Event, 32), shutdowns: make(chan string, 1), done: make(chan struct{}), pending: make(map[string]chan ackMessage), acked: make(map[string]struct{})}
	payload, err := encodeConnect(connectMessage{ClientID: clientID})
	if err != nil || client.send(protocol.Frame{Type: protocol.TypeConnect, Payload: payload}) != nil {
		_ = conn.Close()
		if err != nil {
			return nil, err
		}
		return nil, errors.New("failed to send CONNECT frame")
	}
	go client.readLoop()
	return client, nil
}

// Events returns the stream of events delivered to this client.
func (c *Client) Events() <-chan broker.Event { return c.events }

// Done is closed when the connection stops.
func (c *Client) Done() <-chan struct{} { return c.done }

// Shutdowns returns lifecycle notifications sent by the broker. A non-empty
// value indicates an optional replacement broker address.
func (c *Client) Shutdowns() <-chan string { return c.shutdowns }

// Subscribe subscribes this client to topic.
func (c *Client) Subscribe(topic string) error {
	payload, err := encodeTopic(topicMessage{Topic: topic})
	if err != nil {
		return err
	}
	return c.send(protocol.Frame{Type: protocol.TypeSubscribe, Payload: payload})
}

// Unsubscribe removes this client's subscription to topic.
func (c *Client) Unsubscribe(topic string) error {
	payload, err := encodeTopic(topicMessage{Topic: topic})
	if err != nil {
		return err
	}
	return c.send(protocol.Frame{Type: protocol.TypeUnsubscribe, Payload: payload})
}

// Acknowledge confirms that the consumer finished processing an event.
func (c *Client) Acknowledge(eventID string) error {
	if eventID == "" {
		return errors.New("event ID is required")
	}
	payload, err := encodeAck(ackMessage{ID: eventID, Status: ackDelivered})
	if err != nil {
		return err
	}
	if err := c.send(protocol.Frame{Type: protocol.TypeAck, Payload: payload}); err != nil {
		return err
	}
	c.rememberDelivery(eventID)
	return nil
}

// Publish sends an event to the server.
func (c *Client) Publish(event broker.Event) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.PublishContext(ctx, event)
}

// PublishContext sends an event and waits for its broker acknowledgement.
func (c *Client) PublishContext(ctx context.Context, event broker.Event) error {
	_, err := c.PublishWithResult(ctx, event)
	return err
}

// PublishWithResult sends an event and returns the broker acknowledgement.
func (c *Client) PublishWithResult(ctx context.Context, event broker.Event) (PublishStatus, error) {
	if event.ID == "" {
		id, err := newMessageID()
		if err != nil {
			return 0, err
		}
		event.ID = id
	}
	if event.IdempotencyKey == "" {
		event.IdempotencyKey = event.ID
	}
	acknowledged := make(chan ackMessage, 1)
	c.pendingMu.Lock()
	c.pending[event.ID] = acknowledged
	c.pendingMu.Unlock()
	defer func() {
		c.pendingMu.Lock()
		delete(c.pending, event.ID)
		c.pendingMu.Unlock()
	}()
	payload, err := encodePublish(publishMessage{ID: event.ID, IdempotencyKey: event.IdempotencyKey, Topic: event.Topic, Key: event.Key, Payload: event.Payload})
	if err != nil {
		return 0, err
	}
	if err := c.send(protocol.Frame{Type: protocol.TypePublish, Payload: payload}); err != nil {
		return 0, err
	}
	select {
	case ack := <-acknowledged:
		if ack.Status == ackRejected {
			return 0, errors.New(ack.Error)
		}
		if ack.Status == ackDuplicate {
			return PublishDuplicate, nil
		}
		return PublishAccepted, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-c.done:
		return 0, ErrClientClosed
	}
}

// Close closes the connection and the event stream.
func (c *Client) Close() error {
	c.once.Do(func() {
		close(c.done)
		_ = c.conn.Close()
	})
	return nil
}

func (c *Client) send(frame protocol.Frame) error {
	select {
	case <-c.done:
		return ErrClientClosed
	default:
	}
	c.write.Lock()
	defer c.write.Unlock()
	return protocol.WriteFrame(c.conn, frame)
}

func (c *Client) readLoop() {
	defer close(c.events)
	defer close(c.shutdowns)
	defer c.Close()
	for {
		frame, err := protocol.ReadFrame(c.conn)
		if err != nil {
			return
		}
		switch frame.Type {
		case protocol.TypeEvent:
			event, err := decodeEvent(frame.Payload)
			if err != nil {
				return
			}
			if c.wasDelivered(event.ID) {
				// The previous delivery was acknowledged, but the ACK may have
				// crossed with a retry. Confirm it again without exposing a
				// duplicate to the application.
				_ = c.Acknowledge(event.ID)
				continue
			}
			select {
			case c.events <- event:
			case <-c.done:
				return
			}
		case protocol.TypeAck:
			ack, err := decodeAck(frame.Payload)
			if err != nil {
				return
			}
			c.pendingMu.Lock()
			acknowledged := c.pending[ack.ID]
			c.pendingMu.Unlock()
			if acknowledged != nil {
				acknowledged <- ack
			}
		case protocol.TypeError:
			if _, err := decodeError(frame.Payload); err != nil {
				return
			}
			return
		case protocol.TypeShutdown:
			message, err := decodeShutdown(frame.Payload)
			if err != nil {
				return
			}
			select {
			case c.shutdowns <- message.Redirect:
			case <-c.done:
			}
			return
		}
	}
}

func (c *Client) rememberDelivery(eventID string) {
	c.ackedMu.Lock()
	defer c.ackedMu.Unlock()
	if _, exists := c.acked[eventID]; exists {
		return
	}
	c.acked[eventID] = struct{}{}
	c.ackedIDs = append(c.ackedIDs, eventID)
	if len(c.ackedIDs) > maxRememberedDeliveries {
		oldest := c.ackedIDs[0]
		c.ackedIDs = c.ackedIDs[1:]
		delete(c.acked, oldest)
	}
}

func (c *Client) wasDelivered(eventID string) bool {
	c.ackedMu.Lock()
	defer c.ackedMu.Unlock()
	_, exists := c.acked[eventID]
	return exists
}

func newMessageID() (string, error) {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(data[:]), nil
}
