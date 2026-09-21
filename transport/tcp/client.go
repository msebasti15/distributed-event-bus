package tcp

import (
	"context"
	"errors"
	"net"
	"sync"

	"github.com/yourusername/distributed-event-bus/broker"
	"github.com/yourusername/distributed-event-bus/protocol"
)

var ErrClientClosed = errors.New("tcp client is closed")

// Client is a TCP client for publishing and consuming broker events.
type Client struct {
	conn   net.Conn
	write  sync.Mutex
	events chan broker.Event
	shutdowns chan string
	done   chan struct{}
	once   sync.Once
}

// Dial connects to address and sends the initial CONNECT frame.
func Dial(ctx context.Context, address, clientID string) (*Client, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}

	client := &Client{conn: conn, events: make(chan broker.Event, 32), shutdowns: make(chan string, 1), done: make(chan struct{})}
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

// Publish sends an event to the server.
func (c *Client) Publish(event broker.Event) error {
	payload, err := encodePublish(publishMessage{Topic: event.Topic, Key: event.Key, Payload: event.Payload})
	if err != nil {
		return err
	}
	return c.send(protocol.Frame{Type: protocol.TypePublish, Payload: payload})
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
			select {
			case c.events <- event:
			case <-c.done:
				return
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
