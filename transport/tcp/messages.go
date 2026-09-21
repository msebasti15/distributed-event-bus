package tcp

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/yourusername/distributed-event-bus/broker"
	"github.com/yourusername/distributed-event-bus/protocol"
)

var errInvalidPayload = errors.New("invalid message payload")

type connectMessage struct{ ClientID string }
type topicMessage struct{ Topic string }
type publishMessage struct {
	Topic   string
	Key     string
	Payload []byte
}
type errorMessage struct{ Error string }
type shutdownMessage struct{ Redirect string }

func encodeConnect(message connectMessage) ([]byte, error) {
	var buffer bytes.Buffer
	if err := writeString(&buffer, message.ClientID); err != nil { return nil, err }
	return buffer.Bytes(), nil
}

func decodeConnect(payload []byte) (connectMessage, error) {
	reader := bytes.NewReader(payload)
	clientID, err := readString(reader)
	if err != nil || reader.Len() != 0 { return connectMessage{}, errInvalidPayload }
	return connectMessage{ClientID: clientID}, nil
}

func encodeTopic(message topicMessage) ([]byte, error) {
	var buffer bytes.Buffer
	if err := writeString(&buffer, message.Topic); err != nil { return nil, err }
	return buffer.Bytes(), nil
}

func decodeTopic(payload []byte) (topicMessage, error) {
	reader := bytes.NewReader(payload)
	topic, err := readString(reader)
	if err != nil || reader.Len() != 0 { return topicMessage{}, errInvalidPayload }
	return topicMessage{Topic: topic}, nil
}

func encodePublish(message publishMessage) ([]byte, error) {
	var buffer bytes.Buffer
	if err := writeString(&buffer, message.Topic); err != nil { return nil, err }
	if err := writeString(&buffer, message.Key); err != nil { return nil, err }
	if err := writeBytes(&buffer, message.Payload); err != nil { return nil, err }
	return buffer.Bytes(), nil
}

func decodePublish(payload []byte) (publishMessage, error) {
	reader := bytes.NewReader(payload)
	topic, err := readString(reader)
	if err != nil { return publishMessage{}, errInvalidPayload }
	key, err := readString(reader)
	if err != nil { return publishMessage{}, errInvalidPayload }
	data, err := readBytes(reader)
	if err != nil || reader.Len() != 0 { return publishMessage{}, errInvalidPayload }
	return publishMessage{Topic: topic, Key: key, Payload: data}, nil
}

func encodeEvent(event broker.Event) ([]byte, error) {
	return encodePublish(publishMessage{Topic: event.Topic, Key: event.Key, Payload: event.Payload})
}

func decodeEvent(payload []byte) (broker.Event, error) {
	message, err := decodePublish(payload)
	if err != nil { return broker.Event{}, err }
	return broker.Event{Topic: message.Topic, Key: message.Key, Payload: message.Payload}, nil
}

func encodeError(message errorMessage) ([]byte, error) {
	return encodeTopic(topicMessage{Topic: message.Error})
}

func decodeError(payload []byte) (errorMessage, error) {
	message, err := decodeTopic(payload)
	if err != nil { return errorMessage{}, err }
	return errorMessage{Error: message.Topic}, nil
}

func encodeShutdown(message shutdownMessage) ([]byte, error) {
	return encodeTopic(topicMessage{Topic: message.Redirect})
}

func decodeShutdown(payload []byte) (shutdownMessage, error) {
	message, err := decodeTopic(payload)
	if err != nil {
		return shutdownMessage{}, err
	}
	return shutdownMessage{Redirect: message.Topic}, nil
}

func writeString(w io.Writer, value string) error {
	if len(value) > int(^uint16(0)) { return errInvalidPayload }
	var length [2]byte
	binary.BigEndian.PutUint16(length[:], uint16(len(value)))
	if _, err := w.Write(length[:]); err != nil { return err }
	_, err := w.Write([]byte(value))
	return err
}

func readString(r io.Reader) (string, error) {
	var length [2]byte
	if _, err := io.ReadFull(r, length[:]); err != nil { return "", err }
	data := make([]byte, binary.BigEndian.Uint16(length[:]))
	if _, err := io.ReadFull(r, data); err != nil { return "", err }
	return string(data), nil
}

func writeBytes(w io.Writer, data []byte) error {
	if len(data) > protocol.MaxPayloadSize { return protocol.ErrFrameTooLarge }
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(data)))
	if _, err := w.Write(length[:]); err != nil { return err }
	_, err := w.Write(data)
	return err
}

func readBytes(r io.Reader) ([]byte, error) {
	var length [4]byte
	if _, err := io.ReadFull(r, length[:]); err != nil { return nil, err }
	size := binary.BigEndian.Uint32(length[:])
	if size > protocol.MaxPayloadSize { return nil, protocol.ErrFrameTooLarge }
	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil { return nil, err }
	return data, nil
}
