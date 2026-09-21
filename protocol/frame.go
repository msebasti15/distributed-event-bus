// Package protocol defines the wire format used by the TCP transport.
package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	// HeaderSize is the number of bytes before a frame payload.
	HeaderSize = 5
	// MaxPayloadSize prevents a peer from requesting an unbounded allocation.
	MaxPayloadSize = 16 << 20
)

var (
	ErrFrameTooLarge = errors.New("frame payload exceeds maximum size")
	ErrInvalidFrame  = errors.New("invalid frame")
)

// Type identifies a message on the wire.
type Type byte

const (
	TypeConnect     Type = 1
	TypeSubscribe   Type = 2
	TypePublish     Type = 3
	TypeEvent       Type = 4
	TypeAck         Type = 5
	TypeUnsubscribe Type = 6
	TypePing        Type = 7
	TypePong        Type = 8
	TypeError       Type = 9
	TypeShutdown    Type = 10
)

// Frame is a single length-delimited wire message.
type Frame struct {
	Type    Type
	Payload []byte
}

// ReadFrame reads exactly one frame from r.
func ReadFrame(r io.Reader) (Frame, error) {
	var header [HeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return Frame{}, err
	}

	payloadSize := binary.BigEndian.Uint32(header[:4])
	if payloadSize > MaxPayloadSize {
		return Frame{}, fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, payloadSize)
	}
	if header[4] == 0 {
		return Frame{}, ErrInvalidFrame
	}

	payload := make([]byte, payloadSize)
	if _, err := io.ReadFull(r, payload); err != nil {
		return Frame{}, err
	}
	return Frame{Type: Type(header[4]), Payload: payload}, nil
}

// WriteFrame writes one complete frame to w.
func WriteFrame(w io.Writer, frame Frame) error {
	if frame.Type == 0 {
		return ErrInvalidFrame
	}
	if len(frame.Payload) > MaxPayloadSize {
		return fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, len(frame.Payload))
	}

	var header [HeaderSize]byte
	binary.BigEndian.PutUint32(header[:4], uint32(len(frame.Payload)))
	header[4] = byte(frame.Type)
	if err := writeFull(w, header[:]); err != nil {
		return err
	}
	return writeFull(w, frame.Payload)
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
