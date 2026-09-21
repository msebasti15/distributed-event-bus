package protocol

import (
	"bytes"
	"errors"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	original := Frame{Type: TypePublish, Payload: []byte(`{"topic":"orders","payload":"created"}`)}
	var wire bytes.Buffer
	if err := WriteFrame(&wire, original); err != nil {
		t.Fatal(err)
	}
	got, err := ReadFrame(&wire)
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != original.Type || !bytes.Equal(got.Payload, original.Payload) {
		t.Fatalf("got %#v, want %#v", got, original)
	}
}

func TestRejectsOversizedFrame(t *testing.T) {
	frame := Frame{Type: TypePublish, Payload: make([]byte, MaxPayloadSize+1)}
	if err := WriteFrame(&bytes.Buffer{}, frame); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("got %v, want ErrFrameTooLarge", err)
	}
}

func TestRejectsUnknownZeroType(t *testing.T) {
	var wire bytes.Buffer
	wire.Write([]byte{0, 0, 0, 0, 0})
	if _, err := ReadFrame(&wire); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("got %v, want ErrInvalidFrame", err)
	}
}
