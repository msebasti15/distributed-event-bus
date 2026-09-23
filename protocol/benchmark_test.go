package protocol

import (
	"bytes"
	"testing"
)

func BenchmarkFrameRoundTrip(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 256)
	frame := Frame{Type: TypePublish, Payload: payload}
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var wire bytes.Buffer
		if err := WriteFrame(&wire, frame); err != nil {
			b.Fatal(err)
		}
		got, err := ReadFrame(&wire)
		if err != nil {
			b.Fatal(err)
		}
		if got.Type != frame.Type || !bytes.Equal(got.Payload, frame.Payload) {
			b.Fatal("frame changed during round-trip")
		}
	}
}

func BenchmarkWriteFrame(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 256)
	frame := Frame{Type: TypePublish, Payload: payload}
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	var wire bytes.Buffer
	for i := 0; i < b.N; i++ {
		wire.Reset()
		if err := WriteFrame(&wire, frame); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadFrame(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 256)
	frame := Frame{Type: TypePublish, Payload: payload}
	var encoded bytes.Buffer
	if err := WriteFrame(&encoded, frame); err != nil {
		b.Fatal(err)
	}
	wire := encoded.Bytes()
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		got, err := ReadFrame(bytes.NewReader(wire))
		if err != nil {
			b.Fatal(err)
		}
		if got.Type != frame.Type {
			b.Fatal("frame type changed")
		}
	}
}
