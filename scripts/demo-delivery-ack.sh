#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GO_BIN="${GO_BIN:-go}"
ADDRESS="${EVENT_BUS_ADDRESS:-127.0.0.1:19500}"
TOPIC="${EVENT_BUS_TOPIC:-delivery-demo}"

echo "=== DEMO: consumer delivery ACK and retry ==="
echo "Purpose: show that consumer ACKs go to the Event Bus, not the producer."
echo "Topology: Producer -> Event Bus $ADDRESS -> Consumer; topic=$TOPIC"
echo "Sequence: publish ACK, first delivery, timeout, redelivery, consumer ACK."
echo "Expected: the producer finishes after broker acceptance; the consumer ACKs separately."
echo

if ! "$GO_BIN" version >/dev/null 2>&1; then
	echo "Go is not usable through '$GO_BIN'." >&2
	echo "Install a working Go toolchain or set GO_BIN=/path/to/go." >&2
	exit 1
fi

if command -v gnome-terminal >/dev/null 2>&1; then
	open_terminal() {
		local title="$1"
		local command="$2"
		gnome-terminal --title="$title" -- bash -lc "cd '$ROOT_DIR'; $command; exec bash"
	}
elif command -v konsole >/dev/null 2>&1; then
	open_terminal() {
		local title="$1"
		local command="$2"
		konsole --new-tab --title "$title" -e bash -lc "cd '$ROOT_DIR'; $command; exec bash"
	}
elif command -v xterm >/dev/null 2>&1; then
	open_terminal() {
		local title="$1"
		local command="$2"
		xterm -T "$title" -e bash -lc "cd '$ROOT_DIR'; $command; exec bash" &
	}
else
	echo "No supported terminal emulator found (gnome-terminal, konsole or xterm)." >&2
	exit 1
fi

open_terminal "Delivery ACK Demo - Event Bus" \
	"'$GO_BIN' run ./cmd/event-bus -listen '$ADDRESS' -queue-size 8"
sleep 2
open_terminal "Delivery ACK Demo - Consumer" \
	"'$GO_BIN' run ./cmd/delivery-ack-demo -address '$ADDRESS' -topic '$TOPIC' -ack-after 2500ms"
sleep 2
open_terminal "Delivery ACK Demo - Producer" \
	"'$GO_BIN' run ./cmd/producer -address '$ADDRESS' -topic '$TOPIC' -payload 'delivery ACK demo' -count 1"

echo "Started delivery ACK demonstration on $ADDRESS."
echo "The consumer waits longer than the 2s ACK timeout, so the broker retries the event before the ACK arrives."
