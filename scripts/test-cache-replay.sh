#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ADDRESS="${EVENT_BUS_ADDRESS:-127.0.0.1:19000}"
TOPIC="${EVENT_BUS_TOPIC:-cache-demo}"
GO_BIN="${GO_BIN:-go}"
CACHE_SIZE="${CACHE_SIZE:-64}"
MESSAGE_COUNT="${MESSAGE_COUNT:-10}"
CONSUMER_DELAY_SECONDS="${CONSUMER_DELAY_SECONDS:-3}"

echo "=== DEMO: cache replay ==="
echo "Purpose: retain events published while a topic has no consumers."
echo "Topology: Event Bus $ADDRESS -> Producer -> cache -> Consumer; topic=$TOPIC"
echo "Expected: the consumer receives cached events after ${CONSUMER_DELAY_SECONDS}s."
echo "Limit: cache is in-memory and is lost if the broker crashes."
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

open_terminal "Cache Test - Event Bus" "'$GO_BIN' run ./cmd/event-bus -listen '$ADDRESS' -queue-size '$CACHE_SIZE' -cache-size '$CACHE_SIZE'"
sleep 1

open_terminal "Cache Test - Producer" "'$GO_BIN' run ./cmd/producer -address '$ADDRESS' -topic '$TOPIC' -count '$MESSAGE_COUNT' -interval 100ms"
echo "Published messages will be cached because no consumer is connected."
sleep "$CONSUMER_DELAY_SECONDS"

open_terminal "Cache Test - Consumer" "'$GO_BIN' run ./cmd/consumer -address '$ADDRESS' -topic '$TOPIC' -client-id cache-test-consumer"
echo "Consumer started after ${CONSUMER_DELAY_SECONDS}s; it should receive ${MESSAGE_COUNT} cached messages."
