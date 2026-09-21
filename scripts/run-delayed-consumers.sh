#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ADDRESS="${EVENT_BUS_ADDRESS:-127.0.0.1:19000}"
TOPIC="${EVENT_BUS_TOPIC:-backpressure-demo}"
GO_BIN="${GO_BIN:-go}"
DELAY_SECONDS="${CONSUMER_DELAY_SECONDS:-5}"
MESSAGE_COUNT="${MESSAGE_COUNT:-30}"
QUEUE_SIZE="${QUEUE_SIZE:-4}"
CACHE_SIZE="${CACHE_SIZE:-1024}"

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

open_terminal "Event Bus" "'$GO_BIN' run ./cmd/event-bus -listen '$ADDRESS' -queue-size '$QUEUE_SIZE' -cache-size '$CACHE_SIZE'"
sleep 1

open_terminal "Delayed Producer" "'$GO_BIN' run ./cmd/producer -address '$ADDRESS' -topic '$TOPIC' -count '$MESSAGE_COUNT' -interval 200ms"
echo "Producer started. Consumers will start in ${DELAY_SECONDS}s..."
sleep "$DELAY_SECONDS"

open_terminal "Consumer A" "'$GO_BIN' run ./cmd/consumer -address '$ADDRESS' -topic '$TOPIC' -client-id consumer-a"
open_terminal "Consumer B" "'$GO_BIN' run ./cmd/consumer -address '$ADDRESS' -topic '$TOPIC' -client-id consumer-b"
open_terminal "Consumer C" "'$GO_BIN' run ./cmd/consumer -address '$ADDRESS' -topic '$TOPIC' -client-id consumer-c"

echo "Started three consumers after ${DELAY_SECONDS}s."
