#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ADDRESS="${EVENT_BUS_ADDRESS:-127.0.0.1:19000}"
TOPIC="${EVENT_BUS_TOPIC:-events}"
GO_BIN="${GO_BIN:-go}"

echo "=== DEMO: basic pub/sub ==="
echo "Purpose: publish one topic to three independent consumers."
echo "Topology: Event Bus $ADDRESS -> Producer + Consumer A/B/C; topic=$TOPIC"
echo "Expected: every consumer receives each published event."
echo "Stop the terminals with Ctrl+C when finished."
echo

if ! "$GO_BIN" version >/dev/null 2>&1; then
	echo "Go is not usable through '$GO_BIN'." >&2
	echo "Install a working Go toolchain or run with GO_BIN=/path/to/go ./scripts/run-local.sh" >&2
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

open_terminal "Event Bus" "'$GO_BIN' run ./cmd/event-bus -listen '$ADDRESS'"
sleep 1
open_terminal "Producer" "'$GO_BIN' run ./cmd/producer -address '$ADDRESS' -topic '$TOPIC' -count 0 -interval 1s"
open_terminal "Consumer A" "'$GO_BIN' run ./cmd/consumer -address '$ADDRESS' -topic '$TOPIC' -client-id consumer-a"
open_terminal "Consumer B" "'$GO_BIN' run ./cmd/consumer -address '$ADDRESS' -topic '$TOPIC' -client-id consumer-b"
open_terminal "Consumer C" "'$GO_BIN' run ./cmd/consumer -address '$ADDRESS' -topic '$TOPIC' -client-id consumer-c"

echo "Started one event bus, one producer and three consumers on topic '$TOPIC'."
