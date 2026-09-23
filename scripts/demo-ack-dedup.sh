#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GO_BIN="${GO_BIN:-go}"
ADDRESS="${EVENT_BUS_ADDRESS:-127.0.0.1:19400}"

echo "=== DEMO: publish ACK and idempotency ==="
echo "Purpose: distinguish broker acceptance from duplicate publish detection."
echo "Topology: ACK client (producer role) -> Event Bus $ADDRESS; no consumer."
echo "Expected: first publish is accepted; the same idempotency key is duplicate."
echo "This demo does not demonstrate consumer delivery ACKs."
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

open_terminal "ACK Demo - Event Bus" "'$GO_BIN' run ./cmd/event-bus -listen '$ADDRESS' -cache-size 32 -deduplication-size 32"
sleep 2
open_terminal "ACK Demo - Client" "'$GO_BIN' run ./cmd/ack-demo -address '$ADDRESS'"

echo "ACK/deduplication demonstration started on $ADDRESS."
