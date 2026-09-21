#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GO_BIN="${GO_BIN:-go}"
PRIMARY_ADDRESS="${PRIMARY_ADDRESS:-127.0.0.1:19000}"
STANDBY_ADDRESS="${STANDBY_ADDRESS:-127.0.0.1:19001}"
PRIMARY_HTTP="${PRIMARY_HTTP:-127.0.0.1:19100}"
STANDBY_HTTP="${STANDBY_HTTP:-127.0.0.1:19101}"
TOPIC="${EVENT_BUS_TOPIC:-failover-demo}"
FAILOVER_AFTER_SECONDS="${FAILOVER_AFTER_SECONDS:-8}"

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

open_terminal "Primary Broker" "'$GO_BIN' run ./cmd/event-bus -listen '$PRIMARY_ADDRESS' -http-listen '$PRIMARY_HTTP' -cache-size 128"
open_terminal "Standby Broker" "'$GO_BIN' run ./cmd/event-bus -listen '$STANDBY_ADDRESS' -http-listen '$STANDBY_HTTP' -cache-size 128"

if ! command -v curl >/dev/null 2>&1; then
	echo "curl is required for the failover controller." >&2
	exit 1
fi

wait_for_http() {
	local address="$1"
	local attempts=0
	until curl -fsS "http://$address/state" >/dev/null 2>&1; do
		attempts=$((attempts + 1))
		if [ "$attempts" -ge 60 ]; then
			echo "Timed out waiting for HTTP control server at $address." >&2
			exit 1
		fi
		sleep 1
	done
}

wait_for_http "$PRIMARY_HTTP"
wait_for_http "$STANDBY_HTTP"
open_terminal "Producer" "'$GO_BIN' run ./cmd/producer -address '$PRIMARY_ADDRESS' -standby '$STANDBY_ADDRESS' -topic '$TOPIC' -count 0 -interval 500ms"
open_terminal "Consumer" "'$GO_BIN' run ./cmd/consumer -address '$PRIMARY_ADDRESS' -standby '$STANDBY_ADDRESS' -topic '$TOPIC' -client-id failover-consumer"

open_terminal "Failover Controller" "sleep '$FAILOVER_AFTER_SECONDS'; echo 'Requesting graceful migration to $STANDBY_ADDRESS'; curl -sS -X POST 'http://$PRIMARY_HTTP/shutdown?redirect=$STANDBY_ADDRESS'; echo; echo 'ConnectionManager should now use the standby broker.'; echo 'The consumer remains running and re-subscribes automatically.'; exec bash"

echo "Started primary, standby, producer, consumer and failover controller."
