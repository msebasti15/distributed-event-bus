#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GO_BIN="${GO_BIN:-go}"
PRIMARY_ADDRESS="${PRIMARY_ADDRESS:-127.0.0.1:19200}"
STANDBY_ADDRESS="${STANDBY_ADDRESS:-127.0.0.1:19201}"
PRIMARY_HTTP="${PRIMARY_HTTP:-127.0.0.1:19300}"
STANDBY_HTTP="${STANDBY_HTTP:-127.0.0.1:19301}"
TOPIC="${EVENT_BUS_TOPIC:-abrupt-failover-demo}"
FAIL_AFTER_SECONDS="${FAIL_AFTER_SECONDS:-8}"
DEMO_DIR="${DEMO_DIR:-$(mktemp -d /tmp/distributed-event-bus-demo.XXXXXX)}"
PRIMARY_PID_FILE="$DEMO_DIR/primary.pid"

if ! "$GO_BIN" version >/dev/null 2>&1; then
	echo "Go is not usable through '$GO_BIN'." >&2
	echo "Install a working Go toolchain or set GO_BIN=/path/to/go." >&2
	exit 1
fi

echo "Building demo binaries in $DEMO_DIR..."
"$GO_BIN" build -o "$DEMO_DIR/event-bus" ./cmd/event-bus
"$GO_BIN" build -o "$DEMO_DIR/producer" ./cmd/producer
"$GO_BIN" build -o "$DEMO_DIR/consumer" ./cmd/consumer

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

open_terminal "Abrupt Primary" "echo \"\$BASHPID\" > '$PRIMARY_PID_FILE'; exec '$DEMO_DIR/event-bus' -listen '$PRIMARY_ADDRESS' -http-listen '$PRIMARY_HTTP' -cache-size 128"
open_terminal "Standby Broker" "exec '$DEMO_DIR/event-bus' -listen '$STANDBY_ADDRESS' -http-listen '$STANDBY_HTTP' -cache-size 128"

if ! command -v curl >/dev/null 2>&1; then
	echo "curl is required to wait for the brokers." >&2
	exit 1
fi

wait_for_http() {
	local address="$1"
	local attempts=0
	until curl -fsS "http://$address/state" >/dev/null 2>&1; do
		attempts=$((attempts + 1))
		if [ "$attempts" -ge 60 ]; then
			echo "Timed out waiting for $address." >&2
			exit 1
		fi
		sleep 1
	done
}

wait_for_http "$PRIMARY_HTTP"
wait_for_http "$STANDBY_HTTP"

open_terminal "Producer" "exec '$DEMO_DIR/producer' -address '$PRIMARY_ADDRESS' -standby '$STANDBY_ADDRESS' -topic '$TOPIC' -count 0 -interval 500ms"
open_terminal "Consumer" "exec '$DEMO_DIR/consumer' -address '$PRIMARY_ADDRESS' -standby '$STANDBY_ADDRESS' -topic '$TOPIC' -client-id abrupt-failover-consumer"
open_terminal "Abrupt Failure Controller" "sleep '$FAIL_AFTER_SECONDS'; primary_pid=\$(cat '$PRIMARY_PID_FILE'); echo \"Killing primary broker PID \$primary_pid with SIGKILL (no shutdown event)\"; kill -KILL \"\$primary_pid\"; echo 'Primary is down. ConnectionManager should reconnect producer and consumer to the standby.'; exec bash"

echo "Abrupt failover demo started. Primary will be killed after ${FAIL_AFTER_SECONDS}s."
