#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GO_BIN="${GO_BIN:-go}"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RESULT_FILE="${1:-$ROOT_DIR/benchmarks/results-$TIMESTAMP.txt}"

cd "$ROOT_DIR"

if ! "$GO_BIN" version >/dev/null 2>&1; then
	echo "Go is not usable through '$GO_BIN'." >&2
	echo "Install a working Go toolchain or set GO_BIN=/path/to/go." >&2
	exit 1
fi

mkdir -p "$(dirname "$RESULT_FILE")"

{
	echo "Distributed Event Bus benchmark result"
	echo "timestamp_utc=$TIMESTAMP"
	echo "commit=$(git rev-parse HEAD)"
	echo "go_version=$($GO_BIN version)"
	echo "virtualization=$(systemd-detect-virt 2>/dev/null || echo unknown)"
	echo "os=$(grep '^PRETTY_NAME=' /etc/os-release 2>/dev/null | cut -d= -f2- | tr -d '\"' || echo unknown)"
	echo "kernel=$(uname -srvmo)"
	echo "cpu_model=$(lscpu 2>/dev/null | awk -F: '/Model name/ {gsub(/^ +/, \"\", $2); print $2; exit}' || echo unknown)"
	echo "cpu_count=$(nproc 2>/dev/null || echo unknown)"
	echo "memory=$(free -h 2>/dev/null | awk '/^Mem:/ {print $2; exit}' || echo unknown)"
	echo "hypervisor=$(lscpu 2>/dev/null | awk -F: '/Hypervisor vendor/ {gsub(/^ +/, \"\", $2); print $2; exit}' || echo unknown)"
	echo "machine=$(cat /sys/class/dmi/id/product_name 2>/dev/null || echo unknown)"
	echo "command=$GO_BIN test ./... -run '^$' -bench . -benchmem"
	echo
"$GO_BIN" test ./... -run '^$' -bench . -benchmem
} | tee "$RESULT_FILE"

echo "Saved benchmark result to $RESULT_FILE"
