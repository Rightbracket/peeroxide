#!/usr/bin/env bash
# M6 Docker NAT simulation test.
#
# Builds containers and verifies that two real `peeroxide` processes,
# each behind its own independent simulated NAT (nat-a / nat-b, joined
# only via a shared public bootstrap node), can holepunch and complete a
# `cp send` / `cp recv` file transfer end-to-end. This is the only rig in
# the repo that exercises the holepunch/relay code path against two
# genuinely separate NATs; the `--ignored` live tests in
# peeroxide-cli/tests/live_commands.rs run both peers on one host/NAT and
# cannot prove this.
#
# Prerequisites: Docker with compose plugin, Linux containers
# Usage: bash run-nat-test.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== M6 Docker NAT Simulation Test ==="

command -v docker >/dev/null || { echo "ERROR: docker not found"; exit 1; }
docker compose version >/dev/null 2>&1 || { echo "ERROR: docker compose not found"; exit 1; }

COMPOSE_FILE="$SCRIPT_DIR/docker-compose.nat.yml"

cleanup() {
    echo "Cleaning up containers..."
    docker compose -f "$COMPOSE_FILE" down --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

echo "Building containers..."
docker compose -f "$COMPOSE_FILE" build

echo "Starting NAT simulation..."
# --exit-code-from propagates the receiver's real exit code (0 on a
# verified transfer, non-zero on holepunch/verification failure) instead
# of always returning 0 once `up` itself returns, as the previous version
# of this script did.
docker compose -f "$COMPOSE_FILE" up --abort-on-container-exit --exit-code-from rust-receiver

echo "=== M6 Docker NAT Test PASSED ==="
