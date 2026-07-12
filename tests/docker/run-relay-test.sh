#!/usr/bin/env bash
# Docker relay-proving test: extends the M6 dual-NAT rig with a dedicated
# Rust blind-relay server (relay-rust, see docker-compose.relay.yml) and
# forces both peers' `cp send`/`cp recv` traffic through it via
# PEEROXIDE_FORCE_RELAY, verifying peeroxide-dht's BlindRelayServer
# actually bridges two independently-NATed peers end-to-end (not just a
# unit-test harness) and that the relay-through wire protocol
# (relay_through / relay_addresses in the noise handshake, blind-relay
# pair/unpair over Protomux) works against a real dual-NAT network
# topology.
#
# This does not disturb docker-compose.nat.yml or run-nat-test.sh — it
# layers docker-compose.relay.yml on top via `-f ... -f ...` so the plain
# holepunch test keeps passing/failing independently of relay changes.
#
# Prerequisites: Docker with compose plugin, Linux containers
# Usage: bash run-relay-test.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== M6 Docker Relay Test ==="

command -v docker >/dev/null || { echo "ERROR: docker not found"; exit 1; }
docker compose version >/dev/null 2>&1 || { echo "ERROR: docker compose not found"; exit 1; }

BASE_FILE="$SCRIPT_DIR/docker-compose.nat.yml"
RELAY_FILE="$SCRIPT_DIR/docker-compose.relay.yml"
COMPOSE_ARGS=(-f "$BASE_FILE" -f "$RELAY_FILE")

cleanup() {
    echo "Cleaning up containers..."
    docker compose "${COMPOSE_ARGS[@]}" down --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

echo "Building containers..."
docker compose "${COMPOSE_ARGS[@]}" build

echo "Starting relay simulation..."
docker compose "${COMPOSE_ARGS[@]}" up --abort-on-container-exit --exit-code-from rust-receiver

echo "=== M6 Docker Relay Test PASSED ==="
