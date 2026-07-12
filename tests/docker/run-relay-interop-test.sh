#!/usr/bin/env bash
# Node.js relay-interop proving test: two scenarios, each layering a
# scenario-specific overlay on top of the base M6 NAT rig
# (docker-compose.nat.yml), run sequentially so a failure in one doesn't
# mask the other's result.
#
# Scenario A: real Rust `peeroxide cp` peers bridged by the Node.js
#             reference blind-relay server (relay-node) — proves our Rust
#             blind-relay *client* interoperates with Node's real server.
# Scenario B: real Node.js Hyperswarm peers (relay-cp-send.js /
#             relay-cp-recv.js) bridged by our Rust blind-relay server
#             (relay-rust) — proves our Rust blind-relay *server*
#             interoperates with Node's real Hyperswarm client.
#
# Does not disturb docker-compose.nat.yml, docker-compose.relay.yml, or
# their existing test scripts.
#
# Prerequisites: Docker with compose plugin, Linux containers
# Usage: bash run-relay-interop-test.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BASE_FILE="$SCRIPT_DIR/docker-compose.nat.yml"
RELAY_FILE="$SCRIPT_DIR/docker-compose.relay.yml"
INTEROP_A="$SCRIPT_DIR/docker-compose.relay-interop-a.yml"
INTEROP_B="$SCRIPT_DIR/docker-compose.relay-interop-b.yml"

command -v docker >/dev/null || { echo "ERROR: docker not found"; exit 1; }
docker compose version >/dev/null 2>&1 || { echo "ERROR: docker compose not found"; exit 1; }

cleanup() {
    docker compose -f "$BASE_FILE" -f "$RELAY_FILE" -f "$INTEROP_A" -f "$INTEROP_B" \
        down --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

echo "=== Scenario A: Rust peers via Node relay (relay-node) ==="
COMPOSE_A=(-f "$BASE_FILE" -f "$INTEROP_A")
docker compose "${COMPOSE_A[@]}" build
docker compose "${COMPOSE_A[@]}" up --abort-on-container-exit --exit-code-from rust-receiver
echo "=== Scenario A PASSED ==="

docker compose "${COMPOSE_A[@]}" down --remove-orphans

echo
echo "=== Scenario B: Node peers via Rust relay (relay-rust) ==="
COMPOSE_B=(-f "$BASE_FILE" -f "$RELAY_FILE" -f "$INTEROP_B")
docker compose "${COMPOSE_B[@]}" build
docker compose "${COMPOSE_B[@]}" up --abort-on-container-exit --exit-code-from node-receiver \
    dht-node-1 dht-node-2 dht-node-3 dht-node-4 dht-node-5 dht-node-6 \
    nat-gateway-a nat-gateway-b relay-rust node-sender node-receiver
echo "=== Scenario B PASSED ==="

echo
echo "=== Node Relay Interop Test PASSED (both directions verified) ==="
