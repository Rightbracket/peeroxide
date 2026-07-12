#!/usr/bin/env bash
# Node.js counterpart to run-peer.sh, for the blind-relay interop scenario
# proving Node clients can be bridged by the Rust relay implementation
# (relay-rust) — see docker-compose.relay.yml / run-relay-interop-test.sh.
#
# Roles: sender (behind nat-a, relay-cp-send.js) / receiver (behind
# nat-b, relay-cp-recv.js). Mirrors run-peer.sh's NAT-gateway routing and
# mesh-settle timing; forces every connection through the relay via
# FORCE_RELAY_PUBKEY (required here, not optional — this scenario only
# exists to test the relay path).
#
# Usage: run-node-peer.sh <sender|receiver>
set -euo pipefail

ROLE="${1:?Usage: run-node-peer.sh <sender|receiver>}"
BOOTSTRAP_HOST="${BOOTSTRAP_HOST:-172.30.0.11}"
BOOTSTRAP_PORT="${BOOTSTRAP_PORT:-49737}"
TOPIC="${NAT_TEST_TOPIC:-6e6174746573746e6174746573746e6174746573746e6174746573746e6174}"
PAYLOAD_PREFIX="node-relay-interop payload"

ip route replace default via "${GATEWAY_IP:-10.0.1.1}"

if [ -z "${FORCE_RELAY_PUBKEY:-}" ]; then
  echo "FAIL: FORCE_RELAY_PUBKEY is required for run-node-peer.sh (this rig only tests the relay path)"
  exit 1
fi

case "$ROLE" in
  sender)
    printf '%s %s' "$PAYLOAD_PREFIX" "$(date -u +%Y%m%dT%H%M%S)" > /tmp/nat-payload.txt

    echo "node-peer/sender: letting DHT mesh settle..."
    sleep "${MESH_SETTLE_SECONDS:-6}"

    echo "node-peer/sender: starting relay-cp-send.js (topic=${TOPIC})"
    exec node /app/relay-cp-send.js /tmp/nat-payload.txt "$TOPIC" \
      --bootstrap "${BOOTSTRAP_HOST}:${BOOTSTRAP_PORT}" \
      --relay-through "${FORCE_RELAY_PUBKEY}"
    ;;
  receiver)
    echo "node-peer/receiver: letting DHT mesh + sender settle..."
    sleep "${RECEIVER_SETTLE_SECONDS:-10}"

    echo "node-peer/receiver: starting relay-cp-recv.js (topic=${TOPIC})"
    node /app/relay-cp-recv.js "$TOPIC" /tmp/nat-received.txt \
      --bootstrap "${BOOTSTRAP_HOST}:${BOOTSTRAP_PORT}" \
      --relay-through "${FORCE_RELAY_PUBKEY}" \
      --timeout-ms 90000

    echo "node-peer/receiver: verifying received file..."
    if [ ! -s /tmp/nat-received.txt ]; then
      echo "FAIL: /tmp/nat-received.txt missing or empty"
      exit 1
    fi

    received="$(cat /tmp/nat-received.txt)"
    echo "node-peer/receiver: received: ${received}"
    case "$received" in
      "${PAYLOAD_PREFIX}"*)
        echo "=== Node Relay Interop Test PASSED (Node clients bridged by Rust relay) ==="
        ;;
      *)
        echo "FAIL: unexpected payload content: ${received}"
        exit 1
        ;;
    esac
    ;;
  *)
    echo "Unknown role: $ROLE (expected sender|receiver)"
    exit 1
    ;;
esac
