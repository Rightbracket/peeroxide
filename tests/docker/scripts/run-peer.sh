#!/usr/bin/env bash
# Runs one role of the M6 dual-NAT holepunch test.
#
# Roles:
#   sender   - behind nat-a. Runs `peeroxide cp send`, announcing a fixed
#              topic on the shared bootstrap and waiting for the receiver
#              to connect and pull the file (server side of the swarm).
#   receiver - behind nat-b. Runs `peeroxide cp recv`, looking up the same
#              topic and connecting to the sender (client side of the
#              swarm), then checks that a real file arrived.
#
# Both roles force the real network path (PEEROXIDE_LOCAL_CONNECTION=false)
# and disable the public bootstrap network (--no-public) so this test can
# only succeed via a genuine holepunch/relay through the two independent
# simulated NATs, never via a same-host/LAN shortcut or the real internet.
#
# Usage: run-peer.sh <sender|receiver>
set -euo pipefail

ROLE="${1:?Usage: run-peer.sh <sender|receiver>}"
BOOTSTRAP_HOST="${BOOTSTRAP_HOST:-172.30.0.11}"
BOOTSTRAP_PORT="${BOOTSTRAP_PORT:-49737}"
# Fixed 64-char hex topic shared by both peers for this test run.
TOPIC="${NAT_TEST_TOPIC:-6e6174746573746e6174746573746e6174746573746e6174746573746e6174}"
PAYLOAD_PREFIX="nat-holepunch-rig payload"

# Docker already installs its own default route via the network's bridge
# gateway (e.g. 10.0.1.254) before this script runs. `ip route add default`
# would fail silently against that ("File exists") and leave traffic going
# out via Docker's bridge gateway instead of our simulated NAT gateway -
# which has no forwarding/masquerade rules and silently drops everything,
# so DHT bootstrap requests would appear to "work" (the initial request
# happens to succeed against the bootstrap's single well-known port from
# some other path) while all subsequent replies are lost. Use `replace` so
# our NAT gateway becomes the only default route.
ip route replace default via "${GATEWAY_IP:-10.0.1.1}"

export PEEROXIDE_LOCAL_CONNECTION=false

# Optional: force cp traffic through a blind-relay server (see
# docker-compose.relay.yml / run-relay-test.sh). Unset by default, so the
# plain NAT-holepunch test (docker-compose.nat.yml alone) is unaffected.
if [ -n "${FORCE_RELAY_PUBKEY:-}" ] && [ -n "${FORCE_RELAY_ADDR:-}" ]; then
  export PEEROXIDE_FORCE_RELAY="${FORCE_RELAY_PUBKEY}@${FORCE_RELAY_ADDR}"
  echo "peer: forcing relay through ${FORCE_RELAY_ADDR} (pubkey ${FORCE_RELAY_PUBKEY:0:16}...)"
fi

case "$ROLE" in
  sender)
    echo "peer/sender: waiting for bootstrap ${BOOTSTRAP_HOST}:${BOOTSTRAP_PORT}..."
    printf '%s %s' "$PAYLOAD_PREFIX" "$(date -u +%Y%m%dT%H%M%S)" > /tmp/nat-payload.txt

    # `depends_on` only guarantees the dht-node-* containers have *started*,
    # not that the 6-node public DHT mesh has finished cross-populating its
    # routing tables via their own bootstrap queries. A brief settle delay
    # here avoids racing the sender's announce against a still-converging
    # mesh (observed as artificially low closest_nodes counts).
    echo "peer/sender: letting DHT mesh settle..."
    sleep "${MESH_SETTLE_SECONDS:-6}"

    echo "peer/sender: starting cp send (topic=${TOPIC})"
    exec /usr/local/bin/peeroxide \
      --no-public \
      --bootstrap "${BOOTSTRAP_HOST}:${BOOTSTRAP_PORT}" \
      -vv \
      cp send /tmp/nat-payload.txt "$TOPIC"
    ;;
  receiver)
    echo "peer/receiver: waiting for bootstrap ${BOOTSTRAP_HOST}:${BOOTSTRAP_PORT}..."
    # Give the DHT mesh and the sender a moment to settle/announce before
    # we start looking it up (sender itself delays MESH_SETTLE_SECONDS
    # before announcing, so wait a bit longer than that here).
    sleep "${RECEIVER_SETTLE_SECONDS:-10}"

    echo "peer/receiver: starting cp recv (topic=${TOPIC})"
    /usr/local/bin/peeroxide \
      --no-public \
      --bootstrap "${BOOTSTRAP_HOST}:${BOOTSTRAP_PORT}" \
      -vv \
      cp recv "$TOPIC" /tmp/nat-received.txt --yes --force --timeout 90

    echo "peer/receiver: verifying received file..."
    if [ ! -s /tmp/nat-received.txt ]; then
      echo "FAIL: /tmp/nat-received.txt missing or empty"
      exit 1
    fi

    received="$(cat /tmp/nat-received.txt)"
    echo "peer/receiver: received: ${received}"
    case "$received" in
      "${PAYLOAD_PREFIX}"*)
        echo "=== M6 Docker NAT Test PASSED (holepunch + transfer verified) ==="
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
