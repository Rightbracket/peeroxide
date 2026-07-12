/**
 * Standalone single HyperDHT node for the Docker dual-NAT rig
 * (tests/docker/). Each instance of this script is one real DHT node in
 * one container, given its own static IP on the `public` Docker network
 * (see docker-compose.nat.yml) - unlike an earlier version of this rig
 * which ran several DHT node identities in a single Node.js process (all
 * sharing one container/IP), this properly gives each node a genuinely
 * distinct, externally-routable address.
 *
 * That matters because HyperDHT nodes learn their own advertised address
 * from how *other* peers perceive the source of their requests (a
 * STUN-like mechanism - see dht-rpc's NatSampler). Running multiple node
 * identities in one process meant they bootstrapped against each other
 * over loopback, so they'd converge on "127.0.0.1" as their own address -
 * accurate for loopback peers, but useless to containers reaching in from
 * a different Docker network. With one node per container, each node's
 * peers genuinely see it arrive from its real container IP, so the
 * built-in address discovery converges correctly with no manual
 * workaround needed.
 *
 * This rig's small `public` DHT network is made up of several containers
 * each running one instance of this script (or, for a couple of nodes,
 * the real `peeroxide node` Rust binary instead - see
 * docker-compose.nat.yml) chained together via --bootstrap, so the
 * network is a genuine mix of Node.js and Rust HyperDHT implementations
 * talking the same wire protocol.
 *
 * A handful of nodes is a hard requirement, not just nice-to-have:
 * `HyperDhtHandle::announce`'s `closest_nodes` (peeroxide-dht/src/hyperdht.rs)
 * comes directly from the iterative closest-node query's replies, and
 * empirically that query returns zero replies against networks of only
 * 1-3 total nodes - `announce`/`lookup` then never resolve any peer at
 * all. This rig runs 6 total DHT nodes across the mesh to stay well
 * clear of that floor.
 *
 * It never exits on its own; the container is torn down by
 * `docker compose down` once the peer containers finish.
 *
 * Usage: node dht-node.js <port> [bootstrap-host:port ...]
 */

'use strict'

const DHT = require('hyperdht')

async function main () {
  const port = Number(process.argv[2] || process.env.DHT_PORT || 49737)
  const bootstrap = process.argv.slice(3).map((hp) => {
    const [host, portStr] = hp.split(':')
    return { host, port: Number(portStr) }
  })

  const node = new DHT({
    ephemeral: false,
    firewalled: false,
    host: '0.0.0.0',
    port,
    bootstrap
  })
  await node.fullyBootstrapped()

  process.stdout.write(JSON.stringify({
    ready: true,
    port: node.address().port,
    bootstrap
  }) + '\n')

  const shutdown = async () => {
    try { await node.destroy() } catch (_) {}
    process.exit(0)
  }
  process.on('SIGTERM', shutdown)
  process.on('SIGINT', shutdown)
}

main().catch((err) => {
  process.stderr.write((err && err.stack) || String(err))
  process.stderr.write('\n')
  process.exit(1)
})
