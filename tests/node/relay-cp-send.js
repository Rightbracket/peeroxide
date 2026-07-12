'use strict'

// relay-cp-send.js — minimal Node.js Hyperswarm "server" counterpart to
// `peeroxide cp send`, for blind-relay interop testing only (not a
// general-purpose tool). Announces a topic, and on the first incoming
// connection writes a file's bytes to the socket.
//
// Usage:
//   node relay-cp-send.js <file> <topic-hex>
//     [--bootstrap host:port ...] [--relay-through <pubkey-hex>]
//
// --relay-through forces every connection through the given blind-relay
// pubkey via Hyperswarm's `relayThrough` option (a function that always
// returns the pubkey, ignoring the `force` flag — the Node-side
// equivalent of peeroxide-cli's unconditional `PEEROXIDE_FORCE_RELAY`),
// so this only succeeds if the relay genuinely bridges the connection.

const fs = require('fs')
const Hyperswarm = require('hyperswarm')
const DHT = require('hyperdht')
const b4a = require('b4a')

function parseArgs (argv) {
  const opts = { bootstrap: [], relayThrough: undefined, positional: [] }
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]
    if (arg === '--bootstrap') {
      const [host, portStr] = argv[++i].split(':')
      opts.bootstrap.push({ host, port: Number(portStr) })
    } else if (arg === '--relay-through') {
      opts.relayThrough = argv[++i]
    } else {
      opts.positional.push(arg)
    }
  }
  return opts
}

async function main () {
  const opts = parseArgs(process.argv.slice(2))
  const [filePath, topicHex] = opts.positional
  if (!filePath || !topicHex) {
    console.error('usage: node relay-cp-send.js <file> <topic-hex> [--bootstrap host:port ...] [--relay-through <pubkey-hex>]')
    process.exit(1)
  }

  const payload = fs.readFileSync(filePath)
  const topic = b4a.from(topicHex, 'hex')

  const dhtOpts = {}
  if (opts.bootstrap.length > 0) {
    dhtOpts.ephemeral = true
    dhtOpts.bootstrap = opts.bootstrap
  }
  const dht = new DHT(dhtOpts)
  await dht.fullyBootstrapped()

  const swarmOpts = { dht }
  if (opts.relayThrough) {
    const relayPk = b4a.from(opts.relayThrough, 'hex')
    // Always return the relay pubkey regardless of `force`/NAT quality —
    // the interop test wants to *prove* the relay path, not rely on
    // whatever NAT the Docker rig happens to simulate.
    swarmOpts.relayThrough = () => relayPk
  }

  const swarm = new Hyperswarm(swarmOpts)

  swarm.on('connection', (socket, info) => {
    console.log(`connected: ${b4a.toString(info.publicKey, 'hex')} (initiator=${info.client})`)
    socket.end(payload)
    socket.on('error', (err) => console.error('conn error:', err.message))
  })

  const discovery = swarm.join(topic, { server: true, client: false })
  await discovery.flushed()
  console.log(`relay-cp-send: topic ${topicHex} announced (${payload.length} bytes)`)
  console.log(`relay-cp-send: dht table size=${dht.table ? dht.table.toArray().length : 'n/a'}`)

  process.on('SIGTERM', async () => {
    await swarm.destroy()
    process.exit(0)
  })
}

main().catch((err) => {
  console.error(err.stack || err)
  process.exit(1)
})
