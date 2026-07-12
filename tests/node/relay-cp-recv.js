'use strict'

// relay-cp-recv.js — minimal Node.js Hyperswarm "client" counterpart to
// `peeroxide cp recv`, for blind-relay interop testing only (not a
// general-purpose tool). Joins a topic, connects to the announced peer,
// and writes everything it reads from the socket to a destination file.
//
// Usage:
//   node relay-cp-recv.js <topic-hex> <dest-file>
//     [--bootstrap host:port ...] [--relay-through <pubkey-hex>]
//     [--timeout-ms N]
//
// See relay-cp-send.js for --relay-through semantics.

const fs = require('fs')
const Hyperswarm = require('hyperswarm')
const DHT = require('hyperdht')
const b4a = require('b4a')

function parseArgs (argv) {
  const opts = { bootstrap: [], relayThrough: undefined, timeoutMs: 60000, positional: [] }
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]
    if (arg === '--bootstrap') {
      const [host, portStr] = argv[++i].split(':')
      opts.bootstrap.push({ host, port: Number(portStr) })
    } else if (arg === '--relay-through') {
      opts.relayThrough = argv[++i]
    } else if (arg === '--timeout-ms') {
      opts.timeoutMs = Number(argv[++i])
    } else {
      opts.positional.push(arg)
    }
  }
  return opts
}

async function main () {
  const opts = parseArgs(process.argv.slice(2))
  const [topicHex, destPath] = opts.positional
  if (!topicHex || !destPath) {
    console.error('usage: node relay-cp-recv.js <topic-hex> <dest-file> [--bootstrap host:port ...] [--relay-through <pubkey-hex>] [--timeout-ms N]')
    process.exit(1)
  }

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
    swarmOpts.relayThrough = () => relayPk
  }

  const swarm = new Hyperswarm(swarmOpts)

  const received = await new Promise((resolve, reject) => {
    let settled = false
    const finish = (fn, arg) => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      clearInterval(retryTimer)
      fn(arg)
    }

    const timer = setTimeout(
      () => finish(reject, new Error(`timed out after ${opts.timeoutMs}ms waiting for data`)),
      opts.timeoutMs
    )

    swarm.on('connection', (socket, info) => {
      console.log(`connected: ${b4a.toString(info.publicKey, 'hex')} (initiator=${info.client})`)
      const chunks = []
      socket.on('data', (chunk) => chunks.push(chunk))
      socket.on('end', () => finish(resolve, b4a.concat(chunks)))
      socket.on('error', (err) => finish(reject, err))
    })

    // Hyperswarm's own PeerDiscovery only auto-retries a failed/empty
    // lookup after a ~10-minute refresh interval (see
    // hyperswarm/lib/peer-discovery.js) — far too slow for this test.
    // Destroying and rejoining the topic on a short interval forces a
    // fresh `dht.lookup()` each time, mirroring `peeroxide cp recv`'s own
    // "retrying lookup..." loop, until a connection lands or we time out.
    let discovery = swarm.join(topic, { server: false, client: true })
    const retryTimer = setInterval(() => {
      if (settled) return
      console.log('relay-cp-recv: retrying lookup...')
      discovery.destroy().catch(() => {})
      discovery = swarm.join(topic, { server: false, client: true })
    }, 5000)
  })

  fs.writeFileSync(destPath, received)
  console.log(`relay-cp-recv: wrote ${received.length} bytes to ${destPath}`)

  await swarm.destroy()
  process.exit(0)
}

main().catch((err) => {
  console.error(err.stack || err)
  process.exit(1)
})
