'use strict'

// Blind-relay server — Node.js reference-implementation relay for the
// Docker interop rig (tests/docker/docker-compose.relay.yml, service
// `relay-node`), and also usable standalone against the public network.
//
// Usage:
//   node blind-relay-server.js [--port N] [--host H]
//     [--bootstrap host:port ...] [--key-seed <64-char-hex>]
//
// All arguments are optional and backward compatible with the original
// no-arg invocation (public bootstrap network, random ephemeral
// listen port, random keypair). Pass --bootstrap (repeatable) to join a
// private/isolated mesh instead (e.g. the Docker rig's `public` network),
// and --key-seed for a deterministic public key across runs (so compose
// files can hardcode the relay's pubkey without parsing container logs).

const DHT = require('hyperdht')
const relay = require('blind-relay')
const b4a = require('b4a')

function parseArgs (argv) {
  const opts = { port: 0, host: undefined, bootstrap: [], keySeed: undefined }
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]
    if (arg === '--port') {
      opts.port = Number(argv[++i])
    } else if (arg === '--host') {
      opts.host = argv[++i]
    } else if (arg === '--bootstrap') {
      const [host, portStr] = argv[++i].split(':')
      opts.bootstrap.push({ host, port: Number(portStr) })
    } else if (arg === '--key-seed') {
      opts.keySeed = argv[++i]
    }
  }
  return opts
}

async function main () {
  const opts = parseArgs(process.argv.slice(2))
  const usingPrivateMesh = opts.bootstrap.length > 0

  const dhtOpts = { port: opts.port }
  if (opts.host) dhtOpts.host = opts.host
  if (usingPrivateMesh) {
    // A private/isolated mesh needs an explicit, non-public DHT node —
    // mirrors dht-node.js's settings for the same rig.
    dhtOpts.ephemeral = false
    dhtOpts.firewalled = false
    dhtOpts.bootstrap = opts.bootstrap
  }

  const dht = new DHT(dhtOpts)
  await dht.fullyBootstrapped()

  const relayServer = new relay.Server({
    createStream (streamOpts) {
      return dht.rawStreams.add(streamOpts)
    }
  })

  const server = dht.createServer(function (socket) {
    relayServer.accept(socket, { id: socket.remotePublicKey })
  })

  const keyPair = opts.keySeed
    ? DHT.keyPair(b4a.from(opts.keySeed, 'hex'))
    : DHT.keyPair()
  await server.listen(keyPair)

  const addr = dht.address()

  process.stdout.write(JSON.stringify({
    ready: true,
    publicKey: b4a.toString(keyPair.publicKey, 'hex'),
    host: addr.host,
    port: addr.port
  }) + '\n')

  // Shut down on SIGTERM/SIGINT only (matches dht-node.js's convention for
  // this rig's long-running containers). The original stdin-'end'-based
  // shutdown hook is removed: under Docker (no `stdin_open`/`-i`), stdin
  // is already closed when the process starts, so `process.stdin.resume()`
  // would fire an immediate 'end' event and exit the process right after
  // printing the ready line — before it ever accepts a connection.
  const shutdown = async () => {
    try {
      await relayServer.close()
      await server.close()
      await dht.destroy()
    } catch (_) {}
    process.exit(0)
  }
  process.on('SIGTERM', shutdown)
  process.on('SIGINT', shutdown)
}

main().catch((err) => {
  process.stderr.write(err.stack + '\n')
  process.exit(1)
})
