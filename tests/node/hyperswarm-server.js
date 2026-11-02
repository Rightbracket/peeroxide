'use strict'

const DHT = require('hyperdht')
const Hyperswarm = require('hyperswarm')
const b4a = require('b4a')
const readline = require('readline')

// Optional flags, used by the Node-client tests (no flag: unchanged behaviour).
//   --remote-client  the client skips hyperdht's same-host LAN shortcut and
//                    reports FIREWALL_UNKNOWN, as a NATed client on another
//                    network would; every node here is on 127.0.0.1, so it
//                    would otherwise take the shortcut. One more
//                    DHT node is started: a holepunching client needs 3 NAT
//                    samples (hyperdht lib/nat.js) before it will punch.
//   --open-client    the client's DHT node is reachable (firewalled: false),
//                    like a peer with a public address. It stays ephemeral,
//                    so it does not store the server's announce and reaches
//                    the server through relays.
//   --direct-client  the client's DHT node is reachable and persistent
//                    (ephemeral: false). On this small testnet it then stores
//                    the server's announce itself and sends the handshake
//                    straight to the server, which is the direct path.
const flags = new Set(process.argv.slice(2))

async function main () {
  const bootstrap = new DHT({
    ephemeral: false,
    firewalled: false,
    host: '127.0.0.1',
    port: 0,
    bootstrap: []
  })
  await bootstrap.fullyBootstrapped()
  const bsPort = bootstrap.address().port
  const bsAddr = [{ host: '127.0.0.1', port: bsPort }]

  const node1 = new DHT({ ephemeral: false, firewalled: false, host: '127.0.0.1', bootstrap: bsAddr })
  const node2 = new DHT({ ephemeral: false, firewalled: false, host: '127.0.0.1', bootstrap: bsAddr })
  await node1.fullyBootstrapped()
  await node2.fullyBootstrapped()

  const extraNodes = []
  if (flags.has('--remote-client')) {
    const node3 = new DHT({ ephemeral: false, firewalled: false, host: '127.0.0.1', bootstrap: bsAddr })
    await node3.fullyBootstrapped()
    extraNodes.push(node3)
  }

  const serverDht = new DHT({
    host: '127.0.0.1',
    bootstrap: bsAddr,
    ...(flags.has('--open-client') ? { firewalled: false } : {}),
    ...(flags.has('--direct-client') ? { ephemeral: false, firewalled: false } : {})
  })
  await serverDht.fullyBootstrapped()

  if (flags.has('--remote-client')) {
    const connect = serverDht.connect.bind(serverDht)
    serverDht.connect = (publicKey, opts = {}) => connect(publicKey, { ...opts, localConnection: false })
    // Report FIREWALL_UNKNOWN, as a client behind a NAT would. On loopback,
    // dht-rpc's NAT check can otherwise decide this node is reachable, and
    // the client then says FIREWALL_OPEN and is dialled instead of punching.
    serverDht.remoteAddress = () => null
  }

  const swarm = new Hyperswarm({ dht: serverDht })

  let connResolve = null

  swarm.on('connection', (socket, info) => {
    const remotePk = b4a.toString(info.publicKey, 'hex')

    socket.on('data', (data) => {
      process.stdout.write(JSON.stringify({
        event: 'data',
        from: remotePk,
        payload: b4a.toString(data, 'hex')
      }) + '\n')
    })

    socket.on('error', (err) => {
      process.stderr.write('conn error: ' + err.message + '\n')
    })

    socket.on('close', () => {
      process.stdout.write(JSON.stringify({
        event: 'disconnected',
        from: remotePk
      }) + '\n')
    })

    socket.write(b4a.from('hello from node'))

    process.stdout.write(JSON.stringify({
      event: 'connected',
      remotePk,
      isInitiator: info.client
    }) + '\n')

    if (connResolve) {
      connResolve(socket)
      connResolve = null
    }
  })

  const serverPk = b4a.toString(swarm.keyPair.publicKey, 'hex')

  const serverDhtPort = swarm.dht.address().port

  process.stdout.write(JSON.stringify({
    ready: true,
    port: bsPort,
    publicKey: serverPk,
    node1Port: node1.address().port,
    node2Port: node2.address().port,
    serverDhtPort
  }) + '\n')

  const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity })

  rl.on('line', async (line) => {
    let req
    try {
      req = JSON.parse(line.trim())
    } catch (e) {
      return
    }

    const { cmd, id } = req

    try {
      if (cmd === 'join') {
        const topic = b4a.from(req.topic, 'hex')
        const discovery = swarm.join(topic, { server: true, client: false })
        await discovery.flushed()
        process.stdout.write(JSON.stringify({ id, ok: true }) + '\n')

      } else if (cmd === 'join_client') {
        const topic = b4a.from(req.topic, 'hex')
        const discovery = swarm.join(topic, { server: false, client: true })
        await discovery.flushed()
        process.stdout.write(JSON.stringify({ id, ok: true }) + '\n')

      } else if (cmd === 'wait_connection') {
        const socket = await new Promise((resolve) => { connResolve = resolve })
        process.stdout.write(JSON.stringify({
          id,
          ok: true,
          remotePk: b4a.toString(socket.remotePublicKey, 'hex')
        }) + '\n')

      } else if (cmd === 'stats') {
        // hyperdht counts how each connection was opened: `open` when the
        // client dialled the server itself, `consistent`/`random` when it
        // started a holepunch. Neither moves when the client waited for the
        // server to dial it.
        process.stdout.write(JSON.stringify({ id, ok: true, punches: serverDht.stats.punches }) + '\n')

      } else if (cmd === 'shutdown') {
        await cleanup()
        process.exit(0)
      }
    } catch (err) {
      process.stdout.write(JSON.stringify({ id, ok: false, error: err.message }) + '\n')
    }
  })

  rl.on('close', async () => {
    await cleanup()
    process.exit(0)
  })

  async function cleanup () {
    try { await swarm.destroy() } catch (_) {}
    for (const node of extraNodes) {
      try { await node.destroy() } catch (_) {}
    }
    try { await node2.destroy() } catch (_) {}
    try { await node1.destroy() } catch (_) {}
    try { await bootstrap.destroy() } catch (_) {}
  }
}

main().catch((err) => {
  process.stderr.write(err.stack + '\n')
  process.exit(1)
})
