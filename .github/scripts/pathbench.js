// udx-native counterpart of libudx/examples/pathbench.rs, for
// .github/scripts/pathsweep.py. Same arguments and output:
//
//   node pathbench.js recv|send <own addr> <other addr> <seconds> [limit=<bytes>]
//
// udx-native has no read buffer limit: a receiver ignores limit=.
//
// It finds udx-native through NODE_PATH, which pathsweep.py points at
// tests/node/node_modules.
const UDX = require('udx-native')

const [mode, own, other, seconds] = process.argv.slice(2)
if (!['recv', 'send'].includes(mode) || !other || !(+seconds > 0)) {
  console.error('usage: pathbench.js recv|send <own addr> <other addr> <seconds>')
  process.exit(2)
}
const split = (addr) => { const i = addr.lastIndexOf(':'); return [addr.slice(0, i), +addr.slice(i + 1)] }
const [ownHost, ownPort] = split(own)
const [otherHost, otherPort] = split(other)

const udx = new UDX()
const socket = udx.createSocket()
socket.bind(ownPort, ownHost)

if (mode === 'recv') {
  const stream = udx.createStream(2)
  stream.connect(socket, 1, otherPort, otherHost)
  let received = 0
  let started = null
  stream.on('data', (data) => {
    if (started === null) {
      started = process.hrtime.bigint()
      let ticks = 0
      const tick = () => {
        ticks++
        const at = Number(process.hrtime.bigint() - started) / 1e9
        console.log(`AT ${at.toFixed(3)} ${received}`)
        if (at >= +seconds) process.exit(0)
        const next = Number(started) / 1e6 + ticks * 100 + 100
        setTimeout(tick, Math.max(0, next - Number(process.hrtime.bigint()) / 1e6))
      }
      setTimeout(tick, 100)
    }
    received += data.length
  })
  stream.on('error', (err) => { console.error('recv error', err.message); process.exit(1) })
} else {
  const stream = udx.createStream(1)
  stream.connect(socket, 2, otherPort, otherHost)
  stream.on('error', (err) => { console.error('send error', err.message); process.exit(1) })
  const chunk = Buffer.alloc(64 * 1024, 7)
  const until = Date.now() + (+seconds + 60) * 1000
  const pump = () => {
    while (Date.now() < until) {
      if (!stream.write(chunk)) return
    }
    process.exit(0)
  }
  stream.on('drain', pump)
  pump()
}
