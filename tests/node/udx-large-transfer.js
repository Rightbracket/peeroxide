const UDX = require('udx-native')
const readline = require('readline')
const crypto = require('crypto')

const u = new UDX()
const socket = u.createSocket()
socket.bind(0, '127.0.0.1')

const { port } = socket.address()
const LOCAL_ID = 1
const REMOTE_ID = 2

process.stdout.write(JSON.stringify({ port, localId: LOCAL_ID, remoteId: REMOTE_ID }) + '\n')

const rl = readline.createInterface({ input: process.stdin })
rl.once('line', (line) => {
  const info = JSON.parse(line)
  const remotePort = info.port
  const mode = info.mode || 'echo'
  const expectedSize = info.expectedSize || 0

  const stream = u.createStream(LOCAL_ID)
  stream.connect(socket, REMOTE_ID, remotePort, '127.0.0.1')

  process.stdout.write(JSON.stringify({ ready: true }) + '\n')

  stream.on('error', () => {})

  if (mode === 'echo') {
    stream.on('data', (data) => {
      stream.write(data)
    })
    stream.on('end', () => {
      stream.end()
    })
    stream.on('close', () => {
      socket.close()
      process.exit(0)
    })
  } else if (mode === 'receive_and_hash') {
    const hash = crypto.createHash('sha256')
    let totalReceived = 0

    stream.on('data', (data) => {
      hash.update(data)
      totalReceived += data.length
    })
    stream.on('end', () => {
      const digest = hash.digest('hex')
      process.stdout.write(JSON.stringify({ received: totalReceived, sha256: digest }) + '\n')
      stream.end()
    })
    stream.on('close', () => {
      socket.close()
      process.exit(0)
    })
  } else if (mode === 'send_and_receive') {
    const sendSize = info.sendSize || 102400
    const sendBuf = crypto.randomBytes(sendSize)
    const sendHash = crypto.createHash('sha256').update(sendBuf).digest('hex')

    const recvHash = crypto.createHash('sha256')
    let totalReceived = 0

    stream.on('data', (data) => {
      recvHash.update(data)
      totalReceived += data.length
    })
    stream.on('end', () => {
      const digest = recvHash.digest('hex')
      process.stdout.write(JSON.stringify({
        sent: sendSize,
        sentSha256: sendHash,
        received: totalReceived,
        receivedSha256: digest,
        retransmits: stream.retransmits,
        packetsTransmitted: stream.packetsTransmitted
      }) + '\n')
      stream.end()
    })
    stream.on('close', () => {
      socket.close()
      process.exit(0)
    })

    stream.write(sendBuf, () => {
      stream.end()
    })
  }
})
