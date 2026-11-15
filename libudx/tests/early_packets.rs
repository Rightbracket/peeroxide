#![deny(clippy::all)]

//! A packet that reaches a socket for a stream not yet registered there is
//! kept a little while, and the stream takes it in when it registers, as it
//! would in C libudx, where a stream receives from the moment it is made.
//!
//! A remote can send to a stream before the stream is registered: a server
//! dials a client as soon as it has the client's handshake, and the reply that
//! tells the client the id of the server's stream comes after, through a
//! relay. A packet lost there costs the connection the remote's retransmission
//! timeout, a second.
//!
//! The packets here come from a plain UDP socket, which never sends them
//! again: a stream that reads one took it from what its socket kept.

mod common;

use common::{create_bound_socket, create_runtime, with_timeout};
use libudx::{UdxRuntime, UdxSocket};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::UdpSocket;

/// The stream the packet is for, and the remote stream it comes from.
const STREAM: u32 = 10;
const REMOTE: u32 = 20;

/// The first data packet the remote stream sends, carrying `payload`.
fn first_packet(payload: &[u8]) -> Vec<u8> {
    let mut packet = vec![0xFF, 1, 0x01, 0]; // magic, version, DATA, data offset
    packet.extend_from_slice(&STREAM.to_le_bytes());
    packet.extend_from_slice(&(4u32 << 20).to_le_bytes()); // receive window
    packet.extend_from_slice(&0u32.to_le_bytes()); // seq
    packet.extend_from_slice(&0u32.to_le_bytes()); // ack
    packet.extend_from_slice(payload);
    packet
}

/// A socket whose receive task runs, as a DHT's does, and a remote that has
/// sent it the first packet for `STREAM`. Returns once the socket has read the
/// packet: what no stream takes, it passes on as a datagram.
async fn socket_with_an_early_packet(rt: &UdxRuntime) -> (UdxSocket, UdpSocket) {
    let (socket, addr) = create_bound_socket(rt).await;
    let mut datagrams = socket.recv_start().expect("recv_start");
    let remote = UdpSocket::bind("127.0.0.1:0").await.expect("bind the remote");
    remote
        .send_to(&first_packet(b"early"), addr)
        .await
        .expect("send the early packet");
    let datagram = datagrams.recv().await.expect("the socket reads the packet");
    assert_eq!(datagram.addr, remote.local_addr().expect("remote addr"));
    (socket, remote)
}

/// What the stream tells the remote it has taken in: the `ack` of the first
/// packet that comes for the remote stream.
async fn acknowledged(remote: &UdpSocket) -> u32 {
    let mut buf = [0u8; 2048];
    loop {
        let (len, _) = remote.recv_from(&mut buf).await.expect("receive on the remote");
        let to = u32::from_le_bytes(buf[4..8].try_into().expect("4 bytes"));
        if len >= 20 && to == REMOTE {
            return u32::from_le_bytes(buf[16..20].try_into().expect("4 bytes"));
        }
    }
}

#[tokio::test]
async fn a_stream_takes_in_a_packet_that_came_before_it_was_connected() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();
        let (socket, remote) = socket_with_an_early_packet(&rt).await;

        let mut stream = rt.create_stream(STREAM).await.expect("create the stream");
        stream
            .connect(&socket, REMOTE, remote.local_addr().expect("remote addr"))
            .await
            .expect("connect");

        let data = stream.read().await.expect("read").expect("data, not the end");
        assert_eq!(data, b"early");
        assert_eq!(
            acknowledged(&remote).await,
            1,
            "the stream acknowledges the packet, so the remote need not send it again"
        );
    })
    .await;
}

#[tokio::test]
async fn a_stream_waiting_on_its_firewall_hook_is_let_in_by_a_packet_that_came_before() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();
        let (socket, remote) = socket_with_an_early_packet(&rt).await;
        let remote_addr = remote.local_addr().expect("remote addr");

        let mut stream = rt.create_stream(STREAM).await.expect("create the stream");
        let (seen_tx, seen_rx) = std::sync::mpsc::channel::<SocketAddr>();
        stream
            .set_firewall_hook(&socket, REMOTE, move |_, port, ip| {
                let _ = seen_tx.send(SocketAddr::new(ip, port));
                true
            })
            .expect("set_firewall_hook");

        let data = stream.read().await.expect("read").expect("data, not the end");
        assert_eq!(data, b"early");
        assert_eq!(seen_rx.recv().expect("the hook saw the remote"), remote_addr);
        assert_eq!(acknowledged(&remote).await, 1);
    })
    .await;
}
