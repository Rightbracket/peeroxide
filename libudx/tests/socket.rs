#![deny(clippy::all)]

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use common::{
    create_bound_socket, create_connected_pair, create_runtime, with_timeout, DEFAULT_TIMEOUT,
};

/// How long a refusal is given to come back before the next packet is sent.
const REFUSAL_DELAY: Duration = Duration::from_millis(200);

/// The address of a local port that nothing listens on.
fn closed_port() -> SocketAddr {
    let unused = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind failed");
    unused.local_addr().expect("local_addr failed")
}

#[tokio::test]
async fn socket_bind_and_local_addr() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(DEFAULT_TIMEOUT, async {
        let runtime = create_runtime();
        let (socket, addr) = create_bound_socket(&runtime).await;

        assert!(addr.port() > 0, "expected non-zero port, got {}", addr.port());
        assert_eq!(addr.ip(), "127.0.0.1".parse::<std::net::IpAddr>().unwrap());

        socket.close().await.expect("close failed");
    })
    .await;
}

#[tokio::test]
async fn socket_send_recv_datagram() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(DEFAULT_TIMEOUT, async {
        let runtime = create_runtime();
        let (socket_a, addr_a) = create_bound_socket(&runtime).await;
        let (socket_b, addr_b) = create_bound_socket(&runtime).await;

        let mut rx = socket_b.recv_start().expect("recv_start failed");

        let test_data = b"hello datagram";
        socket_a
            .send_to(test_data, addr_b)
            .expect("send_to failed");

        let datagram = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("recv timed out")
            .expect("channel closed");

        assert_eq!(datagram.data, test_data);
        assert_eq!(datagram.addr.port(), addr_a.port());

        socket_a.close().await.expect("close a failed");
        socket_b.close().await.expect("close b failed");
    })
    .await;
}

#[tokio::test]
async fn socket_close() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(DEFAULT_TIMEOUT, async {
        let runtime = create_runtime();
        let socket = runtime
            .create_socket()
            .await
            .expect("create socket failed");
        socket
            .bind("127.0.0.1:0".parse::<SocketAddr>().unwrap())
            .await
            .expect("bind failed");

        socket.close().await.expect("close failed");
    })
    .await;
}

/// A socket keeps receiving after one of its datagrams is refused.
///
/// Windows reports the ICMP "port unreachable" that such a datagram draws as
/// an error on the socket's next receive (`WSAECONNRESET`). Other systems
/// report nothing. Either way the socket still works, so its receive loop
/// has to carry on.
#[tokio::test]
async fn socket_receives_after_a_refused_datagram() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(DEFAULT_TIMEOUT, async {
        let runtime = create_runtime();
        let (socket_a, _addr_a) = create_bound_socket(&runtime).await;
        let (socket_b, addr_b) = create_bound_socket(&runtime).await;

        let mut rx = socket_b.recv_start().expect("recv_start failed");

        socket_b
            .send_to(b"refused", closed_port())
            .expect("send_to failed");
        tokio::time::sleep(REFUSAL_DELAY).await;

        let test_data = b"still listening";
        socket_a.send_to(test_data, addr_b).expect("send_to failed");

        let datagram = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("socket stopped receiving after a refused datagram")
            .expect("channel closed");

        assert_eq!(datagram.data, test_data);

        socket_a.close().await.expect("close a failed");
        socket_b.close().await.expect("close b failed");
    })
    .await;
}

/// A stream keeps receiving after a datagram from its socket is refused.
///
/// A DHT node sends its RPC datagrams and carries its streams on one socket,
/// so one peer that has gone must not stall every stream on that socket.
#[tokio::test]
async fn stream_receives_after_a_refused_datagram() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(DEFAULT_TIMEOUT, async {
        let runtime = create_runtime();
        let (stream_a, mut stream_b, _socket_a, socket_b) = create_connected_pair(&runtime).await;

        socket_b
            .send_to(b"refused", closed_port())
            .expect("send_to failed");
        tokio::time::sleep(REFUSAL_DELAY).await;

        let test_data = b"still connected";
        let write = tokio::spawn(async move {
            stream_a.write(test_data).await.expect("write failed");
            stream_a
        });

        let received = tokio::time::timeout(Duration::from_secs(5), stream_b.read())
            .await
            .expect("stream stopped receiving after a refused datagram")
            .expect("read failed")
            .expect("unexpected EOF");

        assert_eq!(received, test_data);

        let _stream_a = write.await.expect("join write");
    })
    .await;
}
