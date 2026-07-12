#![deny(clippy::all)]

mod common;

use common::{create_bound_socket, create_runtime, with_timeout};
use std::time::Duration;

#[tokio::test]
async fn firewall_hook_accepts_first_packet() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();

        let (socket_a, addr_a) = create_bound_socket(&rt).await;
        let (socket_b, _addr_b) = create_bound_socket(&rt).await;

        let id_a = 10u32;
        let id_b = 20u32;

        let mut stream_a = rt.create_stream(id_a).await.expect("create stream_a");
        stream_a
            .set_firewall_hook(&socket_a, id_b, |_, _, _| true)
            .expect("set_firewall_hook");

        let stream_b = rt.create_stream(id_b).await.expect("create stream_b");
        stream_b
            .connect(&socket_b, id_a, addr_a)
            .await
            .expect("stream_b connect");

        let msg = b"hello from firewall hook test";
        stream_b.write(msg).await.expect("stream_b write");

        let data = stream_a
            .read()
            .await
            .expect("stream_a read")
            .expect("stream_a unexpected EOF");
        assert_eq!(&data[..], msg.as_ref());
    })
    .await;
}
