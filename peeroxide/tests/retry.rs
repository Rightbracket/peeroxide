//! A peer whose dial failed is dialled again once its wait is over.

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use peeroxide::{spawn as spawn_swarm, JoinOpts, KeyPair, SwarmConfig};
use peeroxide_dht::hyperdht::{spawn, HyperDhtConfig};
use peeroxide_dht::rpc::DhtConfig;

fn local_dht(bootstrap: Vec<String>) -> HyperDhtConfig {
    let mut dht = DhtConfig::default();
    dht.bootstrap = bootstrap;
    dht.port = 0;
    dht.host = "127.0.0.1".to_string();
    dht.firewalled = false;
    dht.ephemeral = Some(false);
    let mut config = HyperDhtConfig::default();
    config.dht = dht;
    config
}

/// What the swarm logs, to tell when its dial has failed.
#[derive(Clone, Default)]
struct Log(Arc<Mutex<Vec<u8>>>);

impl Write for Log {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Log {
    fn dials_failed(&self) -> usize {
        String::from_utf8_lossy(&self.0.lock().unwrap()).matches("peer connect failed").count()
    }
}

#[tokio::test]
async fn a_peer_whose_dial_failed_is_dialled_again() {
    let log = Log::default();
    let writer = log.clone();
    tracing_subscriber::fmt()
        .with_max_level(tracing_subscriber::filter::LevelFilter::INFO)
        .with_writer(move || writer.clone())
        .init();
    tokio::time::timeout(Duration::from_secs(200), run(log))
        .await
        .expect("timed out")
        .expect("failed");
}

async fn run(log: Log) -> Result<(), Box<dyn std::error::Error>> {
    let rt = libudx::UdxRuntime::new()?;
    let (_, bs, _) = spawn(&rt, local_dht(vec![])).await?;
    let bootstrap = vec![format!("127.0.0.1:{}", bs.local_port().await?)];
    let mut nodes = Vec::new();
    for _ in 0..2 {
        let (_, node, _) = spawn(&rt, local_dht(bootstrap.clone())).await?;
        node.bootstrapped().await?;
        nodes.push(node);
    }
    let topic = [0x90; 32];
    let server_keys = KeyPair::generate();
    let server_config = || {
        let mut config = SwarmConfig::default();
        config.dht = local_dht(bootstrap.clone());
        config.key_pair = Some(server_keys.clone());
        config
    };
    let server_only = || {
        let mut opts = JoinOpts::default();
        opts.client = false;
        opts
    };

    // A server announces the topic, then goes away: its record stays, and the
    // client's first dial of it fails.
    // Stopped, not destroyed, so that it does not take its announce back.
    let (gone_task, gone, _) = spawn_swarm(server_config()).await?;
    gone.join(topic, server_only()).await?;
    gone.flush().await?;
    gone_task.abort();
    let _ = gone_task.await;
    drop(gone);

    let mut config = SwarmConfig::default();
    config.dht = local_dht(bootstrap.clone());
    let (_, client, mut conns) = spawn_swarm(config).await?;
    let mut opts = JoinOpts::default();
    opts.server = false;
    client.join(topic, opts).await?;
    client.flush().await?;
    while log.dials_failed() == 0 {
        assert!(conns.try_recv().is_err(), "a connection to a server that is gone");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // The same peer is back: the client's retry reaches it.
    let (_, back, mut accepted) = spawn_swarm(server_config()).await?;
    back.join(topic, server_only()).await?;
    back.flush().await?;
    let conn = tokio::time::timeout(Duration::from_secs(60), conns.recv())
        .await
        .map_err(|_| "the peer whose dial failed was never dialled again")?
        .ok_or("connection channel closed")?;
    assert_eq!(conn.remote_public_key(), &server_keys.public_key);
    let accepted = tokio::time::timeout(Duration::from_secs(10), accepted.recv())
        .await
        .map_err(|_| "the server delivered no connection")?
        .ok_or("connection channel closed")?;
    assert_eq!(accepted.remote_public_key(), &client.key_pair().public_key);

    client.destroy().await?;
    back.destroy().await?;
    for node in &nodes {
        node.destroy().await?;
    }
    bs.destroy().await?;
    Ok(())
}
