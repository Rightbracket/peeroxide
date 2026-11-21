//! Two swarms that find each other on a topic and dial each other at once
//! end up with one live connection between them: the duplicate is destroyed
//! on both sides, as Node's hyperswarm destroys one (`ERR_DUPLICATE`).

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use peeroxide::{spawn as spawn_swarm, JoinOpts, SwarmConfig, SwarmConnection};
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

/// Every connection delivered until none has arrived for `quiet`.
async fn delivered(
    rx: &mut tokio::sync::mpsc::Receiver<SwarmConnection>,
    quiet: Duration,
) -> Vec<SwarmConnection> {
    let mut conns = Vec::new();
    while let Ok(Some(conn)) = tokio::time::timeout(quiet, rx.recv()).await {
        conns.push(conn);
    }
    conns
}

/// What the swarms log, to tell when one met a duplicate: the decision
/// happens at the dial's end, and the losing connection may never reach the
/// application.
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
    fn dedups(&self) -> usize {
        String::from_utf8_lossy(&self.0.lock().unwrap()).matches("dedup: ").count()
    }
}

fn live(conns: &[SwarmConnection]) -> usize {
    conns
        .iter()
        .filter(|c| !c.peer.stream.get_ref().handle().is_closed())
        .count()
}

#[tokio::test]
async fn two_swarms_that_dial_each_other_keep_one_connection() {
    let log = Log::default();
    let writer = log.clone();
    tracing_subscriber::fmt()
        .with_max_level(tracing_subscriber::filter::LevelFilter::DEBUG)
        .with_writer(move || writer.clone())
        .init();
    tokio::time::timeout(Duration::from_secs(300), run(log))
        .await
        .expect("timed out")
        .expect("failed");
}

async fn run(log: Log) -> Result<(), Box<dyn std::error::Error>> {
    let rt = libudx::UdxRuntime::new()?;
    let (_, bs, _) = spawn(&rt, local_dht(vec![])).await?;
    let bootstrap = vec![format!("127.0.0.1:{}", bs.local_port().await?)];
    let mut duplicates_seen = 0;

    // Whether both dial at once is up to timing: rounds go on until a swarm
    // has met a duplicate twice, which it does in about half of them.
    for round in 0..12u8 {
        let topic = [0x80 + round; 32];
        let mut swarms = Vec::new();
        for _ in 0..2 {
            let mut config = SwarmConfig::default();
            config.dht = local_dht(bootstrap.clone());
            swarms.push(spawn_swarm(config).await?);
        }
        for (_, handle, _) in &swarms {
            handle.join(topic, JoinOpts::default()).await?;
        }

        let (mut a, mut b) = (swarms.remove(0), swarms.remove(0));
        let dedups_before = log.dedups();
        let (mut conns_a, mut conns_b) = tokio::join!(
            delivered(&mut a.2, Duration::from_secs(8)),
            delivered(&mut b.2, Duration::from_secs(8)),
        );
        assert!(!conns_a.is_empty() && !conns_b.is_empty(), "round {round}: no connection");
        duplicates_seen += usize::from(log.dedups() > dedups_before);
        // Destroys travel: give them a moment to land.
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(live(&conns_a), 1, "round {round}: a holds {} live of {}", live(&conns_a), conns_a.len());
        assert_eq!(live(&conns_b), 1, "round {round}: b holds {} live of {}", live(&conns_b), conns_b.len());

        // The one kept on each side is the same connection: it carries data
        // both ways.
        let keep = |conns: &mut Vec<SwarmConnection>| {
            let i = conns
                .iter()
                .position(|c| !c.peer.stream.get_ref().handle().is_closed())
                .expect("a live connection");
            conns.swap_remove(i)
        };
        let (mut kept_a, mut kept_b) = (keep(&mut conns_a), keep(&mut conns_b));
        assert_ne!(kept_a.is_initiator, kept_b.is_initiator, "round {round}");
        let a_is_larger = a.1.key_pair().public_key > b.1.key_pair().public_key;
        if log.dedups() > dedups_before {
            assert_eq!(kept_a.is_initiator, a_is_larger, "round {round}: the larger key's connection was not kept");
        }
        kept_a.peer.stream.write(b"from a").await?;
        let got = tokio::time::timeout(Duration::from_secs(5), kept_b.peer.stream.read()).await??;
        assert_eq!(got.as_deref(), Some(&b"from a"[..]), "round {round}");
        kept_b.peer.stream.write(b"from b").await?;
        let got = tokio::time::timeout(Duration::from_secs(5), kept_a.peer.stream.read()).await??;
        assert_eq!(got.as_deref(), Some(&b"from b"[..]), "round {round}");

        a.1.destroy().await?;
        b.1.destroy().await?;
        if duplicates_seen == 2 {
            break;
        }
    }
    assert!(duplicates_seen > 0, "no round had both swarms dial at once");
    eprintln!("rounds with a duplicate: {duplicates_seen}");
    bs.destroy().await?;
    Ok(())
}
