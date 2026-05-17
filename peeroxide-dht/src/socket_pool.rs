//! UDP socket pool for NAT hole-punching and birthday-attack probe management.
//!
//! TODO(Wave 9): add module documentation.

#![allow(missing_docs)]

use std::net::SocketAddr;

use libudx::{Datagram, UdxRuntime, UdxSocket};
use tokio::sync::mpsc;

const HOLEPUNCH_MSG: &[u8] = &[0];
const HOLEPUNCH_TTL: u32 = 5;
const DEFAULT_TTL: u32 = 64;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SocketPoolError {
    #[error("udx error: {0}")]
    Udx(#[from] libudx::UdxError),
    #[error("invalid address: {0}")]
    AddrParse(#[from] std::net::AddrParseError),
}

pub type Result<T> = std::result::Result<T, SocketPoolError>;

pub struct SocketPool {
    host: String,
}

impl SocketPool {
    pub fn new(host: String) -> Self {
        Self { host }
    }

    pub async fn acquire(&self, runtime: &UdxRuntime) -> Result<SocketRef> {
        let socket = runtime.create_socket().await?;
        let addr: SocketAddr = format!("{}:0", self.host).parse()?;
        socket.bind(addr).await?;

        let (hp_tx, hp_rx) = mpsc::unbounded_channel();
        let recv_rx = socket.recv_start()?;

        let recv_task = tokio::spawn(route_messages(recv_rx, hp_tx));

        Ok(SocketRef {
            socket,
            holepunch_rx: Some(hp_rx),
            _recv_task: Some(recv_task),
        })
    }
}

async fn route_messages(
    mut recv_rx: mpsc::UnboundedReceiver<Datagram>,
    hp_tx: mpsc::UnboundedSender<HolepunchEvent>,
) {
    while let Some(dgram) = recv_rx.recv().await {
        if dgram.data.len() <= 1 {
            tracing::info!(addr = %dgram.addr, len = dgram.data.len(), "socket_pool: holepunch probe received");
            let _ = hp_tx.send(HolepunchEvent { addr: dgram.addr });
        }
        // DHT messages (>1 byte) on holepunch sockets are dropped — only the
        // primary client/server sockets in io.rs handle DHT protocol traffic.
    }
}

#[derive(Debug)]
pub struct HolepunchEvent {
    pub addr: SocketAddr,
}

pub struct SocketRef {
    pub socket: UdxSocket,
    pub holepunch_rx: Option<mpsc::UnboundedReceiver<HolepunchEvent>>,
    _recv_task: Option<tokio::task::JoinHandle<()>>,
}

impl SocketRef {
    /// The holepunch receiver is `Some(...)` immediately after `SocketPool::acquire()`.
    /// The Holepuncher recv-adapter takes ownership through this accessor exactly once;
    /// subsequent calls return `None`.
    pub fn take_holepunch_rx(
        &mut self,
    ) -> Option<mpsc::UnboundedReceiver<HolepunchEvent>> {
        self.holepunch_rx.take()
    }

    pub fn send_holepunch(&self, addr: SocketAddr, low_ttl: bool) -> Result<()> {
        let _ttl = if low_ttl { HOLEPUNCH_TTL } else { DEFAULT_TTL };
        // TODO: TTL support requires udx_socket_set_ttl which isn't exposed yet.
        self.socket.send_to(HOLEPUNCH_MSG, addr)?;
        Ok(())
    }

    pub fn send_holepunch_to(&self, host: &str, port: u16, low_ttl: bool) -> Result<()> {
        let addr: SocketAddr = format!("{host}:{port}").parse()?;
        self.send_holepunch(addr, low_ttl)
    }
}

pub fn random_port() -> u16 {
    1000 + (rand::random::<f64>() * 64536.0) as u16
}

/// Map a raw `firewall` value into the punch-strategy bucket the Holepuncher
/// dispatcher understands.
///
/// Node coerces only `OPEN → CONSISTENT` (`hyperdht/lib/holepuncher.js:333-335`),
/// because Node's `Holepuncher.autoSample()` reliably classifies `UNKNOWN`
/// out of the way by pinging 4+ DHT nodes from the puncher socket before
/// `punch()` runs. We do NOT yet have an autoSample equivalent (tracked as
/// future work), so a fresh puncher's NAT stays `FIREWALL_UNKNOWN` and no
/// `punch()` strategy branch matches — the puncher silently gives up
/// without sending any probes, starving the peer's recv adapter of the
/// inbound probe that would emit `Connected`.
///
/// Phase 3 MVP divergence: also coerce `UNKNOWN → CONSISTENT`. This
/// optimistically assumes a cone NAT (the most common home-network case)
/// when classification hasn't settled, which is correct for the live
/// `test_live_cp_send_recv_no_lan` gate and any same-host hairpin
/// scenario. When the Rust port gains an autoSample-equivalent, this
/// coercion should narrow back to `OPEN`-only to match Node exactly.
pub fn coerce_firewall(fw: u64) -> u64 {
    use crate::hyperdht_messages::{FIREWALL_CONSISTENT, FIREWALL_OPEN, FIREWALL_UNKNOWN};
    if fw == FIREWALL_OPEN || fw == FIREWALL_UNKNOWN {
        FIREWALL_CONSISTENT
    } else {
        fw
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hyperdht_messages::{
        FIREWALL_CONSISTENT, FIREWALL_OPEN, FIREWALL_RANDOM, FIREWALL_UNKNOWN,
    };

    #[test]
    fn random_port_in_range() {
        for _ in 0..1000 {
            let p = random_port();
            assert!(p >= 1000);
        }
    }

    #[test]
    fn coerce_open_to_consistent() {
        assert_eq!(coerce_firewall(FIREWALL_OPEN), FIREWALL_CONSISTENT);
    }

    #[test]
    fn coerce_consistent_unchanged() {
        assert_eq!(coerce_firewall(FIREWALL_CONSISTENT), FIREWALL_CONSISTENT);
    }

    #[test]
    fn coerce_random_unchanged() {
        assert_eq!(coerce_firewall(FIREWALL_RANDOM), FIREWALL_RANDOM);
    }

    #[test]
    fn coerce_unknown_treated_as_consistent() {
        assert_eq!(coerce_firewall(FIREWALL_UNKNOWN), FIREWALL_CONSISTENT);
    }
}
