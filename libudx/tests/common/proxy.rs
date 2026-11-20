#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use tokio::net::UdpSocket;
use tokio::task::JoinHandle;

#[derive(Clone, Debug)]
pub struct DirectionConfig {
    /// [0.0, 1.0) — packet drop probability.
    pub loss_rate: f64,
    pub delay: Duration,
    /// Uniform ±jitter added on top of base delay.
    pub jitter: Duration,
    /// [0.0, 1.0) — probability of adding 10–50ms extra delay to cause reorder.
    pub reorder_rate: f64,
    /// Overwrite recv_window header field (bytes 8–11, LE u32) in forwarded packets.
    pub recv_window_override: Option<u32>,
    /// Drop packets exceeding this byte size (simulates path MTU).
    pub max_packet_size: Option<usize>,
}

impl Default for DirectionConfig {
    fn default() -> Self {
        Self {
            loss_rate: 0.0,
            delay: Duration::ZERO,
            jitter: Duration::ZERO,
            reorder_rate: 0.0,
            recv_window_override: None,
            max_packet_size: None,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ProxyConfig {
    pub a_to_b: DirectionConfig,
    pub b_to_a: DirectionConfig,
}

impl ProxyConfig {
    pub fn symmetric(config: DirectionConfig) -> Self {
        Self {
            a_to_b: config.clone(),
            b_to_a: config,
        }
    }
}

pub struct UdpProxy {
    /// Peer A sends here; proxy forwards to Peer B.
    pub addr_for_a: SocketAddr,
    /// Peer B sends here; proxy forwards to Peer A.
    pub addr_for_b: SocketAddr,
    handles: Vec<JoinHandle<()>>,
}

impl UdpProxy {
    pub async fn start(addr_a: SocketAddr, addr_b: SocketAddr, config: ProxyConfig) -> Self {
        let sock_a = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("bind proxy sock_a");
        let sock_b = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("bind proxy sock_b");

        let addr_for_a = sock_a.local_addr().expect("proxy addr_for_a");
        let addr_for_b = sock_b.local_addr().expect("proxy addr_for_b");

        let sock_a = Arc::new(sock_a);
        let sock_b = Arc::new(sock_b);

        // A→B
        let h1 = tokio::spawn(forward_loop(
            Arc::clone(&sock_a),
            Arc::clone(&sock_b),
            addr_b,
            config.a_to_b,
        ));

        // B→A
        let h2 = tokio::spawn(forward_loop(sock_b, sock_a, addr_a, config.b_to_a));

        Self {
            addr_for_a,
            addr_for_b,
            handles: vec![h1, h2],
        }
    }

    pub async fn stop(self) {
        for h in self.handles {
            h.abort();
            let _ = h.await;
        }
    }
}

async fn forward_loop(
    recv_sock: Arc<UdpSocket>,
    send_sock: Arc<UdpSocket>,
    dest: SocketAddr,
    config: DirectionConfig,
) {
    let mut buf = vec![0u8; 2048];

    loop {
        let (len, _src) = match recv_sock.recv_from(&mut buf).await {
            Ok(v) => v,
            Err(_) => break,
        };

        let mut packet = buf[..len].to_vec();

        // All RNG decisions computed before any .await (ThreadRng is !Send).
        let (should_drop, total_delay) = {
            let mut rng = rand::rng();

            let should_drop =
                config.loss_rate > 0.0 && rng.random::<f64>() < config.loss_rate;

            let mut total = config.delay;
            if config.jitter > Duration::ZERO {
                let jitter_us = config.jitter.as_micros() as f64;
                let offset_us = (rng.random::<f64>() * 2.0 - 1.0) * jitter_us;
                let offset = Duration::from_micros(offset_us.abs() as u64);
                total = if offset_us > 0.0 {
                    total + offset
                } else {
                    total.saturating_sub(offset)
                };
            }

            if config.reorder_rate > 0.0 && rng.random::<f64>() < config.reorder_rate {
                let extra_ms = rng.random_range(10u64..50);
                total += Duration::from_millis(extra_ms);
            }

            (should_drop, total)
        };

        if should_drop {
            continue;
        }

        if let Some(max_size) = config.max_packet_size {
            if packet.len() > max_size {
                continue;
            }
        }

        if let Some(rwnd) = config.recv_window_override {
            if packet.len() >= 12 {
                packet[8..12].copy_from_slice(&rwnd.to_le_bytes());
            }
        }

        if total_delay > Duration::ZERO {
            let send_sock = Arc::clone(&send_sock);
            tokio::spawn(async move {
                tokio::time::sleep(total_delay).await;
                let _ = send_sock.send_to(&packet, dest).await;
            });
        } else {
            let _ = send_sock.send_to(&packet, dest).await;
        }
    }
}
