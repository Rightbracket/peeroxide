//! Standalone entry point that runs a blind-relay server on top of a
//! [`HyperDhtHandle`].
//!
//! This module wires the protocol-only engine in [`crate::blind_relay`] to
//! real transport: it accepts incoming `PEER_HANDSHAKE` requests (via
//! [`ServerEvent`]), finalizes each into an encrypted connection, opens a
//! `"blind-relay"` Protomux channel on it, and drives a
//! [`BlindRelaySession`] against a shared [`BlindRelayServer`]. When a
//! pairing matches, it creates the two raw UDX data-plane streams and
//! bridges them with [`UdxStream::relay_to`] (packet-level, blind —
//! peeroxide never decrypts relayed application data).
//!
//! Deliberately independent of `peeroxide::Swarm`: a relay has no topics,
//! no peer discovery, and no retry bookkeeping, so this reimplements only
//! the minimal "accept a handshake, finalize a connection" path rather than
//! reusing `Swarm`'s internals. See `docs`/plan notes for the rationale.
//!
//! Holepunching is intentionally out of scope for v1: a relay is expected
//! to run with a directly reachable address (matches how
//! `blind-relay-service`'s reference deployment works — a plain
//! `dht.createServer()` with no NAT-traversal staging on the relay's own
//! control connections).

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use libudx::{RuntimeHandle, UdxRuntime, UdxStream};
use tokio::sync::mpsc;

use crate::blind_relay::{
    BlindRelayServer, BlindRelayServerConfig, BlindRelaySession, MatchedPairing, SessionOutbound,
};
use crate::hyperdht::{HyperDhtError, HyperDhtHandle, KeyPair, ServerEvent};
use crate::hyperdht_messages::{
    encode_handshake_to_bytes, FIREWALL_UNKNOWN, HandshakeMessage, MODE_FROM_RELAY,
    MODE_FROM_SECOND_RELAY, MODE_FROM_SERVER, MODE_REPLY, NoisePayload, SecretStreamInfo, UdxInfo,
};
use crate::noise::Keypair as NoiseKeypair;
use crate::noise_wrap::NoiseWrap;
use crate::protomux::Mux;
use crate::secret_stream::SecretStream;

static NEXT_RELAY_STREAM_ID: AtomicU32 = AtomicU32::new(1);

fn next_relay_stream_id() -> u32 {
    NEXT_RELAY_STREAM_ID.fetch_add(1, Ordering::Relaxed)
}

/// Configuration for [`run_relay_server`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RelayServiceConfig {
    /// Limits/timeouts for the shared [`BlindRelayServer`] engine.
    pub relay: BlindRelayServerConfig,
    /// How often to sweep expired (timed-out) pending pairings.
    pub sweep_interval: Duration,
    /// Firewall state advertised to connecting peers in the noise reply.
    /// A relay is expected to run reachably, so this defaults to
    /// `FIREWALL_UNKNOWN` (matches Node's default — the relay doesn't
    /// participate in NAT classification for its own control connections).
    pub firewall: u64,
}

impl Default for RelayServiceConfig {
    fn default() -> Self {
        Self {
            relay: BlindRelayServerConfig::default(),
            sweep_interval: Duration::from_secs(30),
            firewall: FIREWALL_UNKNOWN,
        }
    }
}

/// Errors from running the relay service.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RelayServiceError {
    /// The DHT server-event channel closed unexpectedly.
    #[error("DHT server-event channel closed")]
    ChannelClosed,
}

/// Run a blind-relay server against an already-spawned [`HyperDhtHandle`].
///
/// Consumes `server_rx` (the `ServerEvent` receiver returned by
/// `hyperdht::spawn`) until it closes or the returned `shutdown_rx` fires.
/// Returns the shared [`BlindRelayServer`] handle (for stats/inspection —
/// e.g. periodic logging) alongside the driving task.
pub fn run_relay_server(
    runtime_handle: Arc<RuntimeHandle>,
    dht: HyperDhtHandle,
    key_pair: KeyPair,
    mut server_rx: mpsc::UnboundedReceiver<ServerEvent>,
    config: RelayServiceConfig,
) -> (BlindRelayServer, tokio::task::JoinHandle<()>) {
    // Register our own identity so the DHT layer's handshake router treats
    // inbound `PEER_HANDSHAKE` requests targeting `hash(public_key)` as
    // "handle locally" instead of replying CLOSER_NODES (mirrors
    // `peeroxide::swarm`'s `do_join(server: true)` — without this, nothing
    // ever reaches this module's handshake handler at all).
    dht.register_server(&crate::crypto::hash(&key_pair.public_key));

    let relay = BlindRelayServer::new(config.relay.clone());
    let relay_for_task = relay.clone();

    let (matched_tx, matched_rx) = mpsc::unbounded_channel::<MatchedPairing>();

    // Background task: wires the raw UDX data-plane streams for every
    // matched pairing and bridges them bidirectionally.
    let bridge_runtime_handle = Arc::clone(&runtime_handle);
    let bridge_dht = dht.clone();
    tokio::spawn(bridge_matched_pairings(
        bridge_runtime_handle,
        bridge_dht,
        matched_rx,
    ));

    // Background task: periodically sweep pending pairings that timed out.
    let sweep_relay = relay.clone();
    let sweep_interval = config.sweep_interval;
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(sweep_interval);
        ticker.tick().await; // skip immediate first tick
        loop {
            ticker.tick().await;
            sweep_relay.sweep_expired_pairings();
        }
    });

    let firewall = config.firewall;
    let task = tokio::spawn(async move {
        while let Some(event) = server_rx.recv().await {
            match event {
                ServerEvent::PeerHandshake {
                    msg,
                    from: _,
                    peer_address,
                    target: _,
                    reply_tx,
                } => {
                    handle_handshake(
                        &runtime_handle,
                        &dht,
                        &key_pair,
                        firewall,
                        &relay_for_task,
                        &matched_tx,
                        msg,
                        peer_address,
                        reply_tx,
                    )
                    .await;
                }
                ServerEvent::PeerHolepunch { reply_tx, .. } => {
                    // Not supported in v1 — see module docs. Politely decline.
                    let _ = reply_tx.send(None);
                }
            }
        }
    });

    (relay, task)
}

#[allow(clippy::too_many_arguments)]
async fn handle_handshake(
    runtime_handle: &Arc<RuntimeHandle>,
    dht: &HyperDhtHandle,
    key_pair: &KeyPair,
    firewall: u64,
    relay: &BlindRelayServer,
    matched_tx: &mpsc::UnboundedSender<MatchedPairing>,
    msg: HandshakeMessage,
    peer_address: Option<crate::messages::Ipv4Peer>,
    reply_tx: tokio::sync::oneshot::Sender<Option<Vec<u8>>>,
) {
    let noise_kp = NoiseKeypair {
        public_key: key_pair.public_key,
        secret_key: key_pair.secret_key,
    };
    let mut nw = NoiseWrap::new_responder(noise_kp);

    tracing::debug!(mode = msg.mode, noise_len = msg.noise.len(), "relay: received handshake request");

    let remote_payload = match nw.recv(&msg.noise) {
        Ok(p) => p,
        Err(e) => {
            tracing::debug!(err = %e, "relay handshake: noise recv failed");
            let _ = reply_tx.send(None);
            return;
        }
    };

    if remote_payload.error != 0 {
        tracing::debug!(error = remote_payload.error, "relay handshake: remote reported error");
        let _ = reply_tx.send(None);
        return;
    }

    let local_stream_id = next_relay_stream_id();
    let addresses4 = dht.noise_addresses4(dht.local_port().await.unwrap_or(0)).await;

    let reply_payload = NoisePayload {
        version: 1,
        error: 0,
        firewall,
        holepunch: None,
        addresses4,
        addresses6: vec![],
        udx: Some(UdxInfo {
            version: 1,
            reusable_socket: true,
            id: u64::from(local_stream_id),
            seq: 0,
        }),
        secret_stream: Some(SecretStreamInfo { version: 1 }),
        relay_through: None,
        relay_addresses: None,
    };

    let noise_reply = match nw.send(&reply_payload) {
        Ok(b) => b,
        Err(e) => {
            tracing::debug!(err = %e, "relay handshake: noise send failed");
            let _ = reply_tx.send(None);
            return;
        }
    };

    let nw_result = match nw.finalize() {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!(err = %e, "relay handshake: noise finalize failed");
            let _ = reply_tx.send(None);
            return;
        }
    };

    let (reply_mode, reply_peer_address) = match msg.mode {
        MODE_FROM_RELAY | MODE_FROM_SECOND_RELAY => (MODE_FROM_SERVER, peer_address.clone()),
        _ => (MODE_REPLY, None),
    };

    let reply_msg = HandshakeMessage {
        mode: reply_mode,
        noise: noise_reply,
        peer_address: reply_peer_address,
        relay_address: None,
    };
    let _ = reply_tx.send(encode_handshake_to_bytes(&reply_msg).ok());

    let Some(remote_udx) = remote_payload.udx else {
        tracing::debug!("relay: connecting peer advertised no UDX info");
        return;
    };

    let runtime_handle = Arc::clone(runtime_handle);
    let dht = dht.clone();
    let relay = relay.clone();
    let matched_tx = matched_tx.clone();
    let remote_pk = nw_result.remote_public_key;

    tokio::spawn(async move {
        match finalize_relay_connection(runtime_handle, &dht, local_stream_id, &remote_udx, &nw_result)
            .await
        {
            Ok(mux) => {
                run_relay_session(relay, mux, remote_pk, matched_tx).await;
            }
            Err(e) => {
                tracing::debug!(err = %e, "relay: connection finalize failed");
            }
        }
    });
}

/// Finalize the accepted control connection: bind the raw UDX stream the
/// client dialed into, complete the Noise/SecretStream handshake, and wrap
/// the result in a Protomux [`Mux`] ready for a `"blind-relay"` channel.
///
/// Mirrors `peeroxide::swarm::create_server_connection` (kept independent —
/// see module docs).
async fn finalize_relay_connection(
    runtime_handle: Arc<RuntimeHandle>,
    dht: &HyperDhtHandle,
    local_stream_id: u32,
    remote_udx: &UdxInfo,
    noise_result: &crate::noise_wrap::NoiseWrapResult,
) -> Result<Mux, HyperDhtError> {
    let runtime = UdxRuntime::shared(runtime_handle);

    let remote_id = u32::try_from(remote_udx.id)
        .map_err(|_| HyperDhtError::StreamEstablishment("remote UDX id out of u32 range".into()))?;

    let socket = dht
        .server_socket()
        .await?
        .ok_or_else(|| HyperDhtError::StreamEstablishment("DHT server socket not available".into()))?;

    let stream = runtime.create_stream(local_stream_id).await?;
    stream.set_firewall_hook(&socket, remote_id, |_, _, _| true)?;

    let async_stream = stream.into_async_stream();
    let ss = SecretStream::from_session(
        false,
        async_stream,
        noise_result.tx,
        noise_result.rx,
        noise_result.handshake_hash,
        noise_result.remote_public_key,
    )
    .await
    .map_err(HyperDhtError::SecretStream)?;

    let (mux, run) = Mux::new(ss);
    tokio::spawn(run);
    Ok(mux)
}

async fn run_relay_session(
    relay: BlindRelayServer,
    mux: Mux,
    remote_public_key: [u8; 32],
    matched_tx: mpsc::UnboundedSender<MatchedPairing>,
) {
    let channel = match mux
        .create_channel(
            crate::blind_relay::PROTOCOL_NAME,
            Some(remote_public_key.to_vec()),
            None,
        )
        .await
    {
        Ok(c) => c,
        Err(e) => {
            tracing::debug!(err = %e, "relay: failed to open blind-relay channel");
            return;
        }
    };

    let Some(mut session) = BlindRelaySession::new(relay, channel) else {
        tracing::warn!("relay: rejecting session — at max_sessions capacity");
        return;
    };

    if session.wait_opened().await.is_err() {
        return;
    }

    session.run(&matched_tx).await;
}

/// Consume matched pairings, creating and bridging the two raw UDX
/// data-plane streams for each (mirrors Node's `createStream()` +
/// `stream.relayTo(remote.stream)`, both directions).
///
/// Bridged streams are kept alive in `active` for the life of the process
/// (a `UdxStream`'s `Drop` aborts its packet-forwarding task, so something
/// must own them for forwarding to keep working). There is no explicit
/// teardown wired from `unpair`/session-close back to these streams yet —
/// tracked as a follow-up; today they live until the relay process exits.
async fn bridge_matched_pairings(
    runtime_handle: Arc<RuntimeHandle>,
    dht: HyperDhtHandle,
    mut matched_rx: mpsc::UnboundedReceiver<MatchedPairing>,
) {
    let active: Arc<tokio::sync::Mutex<Vec<(UdxStream, UdxStream)>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));

    while let Some(matched) = matched_rx.recv().await {
        let runtime_handle = Arc::clone(&runtime_handle);
        let dht = dht.clone();
        let active = Arc::clone(&active);
        tokio::spawn(async move {
            match bridge_one_pairing(runtime_handle, &dht, matched).await {
                Ok(pair) => active.lock().await.push(pair),
                Err(e) => tracing::debug!(err = %e, "relay: failed to bridge matched pairing"),
            }
        });
    }
}

async fn bridge_one_pairing(
    runtime_handle: Arc<RuntimeHandle>,
    dht: &HyperDhtHandle,
    matched: MatchedPairing,
) -> Result<(UdxStream, UdxStream), HyperDhtError> {
    let runtime = UdxRuntime::shared(runtime_handle);
    let socket = dht
        .server_socket()
        .await?
        .ok_or_else(|| HyperDhtError::StreamEstablishment("DHT server socket not available".into()))?;

    let first_local_id = next_relay_stream_id();
    let second_local_id = next_relay_stream_id();

    let first_remote_id = u32::try_from(matched.first.client_stream_id)
        .map_err(|_| HyperDhtError::StreamEstablishment("client stream id out of range".into()))?;
    let second_remote_id = u32::try_from(matched.second.client_stream_id)
        .map_err(|_| HyperDhtError::StreamEstablishment("client stream id out of range".into()))?;

    let first_stream = runtime.create_stream(first_local_id).await?;
    first_stream.set_firewall_hook(&socket, first_remote_id, |_, _, _| true)?;

    let second_stream = runtime.create_stream(second_local_id).await?;
    second_stream.set_firewall_hook(&socket, second_remote_id, |_, _, _| true)?;

    // Blind, bidirectional packet-level forwarding — peeroxide never reads
    // or decrypts the relayed application data. Safe to wire up before
    // either side's first packet arrives: `UdxStream::process_incoming`
    // runs the firewall-hook gate (single-fire 4-tuple adoption) *before*
    // checking `relay_target`, so the packet that fires a stream's hook
    // still gets forwarded on this same pass rather than being absorbed
    // into normal (unread) stream processing — see the ordering comment
    // in `libudx::native::stream::process_incoming`.
    first_stream.relay_to(&second_stream)?;
    second_stream.relay_to(&first_stream)?;

    let _ = matched.first.outbound_tx.send(SessionOutbound::PairMatched {
        token: matched.token,
        is_initiator: matched.first.is_initiator,
        local_stream_id: u64::from(first_local_id),
    });
    let _ = matched.second.outbound_tx.send(SessionOutbound::PairMatched {
        token: matched.token,
        is_initiator: matched.second.is_initiator,
        local_stream_id: u64::from(second_local_id),
    });

    Ok((first_stream, second_stream))
}
