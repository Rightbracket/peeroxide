use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use libudx::UdxStreamHandle;
use tokio::sync::mpsc;

pub(crate) struct ConnectionInfo {
    pub is_initiator: bool,
    slot: u64,
    stream: Arc<Mutex<SlotStream>>,
}

/// The stream that holds a slot, once there is one, and whether a newer
/// connection with the same peer has replaced it.
#[derive(Default)]
struct SlotStream {
    handle: Option<UdxStreamHandle>,
    superseded: bool,
}

/// A slot released by a dropped [`ConnectionSlot`]: the peer's key and the
/// slot's id.
pub(crate) type SlotRelease = ([u8; 32], u64);

/// One connection's hold on a slot in a [`ConnectionSet`].
///
/// Dropping it asks the swarm actor, over the channel the set was made with,
/// to release the slot. Each slot has its own id, so that a connection that
/// was replaced by a newer one for the same key never frees the newer one's
/// slot when it is dropped.
pub(crate) struct ConnectionSlot {
    public_key: [u8; 32],
    id: u64,
    release_tx: mpsc::UnboundedSender<SlotRelease>,
    stream: Arc<Mutex<SlotStream>>,
}

impl ConnectionSlot {
    /// Tie the slot to the stream of the connection that took it, so that a
    /// newer connection with the same peer can close it. Returns `false` if
    /// one already has: the caller destroys the stream instead of delivering
    /// it.
    pub fn attach(&self, handle: UdxStreamHandle) -> bool {
        let mut stream = self.stream.lock().unwrap_or_else(|e| e.into_inner());
        if stream.superseded {
            return false;
        }
        stream.handle = Some(handle);
        true
    }
}

impl Drop for ConnectionSlot {
    fn drop(&mut self) {
        // The actor is gone once the swarm is destroyed; nothing to release.
        let _ = self.release_tx.send((self.public_key, self.id));
    }
}

/// What a slot's release let go of.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Released {
    /// The slot of the connection held with its peer.
    Current,
    /// The slot of a connection a newer one replaced, counted until now.
    Superseded,
    /// Nothing: the slot was let go of already.
    Stale,
}

pub(crate) struct ConnectionSet {
    by_public_key: HashMap<[u8; 32], ConnectionInfo>,
    /// Slots of connections a newer one replaced, counted until they are
    /// released: what they hold for the application to read is what
    /// `memory_budget` bounds.
    superseded: HashSet<u64>,
    next_slot: u64,
    release_tx: mpsc::UnboundedSender<SlotRelease>,
}

impl ConnectionSet {
    /// A set whose slots, when dropped, send their release to `release_tx`.
    pub fn new(release_tx: mpsc::UnboundedSender<SlotRelease>) -> Self {
        Self {
            by_public_key: HashMap::new(),
            superseded: HashSet::new(),
            next_slot: 0,
            release_tx,
        }
    }

    pub fn has(&self, public_key: &[u8; 32]) -> bool {
        self.by_public_key.contains_key(public_key)
    }

    pub fn get(&self, public_key: &[u8; 32]) -> Option<&ConnectionInfo> {
        self.by_public_key.get(public_key)
    }

    /// Take a slot for a connection with `public_key`, replacing any it
    /// held. The slot is held until the returned guard is dropped.
    pub fn add(&mut self, public_key: [u8; 32], is_initiator: bool) -> ConnectionSlot {
        let id = self.next_slot;
        self.next_slot += 1;
        let stream = Arc::new(Mutex::new(SlotStream::default()));
        self.by_public_key.insert(
            public_key,
            ConnectionInfo {
                is_initiator,
                slot: id,
                stream: Arc::clone(&stream),
            },
        );
        ConnectionSlot {
            public_key,
            id,
            release_tx: self.release_tx.clone(),
            stream,
        }
    }

    /// Replace the connection with `public_key`, as Node's hyperswarm does a
    /// duplicate (`destroy(ERR_DUPLICATE)`): its stream is destroyed, or, if
    /// it is still being set up, it is destroyed once it is, rather than
    /// delivered. Its slot is let go of here.
    pub fn supersede(&mut self, public_key: &[u8; 32]) -> bool {
        let Some(info) = self.by_public_key.remove(public_key) else {
            return false;
        };
        self.superseded.insert(info.slot);
        let mut stream = info.stream.lock().unwrap_or_else(|e| e.into_inner());
        stream.superseded = true;
        if let Some(handle) = stream.handle.take() {
            handle.destroy();
        }
        true
    }

    /// Release the slot `id` of `public_key`.
    pub fn release(&mut self, (public_key, id): SlotRelease) -> Released {
        if self.superseded.remove(&id) {
            return Released::Superseded;
        }
        match self.by_public_key.get(&public_key) {
            Some(info) if info.slot == id => {
                self.by_public_key.remove(&public_key);
                Released::Current
            }
            _ => Released::Stale,
        }
    }

    /// Slots taken: one for each peer connected, and one for each connection
    /// replaced and not yet released.
    pub fn len(&self) -> usize {
        self.by_public_key.len() + self.superseded.len()
    }

    /// Connections replaced and not yet released.
    pub fn superseded(&self) -> usize {
        self.superseded.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set() -> (ConnectionSet, mpsc::UnboundedReceiver<SlotRelease>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (ConnectionSet::new(tx), rx)
    }

    #[test]
    fn add_and_has() {
        let (mut set, _rx) = set();
        let pk = [1u8; 32];
        assert!(!set.has(&pk));
        let _slot = set.add(pk, true);
        assert!(set.has(&pk));
        assert_eq!(set.len(), 1);
    }

    #[tokio::test]
    async fn superseding_a_connection_destroys_its_stream() {
        let runtime = libudx::UdxRuntime::new().unwrap();
        let (mut set, mut rx) = set();
        let pk = [2u8; 32];

        // Set up already: destroyed at once, and counted until released.
        let stream = runtime.create_stream(1).await.unwrap();
        let slot = set.add(pk, false);
        assert!(slot.attach(stream.handle()));
        assert!(set.supersede(&pk));
        assert!(stream.handle().is_closed());
        assert!(!set.has(&pk));
        assert_eq!((set.len(), set.superseded()), (1, 1));
        drop(slot);
        assert_eq!(set.release(rx.try_recv().unwrap()), Released::Superseded);
        assert_eq!(set.len(), 0);

        // Still being set up: refused once it is, for its caller to destroy.
        let slot = set.add(pk, false);
        assert!(set.supersede(&pk));
        let stream = runtime.create_stream(2).await.unwrap();
        assert!(!slot.attach(stream.handle()));
        assert!(!set.supersede(&pk));
    }

    #[test]
    fn get_info() {
        let (mut set, _rx) = set();
        let pk = [3u8; 32];
        let _slot = set.add(pk, true);
        let info = set.get(&pk).expect("should exist");
        assert!(info.is_initiator);
    }

    #[test]
    fn dropping_a_slot_releases_it() {
        let (mut set, mut rx) = set();
        let pk = [4u8; 32];
        drop(set.add(pk, false));
        assert_eq!(set.release(rx.try_recv().unwrap()), Released::Current);
        assert!(!set.has(&pk));
        assert_eq!(set.len(), 0);
    }

    #[test]
    fn dropping_a_replaced_slot_keeps_the_newer_one() {
        let (mut set, mut rx) = set();
        let pk = [5u8; 32];
        let old = set.add(pk, true);
        let new = set.add(pk, false);
        drop(old);
        assert_eq!(set.release(rx.try_recv().unwrap()), Released::Stale);
        assert!(set.has(&pk));

        drop(new);
        assert_eq!(set.release(rx.try_recv().unwrap()), Released::Current);
        assert!(!set.has(&pk));
    }
}
