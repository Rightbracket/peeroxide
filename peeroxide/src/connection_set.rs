use std::collections::HashMap;

use tokio::sync::mpsc;

pub(crate) struct ConnectionInfo {
    pub is_initiator: bool,
    slot: u64,
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
}

impl Drop for ConnectionSlot {
    fn drop(&mut self) {
        // The actor is gone once the swarm is destroyed; nothing to release.
        let _ = self.release_tx.send((self.public_key, self.id));
    }
}

pub(crate) struct ConnectionSet {
    by_public_key: HashMap<[u8; 32], ConnectionInfo>,
    next_slot: u64,
    release_tx: mpsc::UnboundedSender<SlotRelease>,
}

impl ConnectionSet {
    /// A set whose slots, when dropped, send their release to `release_tx`.
    pub fn new(release_tx: mpsc::UnboundedSender<SlotRelease>) -> Self {
        Self {
            by_public_key: HashMap::new(),
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
        self.by_public_key.insert(
            public_key,
            ConnectionInfo {
                is_initiator,
                slot: id,
            },
        );
        ConnectionSlot {
            public_key,
            id,
            release_tx: self.release_tx.clone(),
        }
    }

    pub fn remove(&mut self, public_key: &[u8; 32]) -> bool {
        self.by_public_key.remove(public_key).is_some()
    }

    /// Release the slot `id` of `public_key`, if it still holds it. Returns
    /// whether it did.
    pub fn release(&mut self, (public_key, id): SlotRelease) -> bool {
        match self.by_public_key.get(&public_key) {
            Some(info) if info.slot == id => {
                self.by_public_key.remove(&public_key);
                true
            }
            _ => false,
        }
    }

    pub fn len(&self) -> usize {
        self.by_public_key.len()
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

    #[test]
    fn remove() {
        let (mut set, _rx) = set();
        let pk = [2u8; 32];
        let _slot = set.add(pk, false);
        assert!(set.remove(&pk));
        assert!(!set.has(&pk));
        assert_eq!(set.len(), 0);
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
        assert!(set.release(rx.try_recv().unwrap()));
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
        assert!(!set.release(rx.try_recv().unwrap()));
        assert!(set.has(&pk));

        drop(new);
        assert!(set.release(rx.try_recv().unwrap()));
        assert!(!set.has(&pk));
    }
}
