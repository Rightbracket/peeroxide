//! The address other nodes see this one at, worked out as the Node.js
//! version works it out for `dht.host`, `dht.port` and `dht.remoteAddress()`
//! (`dht-rpc`, with the `nat-sampler` package, 1.0.1).
//!
//! Each message from another node says the address it was sent to: this
//! node's, as that node sees it. The sampler keeps the latest sixteen of
//! those, and settles on a host and a port when nearly all of them agree:
//! all of the first three, and all but one, two or three once there are 4,
//! 8 or 12. Failing that, it settles on a host alone, with port 0, as when
//! the NAT gives each destination a port of its own. Which messages count is
//! up to [`crate::rpc`].
//!
//! It is not [`crate::nat::Nat`], the holepunch analysis, which classifies
//! the NAT of a firewalled node and gives an open one no address, as the
//! Node.js version's `lib/nat.js` does.

use std::collections::HashMap;

use crate::messages::Ipv4Peer;

/// Sixteen samples, each kept twice: with its port, and with port 0.
const WINDOW: usize = 32;

/// One kept address. Entries with the same `id` are one sample of the
/// Node.js version's, which `_bump` hands back to be counted again.
#[derive(Clone)]
struct Entry {
    id: u64,
    host: String,
    port: u16,
}

pub(crate) struct NatSampler {
    /// The host the latest samples agree on, if they do.
    pub(crate) host: Option<String>,
    /// The port they agree on as well, or 0 if they agree on the host alone.
    pub(crate) port: u16,
    size: usize,
    threshold: i64,
    top: usize,
    samples: Vec<Entry>,
    hits: HashMap<u64, i64>,
    a: Option<Entry>,
    b: Option<Entry>,
    next_id: u64,
}

impl NatSampler {
    pub(crate) fn new() -> Self {
        Self {
            host: None,
            port: 0,
            size: 0,
            threshold: 0,
            top: 0,
            samples: Vec::with_capacity(WINDOW),
            hits: HashMap::new(),
            a: None,
            b: None,
            next_id: 0,
        }
    }

    /// Count the address one node saw this one at.
    pub(crate) fn add(&mut self, host: &str, port: u16) {
        let a = self.bump(host, port, 2);
        let b = self.bump(host, 0, 1);

        if self.samples.len() < WINDOW {
            self.size += 1;
            let tolerated = match self.size {
                0..=3 => 0,
                4..=7 => 1,
                8..=11 => 2,
                _ => 3,
            };
            self.threshold = (self.size - tolerated) as i64;
            self.samples.push(a.clone());
            self.samples.push(b.clone());
            self.top += 2;
        } else {
            if self.top == WINDOW {
                self.top = 0;
            }
            let old = std::mem::replace(&mut self.samples[self.top], a.clone());
            self.top += 1;
            self.uncount(old.id);
            let old = std::mem::replace(&mut self.samples[self.top], b.clone());
            self.top += 1;
            self.uncount(old.id);
        }

        if self.a.as_ref().is_none_or(|best| self.hits_of(best) < self.hits_of(&a)) {
            self.a = Some(a);
        }
        if self.b.as_ref().is_none_or(|best| self.hits_of(best) < self.hits_of(&b)) {
            self.b = Some(b);
        }

        let (a, b) = (self.a.clone(), self.b.clone());
        match (a, b) {
            (Some(a), _) if self.hits_of(&a) >= self.threshold => {
                self.host = Some(a.host);
                self.port = a.port;
            }
            (_, Some(b)) if self.hits_of(&b) >= self.threshold => {
                self.host = Some(b.host);
                self.port = 0;
            }
            _ => {
                self.host = None;
                self.port = 0;
            }
        }
        self.forget_the_unkept();
    }

    /// The entry among the latest four of its kind with this address, counted
    /// once more, or a new one counted once.
    fn bump(&mut self, host: &str, port: u16, inc: usize) -> Entry {
        for i in 0..4 {
            let j = (self.top as isize - inc as isize - 2 * i as isize).rem_euclid(WINDOW as isize) as usize;
            let Some(entry) = self.samples.get(j) else {
                break;
            };
            if entry.port == port && entry.host == host {
                let entry = entry.clone();
                *self.hits.entry(entry.id).or_insert(0) += 1;
                return entry;
            }
        }
        let id = self.next_id;
        self.next_id += 1;
        self.hits.insert(id, 1);
        Entry {
            id,
            host: host.to_string(),
            port,
        }
    }

    fn hits_of(&self, entry: &Entry) -> i64 {
        self.hits.get(&entry.id).copied().unwrap_or(0)
    }

    fn uncount(&mut self, id: u64) {
        *self.hits.entry(id).or_insert(0) -= 1;
    }

    /// Drop the counts of entries that are neither kept nor the best so far,
    /// which nothing can count again.
    fn forget_the_unkept(&mut self) {
        let samples = &self.samples;
        let (a, b) = (self.a.as_ref().map(|e| e.id), self.b.as_ref().map(|e| e.id));
        self.hits.retain(|id, _| Some(*id) == a || Some(*id) == b || samples.iter().any(|e| e.id == *id));
    }

    /// The address the samples agree this node is reached at, as `dht-rpc`'s
    /// `remoteAddress()` gives it: none for a firewalled node, and none
    /// unless they agree on a host and on the port it listens on.
    pub(crate) fn remote_address(&self, firewalled: bool, listen_port: u16) -> Option<Ipv4Peer> {
        let host = self.host.as_ref()?;
        if self.port == 0 || firewalled || self.port != listen_port {
            return None;
        }
        Some(Ipv4Peer {
            host: host.clone(),
            port: self.port,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sampled(samples: &[(&str, u16)]) -> NatSampler {
        let mut sampler = NatSampler::new();
        for (host, port) in samples {
            sampler.add(host, *port);
        }
        sampler
    }

    fn settled(sampler: &NatSampler) -> (Option<&str>, u16) {
        (sampler.host.as_deref(), sampler.port)
    }

    fn peer(host: &str, port: u16) -> Option<Ipv4Peer> {
        Some(Ipv4Peer {
            host: host.to_string(),
            port,
        })
    }

    #[test]
    fn an_open_node_is_reached_at_the_agreed_address() {
        let s = sampled(&[("203.0.113.5", 4000)]);
        assert_eq!(s.remote_address(false, 4000), peer("203.0.113.5", 4000));
    }

    #[test]
    fn a_firewalled_node_has_no_remote_address() {
        let s = sampled(&[("203.0.113.5", 4000)]);
        assert_eq!(s.remote_address(true, 4000), None);
    }

    #[test]
    fn the_agreed_port_must_be_the_one_the_node_listens_on() {
        let s = sampled(&[("203.0.113.5", 4000)]);
        assert_eq!(s.remote_address(false, 4001), None);
    }

    #[test]
    fn a_host_alone_is_no_address() {
        let s = sampled(&[("203.0.113.5", 4000), ("203.0.113.5", 4001)]);
        assert_eq!(s.remote_address(false, 4000), None);
        assert_eq!(s.remote_address(false, 0), None);
    }

    #[test]
    fn nothing_is_known_before_a_sample() {
        assert_eq!(settled(&NatSampler::new()), (None, 0));
    }

    #[test]
    fn one_sample_settles_it() {
        assert_eq!(settled(&sampled(&[("203.0.113.5", 4000)])), (Some("203.0.113.5"), 4000));
    }

    #[test]
    fn the_first_samples_must_all_agree_on_the_port() {
        let s = sampled(&[("203.0.113.5", 4000), ("203.0.113.5", 4001)]);
        assert_eq!(settled(&s), (Some("203.0.113.5"), 0), "the host alone");
    }

    #[test]
    fn the_first_samples_must_all_agree_on_the_host() {
        let s = sampled(&[("203.0.113.5", 4000), ("198.51.100.7", 4000)]);
        assert_eq!(settled(&s), (None, 0));
    }

    #[test]
    fn from_four_samples_one_may_disagree() {
        let agree = ("203.0.113.5", 4000);
        let s = sampled(&[agree, agree, agree, ("198.51.100.7", 9)]);
        assert_eq!(settled(&s), (Some("203.0.113.5"), 4000));

        let s = sampled(&[agree, agree, ("198.51.100.7", 9), ("198.51.100.7", 9)]);
        assert_eq!(settled(&s), (None, 0), "two of four may not");
    }

    #[test]
    fn the_latest_sixteen_samples_count() {
        let old = ("203.0.113.5", 4000);
        let new = ("198.51.100.7", 5000);
        let mut s = sampled(&[old; 16]);
        assert_eq!(settled(&s), (Some("203.0.113.5"), 4000));

        // Thirteen of sixteen must agree: the old address holds for three new
        // samples, neither does from four to twelve, and the new one holds
        // from thirteen.
        for n in 1..=16 {
            s.add(new.0, new.1);
            let expected = match n {
                1..=3 => (Some("203.0.113.5"), 4000),
                4..=12 => (None, 0),
                _ => (Some("198.51.100.7"), 5000),
            };
            assert_eq!(settled(&s), expected, "after {n} new samples");
        }
    }

    #[test]
    fn it_keeps_no_count_of_what_it_has_let_go() {
        let mut s = NatSampler::new();
        for i in 0..1_000u16 {
            s.add(&format!("198.51.100.{}", i % 250), i);
        }
        assert!(s.hits.len() <= WINDOW + 2, "{} counts kept", s.hits.len());
    }

    /// It settles where the `nat-sampler` package settles, at every step of
    /// the sequences in `tests/interop/nat-sampler-fixtures.json`, which
    /// `tests/node/generate-nat-sampler-golden.js` writes.
    #[test]
    fn it_settles_where_the_node_package_does() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../tests/interop/nat-sampler-fixtures.json"
        );
        let data = std::fs::read_to_string(path).unwrap_or_else(|e| {
            panic!("Failed to read {path}: {e}. Run `node generate-nat-sampler-golden.js` in tests/node/ first.")
        });
        let file: serde_json::Value = serde_json::from_str(&data).expect("fixtures are JSON");
        let strings = |key: &str| -> Vec<String> {
            let values = file[key].as_array().expect(key);
            values.iter().map(|v| v.to_string().trim_matches('"').to_string()).collect()
        };
        let hosts = strings("hosts");
        let ports: Vec<u16> = strings("ports").iter().map(|p| p.parse().expect("a port")).collect();
        let digit = |c: Option<char>| c.and_then(|c| c.to_digit(10)).expect("an index") as usize;
        let written = |sampler: &NatSampler| match (&sampler.host, sampler.port) {
            (None, _) => "--".to_string(),
            (Some(host), port) => {
                let host = hosts.iter().position(|h| h == host).expect("a known host");
                match ports.iter().position(|p| *p == port) {
                    Some(port) => format!("{host}{port}"),
                    None => format!("{host}-"),
                }
            }
        };

        let sequences = file["sequences"].as_array().expect("sequences");
        assert!(!sequences.is_empty());
        for (n, sequence) in sequences.iter().enumerate() {
            let samples = sequence["samples"].as_str().expect("samples").split(' ');
            let settled = sequence["settled"].as_str().expect("settled").split(' ');
            let mut sampler = NatSampler::new();
            for (step, (sample, expected)) in samples.zip(settled).enumerate() {
                let mut sample = sample.chars();
                let (host, port) = (digit(sample.next()), digit(sample.next()));
                sampler.add(&hosts[host], ports[port]);
                assert_eq!(written(&sampler), expected, "sequence {n}, after sample {step}");
            }
        }
    }
}
