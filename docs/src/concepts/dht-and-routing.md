# DHT and Routing

Peeroxide uses a Kademlia-based Distributed Hash Table (DHT) for peer discovery and coordination. This DHT is wire-compatible with the HyperDHT network used by Hyperswarm.

## Kademlia Basics

The DHT operates on several core Kademlia principles:

- **XOR Distance**: The "distance" between two nodes or a node and a key is calculated using the bitwise XOR of their 32-byte IDs. This metric defines the topology of the network.
- **Routing Table & k-buckets**: Each node maintains a routing table organized into buckets (k-buckets). Each bucket covers a specific range of distances from the node's own ID.
- **Iterative Lookup**: Finding a value or node involves querying the closest known nodes to the target key, which then return even closer nodes, eventually converging on the target.

Peeroxide relies on the [`pkarr`](https://docs.rs/pkarr) and [`mainline`](https://docs.rs/mainline) crates for much of its underlying DHT logic.

## Bootstrap Nodes

A DHT is a decentralized network, but new nodes need an entry point to join. These entry points are called **bootstrap nodes**.

- **Public Network**: By default, `peeroxide` uses a set of stable public bootstrap nodes to connect to the global HyperDHT network. If neither the config file's `network.bootstrap` nor the command-line `--bootstrap` flag supplies any nodes, the runtime auto-fills the public bootstrap set so a fresh install still connects.
- **Configuration**: You can supply custom bootstrap nodes via the `--bootstrap` flag or the `network.bootstrap` setting in your config file. **Note:** CLI `--bootstrap` overrides the config file's `network.bootstrap` rather than combining with it.
- **Public Default Adjustments**: `--public` explicitly adds the default public bootstrap nodes (useful when you have custom bootstraps but also want public connectivity). `--no-public` explicitly removes them from the resolved list.
- **Unreachable at start**: If no bootstrap node answers when a node starts, as when the network comes up after the program, the node keeps asking them: each query made while it knows fewer than 20 nodes also goes to the bootstrap nodes, and a node that knows none refreshes every 40 seconds. A bootstrap hostname that did not resolve at start is resolved again, at most every 10 seconds, while queries need it.
- **Isolated Mode**: Combining `--no-public` with no custom bootstraps (and no `network.bootstrap` in the config) yields an empty bootstrap list. In that state, the node has no entry point and can only be reached by peers who already know its address.

## Node IDs and the First Node

A persistent node (one that stores records for others, such as `peeroxide node`) has a node ID derived from its public IPv4 address and port. Other nodes check that ID against the address they see the node on, and ignore a node whose ID doesn't match: they don't add it to their routing tables or store announcements on it.

A node bound to an explicit address uses that address. A node bound to the wildcard address `0.0.0.0` learns its public address from the replies other nodes send to its requests, and runs as an ephemeral node (advertising no ID) until it has.

The first node of a new or private DHT has nobody to ask, so if it is bound to a wildcard address it can't learn its address, and a DHT built only on it can't store anything. Give it its public IPv4 address with `--public-host` (`node.public_host` in the config file, `DhtConfig::public_host` or `DhtConfig::bootstrapper` in `peeroxide-dht`), or bind `--host` to that address. Node.js `dht-rpc` has the same requirement: its `DHT.bootstrapper(port, host)` takes the public host explicitly. See [node](../node/overview.md#running-the-first-node-of-a-dht).

## Connectivity

The DHT doesn't just store peer records; it also facilitates connectivity:

- **Holepunching**: The DHT helps two firewalled peers coordinate a direct UDP connection.
- **Relaying**: If a direct connection is impossible, the DHT can help set up a relayed connection through other nodes.

For more details on how these primitives are used in practice, see the [lookup](../lookup/overview.md) and [announce](../announce/overview.md) command documentation.
