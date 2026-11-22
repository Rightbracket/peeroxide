# Topics and Discovery

In `peeroxide`, discovery is organized around **topics**. A topic is a 32-byte key that serves as a rendezvous point for peers with shared interests.

## What is a Topic?

Technically, a topic is always a 32-byte value. However, the CLI allows you to specify topics in two ways:

1. **Raw Hex**: A 64-character hexadecimal string is interpreted directly as the 32-byte topic key.
2. **Plaintext Name**: Any other string is hashed using **BLAKE2b-256** to derive a 32-byte topic key. For example, the topic name `"my-application"` becomes the hash of those bytes.

This dual approach allows for both human-readable "namespaced" discovery and opaque, randomly generated "private" rendezvous points.

## How Discovery Works

The discovery process involves two main actions:

### Announcing
When you **announce** on a topic, you are telling the DHT that your peer identity (public key) is available for connections related to that topic. The swarm automatically handles re-announcing at regular intervals (every 10 to 12 minutes) to ensure your record stays fresh on the DHT. An announce or lookup that fails or reaches no DHT node, as when the network is not up yet, is tried again after a few seconds, backing off to once a minute, so a peer that joins a topic before it can reach the DHT is announced and finds its peers soon after it can.

### Looking Up
When you **lookup** a topic, you query the DHT for the public keys and addresses of all peers currently announcing on that topic.

## Usage in Tools

Topic-based discovery is the foundation for several `peeroxide` commands:

- **[lookup](../lookup/overview.md)**: Find peers on a topic.
- **[announce](../announce/overview.md)**: Join a topic.
- **[cp](../cp/overview.md)**: Uses a topic as a one-time rendezvous point for a file transfer.
- **[ping](../ping/overview.md)**: Can resolve a topic name to find and ping the associated peers.

By using the same topic name or hex key across different tools and peers, you can easily build decentralized workflows without needing a central coordinator.
