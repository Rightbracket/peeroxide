# Chat Subsystem Overview

Peeroxide chat provides a serverless, end-to-end encrypted messaging environment built on the HyperDHT. It enables real-time communication without centralized accounts, phone numbers, or servers. Every identity is a public key, and every message is a cryptographically signed and encrypted record stored briefly in the distributed hash table.

## Why Chat?

Traditional messaging apps rely on central servers to store your messages, manage your identity, and route your traffic. Peeroxide chat removes these intermediaries. It treats the network as a shared space where peers discover each other through topics and exchange data directly.

This design ensures:
- **Censorship Resistance**: There is no central point to shut down.
- **Privacy by Default**: All messages are encrypted. Metadata is minimized through epoch-based topic rotation.
- **Self-Sovereign Identity**: You own your cryptographic keys. Your identity is not tied to a service provider.

## Identity Model

Your identity in Peeroxide is an Ed25519 keypair. This keypair is stored in a local profile. When you send a message, it is signed with your private key, allowing anyone with your public key to verify that it came from you.

Profiles allow you to manage multiple identities on one machine. Each profile includes:
- A permanent secret seed.
- An optional screen name.
- An optional biography.
- A friends list.

Separately, a shared known-users name cache lives at `~/.config/peeroxide/chat/known_users`. It is process-wide (not per profile) and acts as a soft directory mapping public keys to the most-recently-seen screen name for each peer you have encountered.

Set `PEEROXIDE_HOME` to keep chat state under another directory in place of `~`; see [Profile Directory Layout](./reference.md#profile-directory-layout).

## Channels

Peeroxide uses a topic-based discovery system. A "channel" is simply a name that maps to a DHT topic.

### Public Channels
Public channels use a well-known derivation for their discovery topic. Anyone who knows the channel name (e.g., `general` or `rust-dev`) can join, read history, and post messages.

### Private Channels
Private channels add a secret "group salt" to the topic derivation. Only peers who possess the salt can discover the channel topic or decrypt the messages within it. This enables private group conversations on the public DHT without revealing the participants or the content to outsiders.

## Direct Messaging (DMs)

Direct messaging allows private, one-to-one communication between two specific public keys.

When you start a DM with another user, Peeroxide derives a unique `dm_channel_key` using your public key and theirs. Because the derivation is order-independent, both parties arrive at the same key. The communication is further secured using an ephemeral shared secret derived via X25519 Elliptic Curve Diffie-Hellman (ECDH).

## The Inbox Concept

Because there is no server to hold messages while you are offline, Peeroxide uses an "Inbox" mechanism to facilitate discovery.

Your inbox is a set of rotating DHT topics derived from your public key. When someone wants to start a DM or invite you to a private channel, they generate a one-shot invite-feed keypair, publish an encrypted `InviteRecord` at that feed via `mutable_put`, and then `announce` that feed on your current inbox topic.

Your client periodically monitors these topics. When a new invite appears, it notifies you and provides the necessary keys to join the conversation. This "nudge" mechanism allows peers to find each other even if they aren't currently in the same channel.

## Profiles and the Nexus

The "Nexus" is your personal landing page on the DHT. It contains your screen name and biography. When you are active, your client publishes your Nexus record directly under your identity public key (via `mutable_put` on that key, with no extra topic derivation).

Your friends fetch your Nexus by `mutable_get`-ing your identity public key, picking up name and bio updates. This keeps your identity consistent across different channels and sessions.

For more details on the technical implementation, see [Wire Format](./wire-format.md) and [Protocol](./protocol.md).
