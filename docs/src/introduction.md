# Introduction

```text
,____  _____ _____ ____   _____  _____ ___  ,______ 
|  _ \| ____| ____|  _ \ / _ \ \/ /_ _|  _ \| ____|
| |_) |  _| |  _| | |_) | | | \  / | || | | |  _|  
|  __/| |___| |___|  _ <| |_| /  \ | || |_| | |___ 
|_|   |_____|_____|_| \_\\___/_/\_\___|____/|_____|

ENCRYPTED BY DEFAULT. PSEUDONYMOUS BY DESIGN.
NO SERVERS. NO ACCOUNTS. NO GATEKEEPERS.
TRUST NO ONE. TALK TO ANYONE.
```

`peeroxide-cli` is a command-line toolkit for interacting with the peeroxide P2P networking stack. It provides a set of tools for peer discovery, connectivity diagnostics, and decentralized data transfer, all while maintaining full wire-compatibility with the existing Hyperswarm and HyperDHT networks.

The binary is named `peeroxide`.

## Core Tools

The toolkit consists of nine primary commands:

- **[init](./init/overview.md)**: Generate configuration files and install man pages.
- **[node](./node/overview.md)**: Run a long-running DHT bootstrap / coordination node, optionally with a courtesy blind-relay.
- **[relay](./relay/overview.md)**: Run a standalone blind-relay server that bridges two NATed/firewalled peers who can't connect directly.
- **[lookup](./lookup/overview.md)**: Query the DHT to find peers announcing a specific topic.
- **[announce](./announce/overview.md)**: Announce your presence on a topic so others can discover you.
- **[ping](./ping/overview.md)**: Diagnose reachability through bootstrap checks, NAT classification, or targeted peer pings.
- **[cp](./cp/overview.md)**: Transfer files directly between peers over an encrypted swarm connection.
- **[dd (Dead Drop)](./dd/overview.md)**: Perform anonymous store-and-forward messaging via the DHT. The `dd` command supports both v1 and v2 protocols, with v2 auto-selected for new put operations.
- **[chat](./chat/overview.md)**: Join topic-based interactive chat rooms.

## Quick Start

It's recommended to run `peeroxide init` first to generate a default configuration and install system manual pages.

## Key Concepts

To use `peeroxide` effectively, it helps to understand the underlying architecture:

- **[DHT and Routing](./concepts/dht-and-routing.md)**: How the Kademlia-based Distributed Hash Table handles peer discovery and routing.
- **[Keys and Identity](./concepts/keys-and-identity.md)**: How Ed25519 keypairs define peer identity and secure connections.
- **[Topics and Discovery](./concepts/topics-and-discovery.md)**: How peers group together using 32-byte topic keys.
