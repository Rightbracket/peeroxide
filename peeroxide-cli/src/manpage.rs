//! Man page generation with rich descriptions, examples, and cross-references.

use clap::CommandFactory;
use std::io::Write;

const CONSOLIDATED: &[&str] = &["peeroxide-cp", "peeroxide-dd", "peeroxide-chat"];

/// Generate all man pages and return them as (filename_stem, content) pairs.
pub fn generate_all() -> Vec<(String, Vec<u8>)> {
    let cmd = super::Cli::command();
    let mut pages = Vec::new();
    collect_pages(cmd, "peeroxide", &mut pages);
    pages
}

fn collect_pages(cmd: clap::Command, prefix: &str, pages: &mut Vec<(String, Vec<u8>)>) {
    let page = render_page(cmd.clone(), prefix);
    pages.push((prefix.to_string(), page));

    for sub in cmd.get_subcommands() {
        if sub.is_hide_set() {
            continue;
        }
        let sub_prefix = format!("{prefix}-{}", sub.get_name());
        if CONSOLIDATED.contains(&sub_prefix.as_str()) {
            let page = render_consolidated_page(sub.clone(), &sub_prefix);
            pages.push((sub_prefix, page));
        } else {
            collect_pages(sub.clone(), &sub_prefix, pages);
        }
    }
}

fn render_page(cmd: clap::Command, name: &str) -> Vec<u8> {
    let cmd = cmd
        .display_name(name.to_owned())
        .bin_name(name.to_owned());
    let cmd = apply_long_about(cmd, name);

    let man = clap_mangen::Man::new(cmd.clone())
        .title(name.to_string())
        .manual("peeroxide manual".to_string())
        .source(format!(
            "peeroxide {}",
            cmd.get_version().unwrap_or_default()
        ));

    let mut buf: Vec<u8> = Vec::new();
    man.render_title(&mut buf).unwrap();
    man.render_name_section(&mut buf).unwrap();
    man.render_synopsis_section(&mut buf).unwrap();
    man.render_description_section(&mut buf).unwrap();
    man.render_options_section(&mut buf).unwrap();

    if cmd.has_subcommands() {
        man.render_subcommands_section(&mut buf).unwrap();
    }

    if let Some(examples) = examples_for(name) {
        write_examples_section(&mut buf, examples);
    }

    if let Some(exit) = exit_status_for(name) {
        write_section(&mut buf, "EXIT STATUS", exit);
    }

    if let Some(see_also) = see_also_for(name) {
        write_see_also_section(&mut buf, see_also);
    }

    buf
}

fn render_consolidated_page(cmd: clap::Command, name: &str) -> Vec<u8> {
    let cmd = cmd
        .display_name(name.to_owned())
        .bin_name(name.to_owned());
    let cmd = apply_long_about(cmd, name);

    let man = clap_mangen::Man::new(cmd.clone())
        .title(name.to_string())
        .manual("peeroxide manual".to_string())
        .source(format!(
            "peeroxide {}",
            cmd.get_version().unwrap_or_default()
        ));

    let mut buf: Vec<u8> = Vec::new();
    man.render_title(&mut buf).unwrap();
    man.render_name_section(&mut buf).unwrap();

    write_consolidated_synopsis(&mut buf, &cmd, name);
    man.render_description_section(&mut buf).unwrap();

    // If the parent command has its own non-global, non-hidden args
    // (e.g. peeroxide chat carries --debug / --probe / --line-mode),
    // surface them in an OPTIONS section before listing subcommands.
    let parent_has_own_args = cmd
        .get_arguments()
        .any(|a| !a.is_hide_set() && !is_global_arg(a) && !a.is_positional());
    if parent_has_own_args {
        man.render_options_section(&mut buf).unwrap();
    }

    write_consolidated_commands(&mut buf, &cmd, name);

    if let Some(examples) = examples_for(name) {
        write_examples_section(&mut buf, examples);
    }

    if let Some(exit) = exit_status_for(name) {
        write_section(&mut buf, "EXIT STATUS", exit);
    }

    if let Some(see_also) = see_also_for(name) {
        write_see_also_section(&mut buf, see_also);
    }

    buf
}

fn write_consolidated_synopsis(buf: &mut Vec<u8>, cmd: &clap::Command, parent_name: &str) {
    buf.write_all(b".SH SYNOPSIS\n").unwrap();
    let invocation_base = parent_name.replace('-', " ");
    write_synopsis_recursive(buf, cmd, &invocation_base);
}

fn write_synopsis_recursive(buf: &mut Vec<u8>, cmd: &clap::Command, invocation: &str) {
    for sub in cmd.get_subcommands() {
        if sub.is_hide_set() || sub.get_name() == "help" {
            continue;
        }
        let sub_invocation = format!("{invocation} {}", sub.get_name());

        if sub.get_subcommands().next().is_some() {
            // Subgroup: recurse to enumerate its leaves; do not emit a
            // synopsis line for the group itself.
            write_synopsis_recursive(buf, sub, &sub_invocation);
            continue;
        }

        writeln!(buf, ".B {sub_invocation}").unwrap();
        let mut opts = Vec::new();
        for arg in sub.get_arguments().filter(|a| !a.is_hide_set() && !is_global_arg(a)) {
            if arg.is_positional() {
                if arg.is_required_set() {
                    opts.push(format!("<{}>", arg.get_id()));
                } else {
                    opts.push(format!("[{}]", arg.get_id()));
                }
            } else if let Some(long) = arg.get_long() {
                if arg.get_action().takes_values() {
                    opts.push(format!("[--{long} {val}]", val = arg.get_id().as_str().to_uppercase()));
                } else {
                    opts.push(format!("[--{long}]"));
                }
            }
        }
        if !opts.is_empty() {
            writeln!(buf, "{}", opts.join(" ")).unwrap();
        }
        buf.write_all(b".br\n").unwrap();
    }
}

fn write_consolidated_commands(buf: &mut Vec<u8>, cmd: &clap::Command, parent_name: &str) {
    buf.write_all(b".SH COMMANDS\n").unwrap();
    write_commands_recursive(buf, cmd, parent_name, "");
}

fn write_commands_recursive(
    buf: &mut Vec<u8>,
    cmd: &clap::Command,
    parent_name: &str,
    path: &str,
) {
    for sub in cmd.get_subcommands() {
        if sub.is_hide_set() || sub.get_name() == "help" {
            continue;
        }
        let sub_key = format!("{parent_name}-{}", sub.get_name());
        let display_path = if path.is_empty() {
            sub.get_name().to_string()
        } else {
            format!("{path} {}", sub.get_name())
        };

        let is_group = sub.get_subcommands().next().is_some();

        writeln!(buf, ".SS {display_path}").unwrap();

        // Render a description for this command/group. Prefer the
        // long_about_for override; fall back to the clap short about.
        if let Some(long) = long_about_for(&sub_key) {
            for line in long.lines() {
                if line.trim().is_empty() {
                    buf.write_all(b".PP\n").unwrap();
                } else {
                    writeln!(buf, "{line}").unwrap();
                }
            }
        } else if let Some(about) = sub.get_about() {
            writeln!(buf, "{about}").unwrap();
        }

        // For leaves, emit the argument list. Groups don't have their
        // own args (their leaves do), so skip.
        if !is_group {
            let args: Vec<_> = sub
                .get_arguments()
                .filter(|a| !a.is_hide_set() && !is_global_arg(a))
                .collect();
            if !args.is_empty() {
                buf.write_all(b".PP\n").unwrap();
                for arg in args {
                    write_arg_tp(buf, arg);
                }
            }
        } else {
            write_commands_recursive(buf, sub, &sub_key, &display_path);
        }
    }
}

fn is_global_arg(arg: &clap::Arg) -> bool {
    matches!(
        arg.get_id().as_str(),
        "config"
            | "no_default_config"
            | "public"
            | "no_public"
            | "bootstrap"
            | "verbose"
            | "help"
    )
}

fn write_arg_tp(buf: &mut Vec<u8>, arg: &clap::Arg) {
    buf.write_all(b".TP\n").unwrap();
    if arg.is_positional() {
        let name = arg.get_id().as_str().to_uppercase();
        if arg.is_required_set() {
            writeln!(buf, "\\fB<{name}>\\fR").unwrap();
        } else {
            writeln!(buf, "\\fB[{name}]\\fR").unwrap();
        }
    } else {
        let mut label = String::new();
        if let Some(short) = arg.get_short() {
            label.push_str(&format!("\\fB-{short}\\fR"));
            if arg.get_long().is_some() {
                label.push_str(", ");
            }
        }
        if let Some(long) = arg.get_long() {
            label.push_str(&format!("\\fB--{long}\\fR"));
        }
        if arg.get_action().takes_values() {
            let val = arg
                .get_value_names()
                .and_then(|v| v.first().map(|s| s.to_string()))
                .unwrap_or_else(|| arg.get_id().as_str().to_uppercase());
            label.push_str(&format!(" \\fI{val}\\fR"));
        }
        writeln!(buf, "{label}").unwrap();
    }
    if let Some(help) = arg.get_help() {
        writeln!(buf, "{help}").unwrap();
    }
}

fn write_examples_section(buf: &mut Vec<u8>, examples: &[(&str, &str)]) {
    buf.write_all(b".SH EXAMPLES\n").unwrap();
    for (cmd, desc) in examples {
        buf.write_all(b".PP\n").unwrap();
        writeln!(buf, "{desc}").unwrap();
        buf.write_all(b".RS 4\n.nf\n").unwrap();
        writeln!(buf, "\\fB{cmd}\\fR").unwrap();
        buf.write_all(b".fi\n.RE\n").unwrap();
    }
}

fn write_see_also_section(buf: &mut Vec<u8>, refs: &[&str]) {
    buf.write_all(b".SH SEE ALSO\n").unwrap();
    let formatted: Vec<String> = refs
        .iter()
        .map(|r| format!("\\fB{r}\\fR(1)"))
        .collect();
    writeln!(buf, "{}", formatted.join(",\n")).unwrap();
}

fn write_section(buf: &mut Vec<u8>, heading: &str, body: &str) {
    write!(buf, ".SH {heading}\n{body}\n").unwrap();
}

fn apply_long_about(cmd: clap::Command, name: &str) -> clap::Command {
    match long_about_for(name) {
        Some(text) => cmd.long_about(text),
        None => cmd,
    }
}

fn long_about_for(name: &str) -> Option<&'static str> {
    match name {
        "peeroxide" => Some(
            "peeroxide is a command-line interface for the peeroxide P2P networking stack. \
             It provides tools for DHT-based peer discovery, direct connectivity testing, \
             file transfer, and anonymous messaging over the Hyperswarm-compatible network.\n\n\
             The tool connects to the public Hyperswarm DHT by default, or to custom \
             bootstrap nodes specified via --bootstrap flags or the configuration file. \
             All subcommands share a common set of global options for network configuration.\n\n\
             Use --public to include the public HyperDHT bootstrap nodes, or --no-public \
             to exclude them. If no bootstrap nodes are configured and --no-public is not \
             given, the public bootstrap is used automatically.",
        ),
        "peeroxide-node" => Some(
            "Run a long-lived DHT coordination (bootstrap) node that participates in the \
             distributed hash table routing layer. Bootstrap nodes help new peers discover \
             the network and facilitate Kademlia routing table population.\n\n\
             A node listens for incoming DHT RPC requests and maintains routing state. \
             Use --public to include the public HyperDHT bootstrap nodes (required for \
             production bootstrap nodes to join the network). The --port flag binds to a \
             specific UDP port for consistent addressing.\n\n\
             The node runs until terminated by SIGTERM or SIGINT.",
        ),
        "peeroxide-lookup" => Some(
            "Query the DHT for peers that have announced a given topic. Topics can be \
             specified as a human-readable string (hashed with BLAKE2b to produce a \
             discovery key) or as a 64-character hex-encoded raw topic.\n\n\
             Results are printed as they arrive from the DHT. Each result includes the \
             peer's public key and relay addresses (if hole-punching assistance is needed). \
             Use --json for machine-parseable NDJSON output. Use --with-data to include \
             any opaque data blobs peers attached during their announcement.\n\n\
             The lookup runs until the DHT query completes or SIGINT is received.",
        ),
        "peeroxide-announce" => Some(
            "Announce this peer's presence on a topic in the DHT so other peers can \
             discover it via lookup. The announcement is periodically refreshed to remain \
             active in the DHT.\n\n\
             Topics can be specified as a human-readable string (hashed with BLAKE2b to \
             produce a discovery key) or as a 64-character hex-encoded raw topic.\n\n\
             With --ping, the peer also listens for incoming encrypted connections and \
             responds to echo (PING/PONG) probes, useful for verifying end-to-end \
             connectivity. With --data, an opaque blob (up to 64 bytes) is attached to \
             the announcement and returned in lookup results.\n\n\
             The announcement continues until the specified --duration elapses, or until \
             terminated by SIGTERM or SIGINT.",
        ),
        "peeroxide-ping" => Some(
            "Diagnose reachability of a DHT node or peer. Supports three target forms, \
             plus a no-target bootstrap check mode:\n\n\
             (no target) — Ping all configured bootstrap nodes and report your public \
             address, NAT type, and the number of DHT peers each bootstrap knows about.\n\n\
             host:port — Send a DHT-level UDP ping to the given address.\n\n\
             @<pubkey> — Look up the peer by public key in the DHT, then attempt a \
             Noise-encrypted connection and PING/PONG echo exchange.\n\n\
             <topic> — Look up the topic in the DHT, then ping all discovered peers.\n\n\
             In bootstrap check mode (no target), the resolved bootstrap list comes from \
             the config file, --bootstrap flags, or public defaults (with --public or by \
             default when no other bootstrap is configured). The \
             output includes per-node reachability and routing table size, your reflexive \
             public address, a NAT type classification (open, consistent, random, or \
             multi-homed), and the total unique peers discovered across all bootstraps.\n\n\
             For address pings, RTT is measured from the UDP request/response. For peer \
             pings, the full connection setup (DHT lookup + Noise handshake + echo) is timed.\n\n\
             Use --count to send multiple probes with summary statistics. Use --connect with \
             address targets to perform a full encrypted connection test instead of just UDP. \
             Use --json for machine-parseable NDJSON output.\n\n\
             Exits 0 if all probes succeed, 1 if any fail, 130 on SIGINT.",
        ),
        "peeroxide-cp" => Some(
            "Copy files between peers over the Hyperswarm network using encrypted \
             peer-to-peer connections. The sender and receiver rendezvous on a shared \
             topic string.\n\n\
             The transfer uses a streaming protocol with 64KB chunks and includes \
             metadata (filename, size) exchanged before the payload. Files are written \
             atomically using a temporary file that is renamed on completion.",
        ),
        "peeroxide-cp-send" => Some(
            "Send a file to a peer over the Hyperswarm network. The sender announces on \
             the specified topic and waits for a receiver to connect. Once connected, the \
             file metadata (name, size) is sent as a JSON header followed by the file \
             contents in 64KB streaming chunks.\n\n\
             The file path may be '-' to read from stdin (requires specifying a filename \
             in the topic or metadata). Use --keep-alive to remain available for additional \
             receivers after the first transfer completes.\n\n\
             Prints the resolved topic to stdout so it can be communicated to the receiver.",
        ),
        "peeroxide-cp-recv" => Some(
            "Receive a file from a peer over the Hyperswarm network. The receiver looks \
             up the specified topic in the DHT, connects to the sender, and downloads the \
             file. The output path specifies where to write the received file.\n\n\
             If the output path is a directory, the filename from the sender's metadata is \
             used. If the file already exists, the transfer is aborted unless --force is \
             specified. Use --yes to skip all confirmation prompts.\n\n\
             The file is written atomically: data goes to a temporary file first, which is \
             renamed to the final path only after the full transfer succeeds and the size \
             is validated.",
        ),
        "peeroxide-dd" => Some(
            "Dead Drop: anonymous store-and-forward messaging via the DHT. Two wire protocols \
             ship in this binary, distinguished by their leading version byte.\n\n\
             Version 1 (0x01) is the original single-chain format: chunks form a \
             linked list of mutable DHT records (~1 KB each), each pointing to the next. \
             Simple, capped near 60 MB of payload, suitable for short messages. Still used \
             when the sender passes --v1 on dd put.\n\n\
             Version 2 (0x02) is a tree-indexed protocol: data chunks are stored \
             content-addressed via immutable_put, and a tree of mutable index records \
             names them. The receiver fetches the index tree breadth-first in parallel \
             and reconstructs the file in DFS order. Default protocol for dd put. \
             The soft depth cap of 4 supports up to about 27 GB at the current 998-byte \
             chunk size; depth 5+ would extend that further but is rejected at PUT time \
             to keep tree-walk latency bounded.\n\n\
             dd get detects the protocol from the first byte of the root record \
             and runs the matching v1 or v2 fetch path automatically; there is no --v1 \
             flag on the get side.\n\n\
             Both protocols periodically refresh their records to keep them alive in the \
             DHT (records age out of node storage after about 20 minutes by default). \
             A passphrase-derived keypair can be used so both sender and receiver agree \
             on the pickup key without exchanging it directly.",
        ),
        "peeroxide-dd-put" => Some(
            "Store an anonymous message at a dead drop location in the DHT. The message \
             is encrypted with a passphrase-derived keypair and stored as a mutable DHT \
             record.\n\n\
             The passphrase can be provided inline with --passphrase or prompted \
             interactively (hidden input) with --interactive-passphrase. The keypair \
             is deterministically derived from the passphrase, so both sender and \
             receiver must use the same passphrase to exchange messages.\n\n\
             The positional argument is a file path to read, or '-' to read from stdin. \
             The DHT record is stored with acknowledgement confirmation from routing table \
             peers. Records persist in the DHT as long as nodes cache them (typically hours \
             to days depending on network conditions).",
        ),
        "peeroxide-dd-get" => Some(
            "Retrieve a message from a dead drop location in the DHT. The pickup key \
             can be a 64-character hex public key, a passphrase string (if less than 64 \
             hex chars), or derived interactively.\n\n\
             Use --passphrase to supply the passphrase inline, or --interactive-passphrase \
             to be prompted with hidden input. If a positional key argument is given that \
             is not valid hex, it is treated as a passphrase.\n\n\
             The retrieved message is written to stdout (or to a file with --output). If \
             no message is found at the specified location, or if decryption fails (wrong \
             passphrase), an error is reported.\n\n\
             The get operation is read-only and does not modify or consume the stored \
             record -- the same message can be retrieved multiple times by different peers.",
        ),

        "peeroxide-init" => Some(
            "Initialize a peeroxide config file or install man pages. This command has \
             two mutually exclusive modes:\n\n\
             Config mode (default): Creates a commented TOML config file with sane defaults \
             at ~/.config/peeroxide/config.toml (or the path given by --config). Use --force \
             to overwrite an existing config, or --update to patch specific fields without \
             disturbing other settings.\n\n\
             Man page mode (--man-pages): Generates and installs roff man pages into the \
             specified directory (default: /usr/local/share/man/man1/). No config is touched \
             in this mode.",
        ),
        "peeroxide-chat" => Some(
            "End-to-end-encrypted peer-to-peer chat over the Hyperswarm DHT. No central \
             server, no account signup, no message storage beyond the ephemeral DHT.\n\n\
             Identity is a local Ed25519 keypair stored per profile under \
             ~/.config/peeroxide/chat/profiles/. A separate process-wide \
             ~/.config/peeroxide/chat/known_users cache records the most-recent \
             screen name observed for each pubkey, shared across all profiles on the \
             machine. Set PEEROXIDE_HOME to use another directory in place of ~ \
             for both.\n\n\
             Two conversation shapes are supported. Channels are public or private \
             group rooms keyed by a channel name (plus an optional group salt for \
             privacy). Direct messages are 1:1 between two identity public keys; the \
             DM channel key is derived deterministically from the pair, so both sides \
             arrive at the same key without prior coordination.\n\n\
             Discovery uses the DHT announce/lookup rendezvous pattern across rotating \
             epoch+bucket topics, so an observer cannot trivially correlate one feed \
             across long time windows. Message records are encrypted with \
             XSalsa20-Poly1305 and signed with the author's Ed25519 key; readers verify \
             both the chain-of-prev-hashes and the per-message signature before \
             releasing a message to the UI.\n\n\
             The TUI auto-activates when both stdin and stdout are terminals; otherwise \
             chat runs in line mode (one message per line on stdout). Force line mode \
             with --line-mode or PEEROXIDE_LINE_MODE=1.",
        ),
        "peeroxide-chat-join" => Some(
            "Join a channel. Interactive TUI by default on a terminal; line mode \
             otherwise (one message per line on stdout).\n\n\
             Channel name is positional. Pass \\fB--group <SALT>\\fR (or read the salt \
             from a file with \\fB--keyfile <PATH>\\fR) to join a private channel \
             whose discovery topic is derived from both the channel name and the \
             salt -- two people who don't share the salt cannot find each other or \
             decrypt each other's messages.\n\n\
             By default the session also publishes the local profile's Nexus record \
             and refreshes friend Nexus data in the background; suppress with \
             \\fB--no-nexus\\fR / \\fB--no-friends\\fR, or use \\fB--stealth\\fR for both \
             plus \\fB--read-only\\fR.\n\n\
             Stdin EOF exits the session by default. Pass \\fB--stay-after-eof\\fR to \
             keep the session listening after the input stream closes -- useful when \
             piping a transcript and then watching for replies.",
        ),
        "peeroxide-chat-dm" => Some(
            "Open a direct-message session with another identity. Interactive TUI by \
             default; line mode otherwise.\n\n\
             The recipient is resolved in this order: a 64-char hex public key, \
             \\fB@SHORTKEY\\fR (the first 8 hex characters of a pubkey), \
             \\fBNAME@SHORTKEY\\fR (validates the screen name against the known_users \
             cache), a bare 8-char shortkey, a friend alias from the current profile, \
             or a screen name from the shared known_users cache. The DM channel key \
             is derived deterministically from your identity pubkey and theirs, so \
             you both arrive at the same key without coordination.\n\n\
             Pass \\fB--message <TEXT>\\fR to seed an initial inbox-invite lure for the \
             recipient -- their inbox monitor surfaces a notification with this text \
             so they know who is reaching out and on what topic.\n\n\
             Other session flags mirror \\fBchat join\\fR (\\fB--no-nexus\\fR, \
             \\fB--no-friends\\fR, \\fB--read-only\\fR, \\fB--stealth\\fR, \
             \\fB--feed-lifetime\\fR, \\fB--batch-size\\fR, \\fB--batch-wait-ms\\fR, \
             \\fB--stay-after-eof\\fR, \\fB--no-inbox\\fR, \\fB--inbox-poll-interval\\fR). \
             \\fB--group\\fR / \\fB--keyfile\\fR do NOT apply to DMs (the channel key \
             is derived from the participants).",
        ),
        "peeroxide-chat-inbox" => Some(
            "Monitor the local profile's inbox for new invites (DMs from new senders \
             and private-channel invites). Prints each new invite to stdout as it \
             arrives; does NOT enter the interactive chat -- use \\fBchat dm\\fR or \
             \\fBchat join\\fR to act on an invite.\n\n\
             Each poll scans the current and previous inbox epochs across all 4 \
             buckets in parallel (8 DHT lookups). \\fB--poll-interval\\fR sets the \
             cycle length in seconds; values below 1 are clamped to 1.\n\n\
             \\fB--no-nexus\\fR and \\fB--no-friends\\fR are accepted for flag-surface \
             parity with \\fBchat join\\fR / \\fBchat dm\\fR but have no effect here -- \
             the inbox CLI does not run nexus publish or friend refresh tasks.",
        ),
        "peeroxide-chat-whoami" => Some(
            "Print the current profile's identity: profile name, full 64-char identity \
             public key, screen name (if set), and the topic hash other peers would \
             use to discover this identity's Nexus record.",
        ),
        "peeroxide-chat-profiles" => Some(
            "Manage local identity profiles. Each profile is a directory under \
             \\fB~/.config/peeroxide/chat/profiles/<NAME>/\\fR containing the Ed25519 \
             seed, an optional screen name and bio, and a friend list. The \\fBdefault\\fR \
             profile is auto-created on first run and cannot be deleted.",
        ),
        "peeroxide-chat-profiles-list" => Some(
            "List all locally-known profile names.",
        ),
        "peeroxide-chat-profiles-create" => Some(
            "Create a new profile with a freshly generated Ed25519 keypair. If \
             \\fB--screen-name\\fR is omitted, a vendor name is generated deterministically \
             from the public key and stored in the profile.",
        ),
        "peeroxide-chat-profiles-delete" => Some(
            "Delete a profile and all of its local state (seed, screen name, bio, \
             friend list). The \\fBdefault\\fR profile is rejected.",
        ),
        "peeroxide-chat-friends" => Some(
            "Manage the current profile's friend list. Friends are saved by identity \
             public key with an optional local alias and the last-seen screen name / \
             bio fetched from their Nexus. Friend Nexus data is refreshed periodically \
             during chat sessions.",
        ),
        "peeroxide-chat-friends-list" => Some(
            "List the friends recorded under the current profile, with their aliases, \
             screen names, and shortened public keys.",
        ),
        "peeroxide-chat-friends-add" => Some(
            "Add a friend to the current profile. The key argument follows the same \
             resolution rules as \\fBchat dm\\fR's recipient. If \\fB--alias\\fR is omitted, \
             the alias is auto-filled from the known_users cache (or a generated vendor \
             name if no cached screen name is available).",
        ),
        "peeroxide-chat-friends-remove" => Some(
            "Remove a friend from the current profile's friend list. The key argument \
             follows the same resolution rules as \\fBchat friends add\\fR.",
        ),
        "peeroxide-chat-friends-refresh" => Some(
            "Perform a one-shot DHT refresh of the friend Nexus records for the \
             \\fBdefault\\fR profile. Does not accept \\fB--profile\\fR.",
        ),
        "peeroxide-chat-nexus" => Some(
            "Manage the current profile's Nexus record (a public-key-addressed mutable \
             DHT record carrying your screen name and bio).\n\n\
             By default \\fBchat nexus\\fR performs a one-shot publish of the current \
             profile's Nexus. With \\fB--set-name\\fR or \\fB--set-bio\\fR (or both), \
             the new values are written to the profile first; if neither \\fB--publish\\fR \
             nor \\fB--daemon\\fR is supplied with the setters, the command exits after \
             writing without publishing. \\fB--publish\\fR forces a one-shot publish. \
             \\fB--daemon\\fR runs continuously, publishing your own Nexus every 480 \
             seconds and refreshing all friend Nexus records every 600 seconds.\n\n\
             \\fB--lookup <PUBKEY>\\fR short-circuits all other modes: fetch and print \
             the named identity's Nexus record (screen name + bio).",
        ),
        _ => None,
    }
}

fn examples_for(name: &str) -> Option<&'static [(&'static str, &'static str)]> {
    match name {
        "peeroxide" => Some(&[
            (
                "peeroxide --public ping",
                "Verify bootstrap connectivity and discover your public address:",
            ),
            (
                "peeroxide ping 1.2.3.4:49737",
                "Ping a known DHT bootstrap node:",
            ),
            (
                "peeroxide lookup my-service --json",
                "Look up peers on a topic with JSON output:",
            ),
            (
                "peeroxide announce my-service --ping",
                "Announce on a topic and respond to echo probes:",
            ),
            (
                "peeroxide cp send ./file.tar.gz my-transfer",
                "Send a file to a peer:",
            ),
            (
                "peeroxide node --public --port 49737",
                "Run a public bootstrap node:",
            ),
        ]),
        "peeroxide-node" => Some(&[
            (
                "peeroxide node",
                "Run a bootstrap node with default settings:",
            ),
            (
                "peeroxide node --port 49737 --public",
                "Run a public bootstrap node on the standard port:",
            ),
            (
                "peeroxide node --bootstrap other-node.example.com:49737",
                "Run a node bootstrapping from an existing peer:",
            ),
        ]),
        "peeroxide-lookup" => Some(&[
            (
                "peeroxide lookup my-service",
                "Look up peers announcing a human-readable topic:",
            ),
            (
                "peeroxide lookup a1b2c3...64chars --json",
                "Look up a hex topic with JSON output:",
            ),
            (
                "peeroxide lookup chat-room --with-data",
                "Include announcement data in results:",
            ),
        ]),
        "peeroxide-announce" => Some(&[
            (
                "peeroxide announce my-service",
                "Announce on a topic indefinitely:",
            ),
            (
                "peeroxide announce my-service --ping",
                "Announce and respond to connectivity probes:",
            ),
            (
                "peeroxide announce my-service --duration 3600 --data hello",
                "Announce for one hour with attached data:",
            ),
            (
                "peeroxide announce my-service --seed 0a1b2c...64chars",
                "Announce with a specific keypair seed:",
            ),
        ]),
        "peeroxide-ping" => Some(&[
            (
                "peeroxide --public ping",
                "Check bootstrap connectivity and discover your public address:",
            ),
            (
                "peeroxide ping 1.2.3.4:49737",
                "Send a UDP DHT ping to an address:",
            ),
            (
                "peeroxide ping @a1b2c3...64chars",
                "Ping a peer by public key (Noise handshake + echo):",
            ),
            (
                "peeroxide ping @a1b2c3...64chars --count 5 --interval 2",
                "Send 5 probes at 2-second intervals:",
            ),
            (
                "peeroxide ping 1.2.3.4:49737 --connect",
                "Full encrypted connection test to an address:",
            ),
            (
                "peeroxide ping my-service --json",
                "Ping all peers on a topic with JSON output:",
            ),
        ]),
        "peeroxide-cp" => Some(&[
            (
                "peeroxide cp send ./report.pdf my-transfer",
                "Send a file, printing the topic for the receiver:",
            ),
            (
                "cat data.bin | peeroxide cp send - my-transfer",
                "Stream from stdin:",
            ),
            (
                "peeroxide cp send ./file.txt my-transfer --keep-alive",
                "Send to multiple receivers (stays alive after first):",
            ),
            (
                "peeroxide cp recv my-transfer ./downloads/",
                "Receive into a directory (uses sender's filename):",
            ),
            (
                "peeroxide cp recv my-transfer ./output.bin --force",
                "Receive and overwrite an existing file:",
            ),
            (
                "peeroxide cp recv my-transfer - > file.bin",
                "Receive to stdout:",
            ),
        ]),
        "peeroxide-dd" => Some(&[
            (
                "echo 'secret message' | peeroxide dd put - --passphrase s3cret",
                "Put a v2 message at a dead drop with an inline passphrase (read from stdin):",
            ),
            (
                "peeroxide dd put ./msg.txt --interactive-passphrase",
                "Put a file at a dead drop with a prompted passphrase (hidden input):",
            ),
            (
                "peeroxide dd put ./large.tar --passphrase s3cret --v1",
                "Force the legacy v1 single-chain protocol on put:",
            ),
            (
                "peeroxide dd put ./file.bin --passphrase s3cret --no-progress",
                "Suppress the progress bar (useful in scripts or when stderr is not a TTY):",
            ),
            (
                "peeroxide dd put ./file.bin --passphrase s3cret --json",
                "Emit JSON-Lines progress events on stdout (suitable for scripting):",
            ),
            (
                "peeroxide dd get --passphrase s3cret",
                "Get a message from a dead drop using the same passphrase (auto-detects v1/v2):",
            ),
            (
                "peeroxide dd get --interactive-passphrase --output ./msg.txt",
                "Get with prompted passphrase, write to file:",
            ),
            (
                "peeroxide dd get a1b2c3...64chars --output ./out.bin --json",
                "Get using a raw hex public key and emit JSON progress (requires --output):",
            ),
        ]),

        "peeroxide-chat" => Some(&[
            (
                "peeroxide chat join general",
                "Join a public channel named \"general\":",
            ),
            (
                "peeroxide chat join dev-room --group s3cret-salt",
                "Join a private channel (only peers with the same salt can find each other):",
            ),
            (
                "peeroxide chat dm @a1b2c3d4 --message 'hey, got a minute?'",
                "Open a DM to a peer by 8-char shortkey, leaving an inbox lure:",
            ),
            (
                "peeroxide chat inbox --poll-interval 30",
                "Watch the local inbox for new invites, polling every 30 seconds:",
            ),
            (
                "peeroxide chat nexus --set-name 'Alice' --set-bio 'building stuff'",
                "Update your screen name and bio in your profile (no DHT publish):",
            ),
            (
                "peeroxide chat nexus --set-name 'Alice' --publish",
                "Update your screen name and immediately publish to the DHT:",
            ),
            (
                "peeroxide chat nexus --daemon",
                "Run a background Nexus refresher (publish self every 480s, refresh friends every 600s):",
            ),
            (
                "peeroxide chat profiles create work --screen-name 'Alice (work)'",
                "Create a second profile with its own identity keypair:",
            ),
            (
                "peeroxide chat friends add @a1b2c3d4 --alias bob",
                "Add a friend to the current profile under a local alias:",
            ),
        ]),

        "peeroxide-init" => Some(&[
            (
                "peeroxide init",
                "Create a default config file at ~/.config/peeroxide/config.toml:",
            ),
            (
                "peeroxide init --public --bootstrap node1.example.com:49737",
                "Create a config with public mode and custom bootstrap:",
            ),
            (
                "peeroxide init --force",
                "Overwrite an existing config file:",
            ),
            (
                "peeroxide init --update --public",
                "Enable public mode in an existing config without changing other settings:",
            ),
            (
                "peeroxide init --man-pages",
                "Install man pages to /usr/local/share/man/man1/:",
            ),
            (
                "peeroxide init --man-pages ~/.local/share/man/",
                "Install man pages to a custom directory:",
            ),
        ]),
        _ => None,
    }
}

fn exit_status_for(name: &str) -> Option<&'static str> {
    match name {
        "peeroxide" | "peeroxide-init" | "peeroxide-node" | "peeroxide-lookup"
        | "peeroxide-announce" | "peeroxide-cp" | "peeroxide-dd" | "peeroxide-chat" => Some(
            ".TP\n\\fB0\\fR\nSuccess.\n\
             .TP\n\\fB1\\fR\nFailure or partial failure.\n\
             .TP\n\\fB2\\fR\nUsage error (invalid arguments).\n\
             .TP\n\\fB130\\fR\nInterrupted by SIGINT.",
        ),
        "peeroxide-ping" => Some(
            ".TP\n\\fB0\\fR\nAll probes succeeded.\n\
             .TP\n\\fB1\\fR\nOne or more probes failed.\n\
             .TP\n\\fB2\\fR\nUsage error (invalid arguments).\n\
             .TP\n\\fB130\\fR\nInterrupted by SIGINT.",
        ),
        _ => None,
    }
}

fn see_also_for(name: &str) -> Option<&'static [&'static str]> {
    match name {
        "peeroxide" => Some(&[
            "peeroxide-init",
            "peeroxide-node",
            "peeroxide-lookup",
            "peeroxide-announce",
            "peeroxide-ping",
            "peeroxide-cp",
            "peeroxide-dd",
            "peeroxide-chat",
        ]),
        "peeroxide-init" => Some(&["peeroxide"]),
        "peeroxide-node" => Some(&["peeroxide"]),
        "peeroxide-lookup" => Some(&["peeroxide-announce", "peeroxide"]),
        "peeroxide-announce" => Some(&["peeroxide-lookup", "peeroxide-ping", "peeroxide"]),
        "peeroxide-ping" => Some(&[
            "peeroxide-node",
            "peeroxide-announce",
            "peeroxide-lookup",
            "peeroxide",
        ]),
        "peeroxide-cp" => Some(&["peeroxide-dd", "peeroxide"]),

        "peeroxide-dd" => Some(&["peeroxide-cp", "peeroxide"]),
        "peeroxide-chat" => Some(&["peeroxide", "peeroxide-init"]),
        _ => None,
    }
}
