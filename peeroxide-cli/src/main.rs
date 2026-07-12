#![deny(clippy::all)]

use clap::{CommandFactory, FromArgMatches, Parser, Subcommand};
use tracing_subscriber::EnvFilter;

mod cmd;
mod config;
mod manpage;

// Shown by `peeroxide --version` (long form). `-V` keeps showing just
// the bare semver, which is what scripts expect. Clap automatically
// prefixes `--version` output with the binary name, so starting this
// const with the version number yields the standard `peeroxide X.Y.Z`
// header followed by the banner.
const LONG_VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    "\n\n",
    include_str!("ascii_art.txt"),
);

#[derive(Parser)]
#[command(name = "peeroxide", version, long_version = LONG_VERSION, about = "P2P networking CLI for the Hyperswarm-compatible network")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Use this config file instead of the default location
    #[arg(long, global = true)]
    config: Option<String>,

    /// Ignore default config file entirely
    #[arg(long, global = true)]
    no_default_config: bool,

    /// Use the public HyperDHT bootstrap network
    #[arg(long, global = true, conflicts_with = "no_public")]
    public: bool,

    /// Do not use the public HyperDHT bootstrap network
    #[arg(long, global = true, conflicts_with = "public")]
    no_public: bool,

    /// Bootstrap node addresses (host:port or ip:port), repeatable
    #[arg(long, global = true, action = clap::ArgAction::Append)]
    bootstrap: Vec<String>,

    /// Increase output verbosity (-v info, -vv debug)
    #[arg(short = 'v', long, global = true, action = clap::ArgAction::Count)]
    verbose: u8,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize config file or install man pages
    Init(cmd::init::InitArgs),
    /// Run a long-running DHT coordination (bootstrap) node
    Node(cmd::node::NodeArgs),
    /// Query the DHT for peers announcing a topic
    Lookup(cmd::lookup::LookupArgs),
    /// Announce presence on a topic
    Announce(cmd::announce::AnnounceArgs),
    /// Run a standalone blind-relay server
    Relay(cmd::relay::RelayArgs),
    /// Diagnose reachability of a DHT node or peer
    Ping(cmd::ping::PingArgs),
    /// Copy files between peers over the swarm
    Cp {
        #[command(subcommand)]
        command: cmd::cp::CpCommands,
    },
    /// Dead Drop: anonymous store-and-forward via the DHT
    #[command(name = "dd")]
    Dd {
        #[command(subcommand)]
        command: cmd::deaddrop::DdCommands,
    },
    /// Anonymous verifiable P2P chat
    Chat(cmd::chat::ChatArgs),
}

fn apply_config_footer(cmd: clap::Command, footer: &str) -> clap::Command {
    let sub_names: Vec<String> = cmd
        .get_subcommands()
        .map(|s| s.get_name().to_string())
        .collect();
    let mut cmd = cmd;
    for name in sub_names {
        let f = footer.to_string();
        cmd = cmd.mut_subcommand(name, |sub| apply_config_footer(sub, &f));
    }
    cmd.after_help(footer.to_string())
}

fn init_tracing(verbose: u8) {
    let filter = if std::env::var("RUST_LOG").is_ok() {
        EnvFilter::from_default_env()
    } else {
        match verbose {
            0 => EnvFilter::new("warn,peeroxide::_events=info"),
            1 => EnvFilter::new("peeroxide=info,peeroxide_dht=info,peeroxide::_events=info,warn"),
            _ => EnvFilter::new(
                "peeroxide=debug,peeroxide_dht=debug,libudx=debug,peeroxide::_events=info,info",
            ),
        }
    };
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
}

fn main() {
    let footer = config::config_path_footer();
    let cmd = apply_config_footer(Cli::command(), &footer);
    let mut help_cmd = cmd.clone();
    let matches = cmd.get_matches();
    let cli = Cli::from_arg_matches(&matches).unwrap_or_else(|e: clap::Error| e.exit());

    init_tracing(cli.verbose);

    let Some(command) = cli.command else {
        help_cmd.print_help().ok();
        eprintln!();
        std::process::exit(2);
    };

    let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
    let exit_code = rt.block_on(async {
        match command {
            Commands::Init(args) => {
                let ctx = cmd::init::InitContext {
                    config_path: cli.config,
                };
                cmd::init::run(args, ctx)
            }
            command => {
                let global = config::GlobalFlags {
                    config_path: cli.config,
                    no_default_config: cli.no_default_config,
                    public: if cli.public {
                        Some(true)
                    } else if cli.no_public {
                        Some(false)
                    } else {
                        None
                    },
                    bootstrap: if cli.bootstrap.is_empty() {
                        None
                    } else {
                        Some(cli.bootstrap)
                    },
                };

                let cfg = match config::load_config(&global) {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("error: failed to load config: {e}");
                        return 1;
                    }
                };

                match command {
                    Commands::Node(args) => cmd::node::run(args, &cfg).await,
                    Commands::Lookup(args) => cmd::lookup::run(args, &cfg).await,
                    Commands::Announce(args) => cmd::announce::run(args, &cfg).await,
                    Commands::Relay(args) => cmd::relay::run(args, &cfg).await,
                    Commands::Ping(args) => cmd::ping::run(args, &cfg).await,
                    Commands::Cp { command } => cmd::cp::run(command, &cfg).await,
                    Commands::Dd { command } => cmd::deaddrop::run(command, &cfg).await,
                    Commands::Chat(args) => cmd::chat::run(args, &cfg).await,
                    Commands::Init(_) => unreachable!(),
                }
            }
        }
    });
    std::process::exit(exit_code);
}
