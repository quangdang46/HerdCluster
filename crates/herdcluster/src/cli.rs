use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "herdcluster",
    version,
    about = "Distributed coding-agent cluster on top of herdr"
)]
struct Cli {
    /// Print verbose diagnostics to stderr
    #[arg(short, long, global = true)]
    verbose: bool,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
pub(crate) enum Commands {
    /// Initialize this machine as the cluster controller
    Init {
        /// Cluster display name (default: hostname)
        #[arg(long)]
        name: Option<String>,
        /// Skip orchestration skill provisioning offer
        #[arg(long)]
        no_skills: bool,
        /// TCP listen port (default: 47820; 0 = ephemeral)
        #[arg(long)]
        port: Option<u16>,
        /// Non-interactive: auto-accept pairing requests, auto-install skills
        #[arg(long)]
        yes: bool,
    },
    /// Pair this machine as a worker with a discovered controller,
    /// then run as the worker daemon
    Join {
        /// Controller hint (optional; auto-discovery by default)
        #[arg(long)]
        controller: Option<String>,
        /// Non-interactive mode
        #[arg(long)]
        yes: bool,
        /// JSON output
        #[arg(long)]
        json: bool,
    },
    /// Run as a worker daemon (requires a prior `herdcluster join`)
    Worker {
        /// Controller address override (`ip:port` or `host:port`)
        #[arg(long)]
        controller: Option<String>,
    },
    /// Diagnose cluster health; --fix attempts safe remediations
    Doctor {
        #[arg(long)]
        fix: bool,
        /// Confirm safe fixes without prompting
        #[arg(long)]
        yes: bool,
        #[arg(long)]
        machine: Option<String>,
        /// Repo path to verify exists (checked on the machine running doctor)
        #[arg(long)]
        repo: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Revoke a worker's pairing (removes registry entry + credential)
    Revoke {
        /// Worker label to revoke
        label: String,
        #[arg(long)]
        json: bool,
    },
    /// Manage cluster configuration (cluster.json)
    Config {
        #[command(subcommand)]
        command: ConfigCommands,
    },
    /// Manage worker autostart (launch at login/boot)
    Autostart {
        #[command(subcommand)]
        command: AutostartCommands,
    },
    /// List cluster machines and their status
    Machines {
        #[arg(long)]
        json: bool,
    },
    /// Spawn a coding agent on a machine
    Spawn {
        name: String,
        #[arg(long)]
        kind: String,
        #[arg(long)]
        machine: String,
        /// Repo path on the target machine ("~" = its home dir). Optional:
        /// omit to reuse the machine's only workspace, or create one in the
        /// server's default directory (home).
        #[arg(long)]
        repo: Option<String>,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long)]
        json: bool,
        #[arg(last = true)]
        agent_args: Vec<String>,
    },
    /// Agent operations across the cluster
    Agent {
        #[command(subcommand)]
        command: AgentCommands,
    },
    /// Cluster status overview
    Status {
        #[arg(long)]
        all: bool,
        #[arg(long)]
        json: bool,
    },
    /// Manage the bundled orchestration skill
    Skills {
        #[command(subcommand)]
        command: SkillsCommands,
    },
}

#[derive(Subcommand)]
pub(crate) enum AgentCommands {
    /// List agents (use --all for every machine)
    List {
        #[arg(long)]
        all: bool,
        #[arg(long)]
        json: bool,
    },
    /// Show agent details
    Get {
        id: String,
        #[arg(long)]
        json: bool,
    },
    /// Read agent terminal output
    Read {
        id: String,
        #[arg(long)]
        json: bool,
    },
    /// Wait for an agent state
    Wait {
        id: String,
        #[arg(long)]
        json: bool,
    },
    /// Send a prompt to an agent
    Prompt { id: String, text: String },
    /// Send keys to an agent
    SendKeys { id: String, keys: String },
    /// Rename an agent
    Rename { id: String, name: String },
}

#[derive(Subcommand)]
pub(crate) enum ConfigCommands {
    /// Show cluster configuration and machine overrides
    Get {
        #[arg(long)]
        json: bool,
    },
    /// Set a config value, e.g. `machines.<label>.max_agents 6`
    /// or `machines.<label>.kinds_override claude,codex`
    Set {
        /// Config key
        key: String,
        /// Value to set
        value: String,
        #[arg(long)]
        json: bool,
    },
    /// Remove a machines.<label> override entirely
    Unset {
        /// Config key (machines.<label>)
        key: String,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
pub(crate) enum AutostartCommands {
    /// Install the worker daemon as a login/boot autostart entry
    Install,
    /// Remove the autostart entry
    Uninstall,
    /// Show autostart status
    Status,
}

#[derive(Subcommand)]
pub(crate) enum SkillsCommands {
    /// Install the orchestration skill into the current project
    Install {
        /// Install without asking (also overwrites diverged installs)
        #[arg(long)]
        yes: bool,
        /// Overwrite a diverged local copy without asking
        #[arg(long)]
        overwrite: bool,
    },
    /// Show skill installation status
    Status,
}

pub fn run() -> i32 {
    let cli = Cli::parse();
    crate::commands::dispatch(cli.command, cli.verbose)
}
