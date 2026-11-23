use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "herdcluster", version, about = "Distributed coding-agent cluster on top of herdr")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize this machine as the cluster controller
    Init {
        /// Cluster display name (default: hostname)
        #[arg(long)]
        name: Option<String>,
        /// Skip orchestration skill provisioning offer
        #[arg(long)]
        no_skills: bool,
    },
    /// Pair this machine as a worker with a discovered controller
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
    /// Run as a worker daemon (used internally after join)
    Worker,
    /// Diagnose cluster health; --fix attempts safe remediations
    Doctor {
        #[arg(long)]
        fix: bool,
        #[arg(long)]
        machine: Option<String>,
        #[arg(long)]
        json: bool,
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
        #[arg(long)]
        repo: String,
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
enum AgentCommands {
    /// List agents (use --all for every machine)
    List {
        #[arg(long)]
        all: bool,
        #[arg(long)]
        json: bool,
    },
    /// Show agent details
    Get { id: String, #[arg(long)] json: bool },
    /// Read agent terminal output
    Read { id: String, #[arg(long)] json: bool },
    /// Wait for an agent state
    Wait { id: String, #[arg(long)] json: bool },
    /// Send a prompt to an agent
    Prompt { id: String, text: String },
    /// Send keys to an agent
    SendKeys { id: String, keys: String },
    /// Rename an agent
    Rename { id: String, name: String },
}

#[derive(Subcommand)]
enum SkillsCommands {
    /// Install the orchestration skill into the current project
    Install,
    /// Show skill installation status
    Status,
}

pub fn run() -> i32 {
    let cli = Cli::parse();
    match cli.command {
        Commands::Init { .. } => not_implemented("init"),
        Commands::Join { .. } => not_implemented("join"),
        Commands::Worker => not_implemented("worker"),
        Commands::Doctor { .. } => not_implemented("doctor"),
        Commands::Machines { .. } => not_implemented("machines"),
        Commands::Spawn { .. } => not_implemented("spawn"),
        Commands::Agent { .. } => not_implemented("agent"),
        Commands::Status { .. } => not_implemented("status"),
        Commands::Skills { .. } => not_implemented("skills"),
    }
}

fn not_implemented(cmd: &str) -> i32 {
    eprintln!("herdcluster {cmd}: not implemented yet (see docs/herdcluster/PLAN.md)");
    2
}
