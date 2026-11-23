mod cli;
mod commands;

fn main() {
    std::process::exit(cli::run());
}
