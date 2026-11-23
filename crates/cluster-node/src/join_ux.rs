//! `join_ux` — small interactive prompt helpers shared by worker
//! pairing, `doctor --fix`, and skill provisioning (bead `m3-join-ux`).
//!
//! These are deliberately minimal stdin readers:
//!
//! - Non-TTY stdin never blocks: [`confirm`] returns its declared
//!   `default_yes`, [`prompt_line`] returns `None`. Callers that need
//!   unattended behavior should pass their own `--yes` flag *before*
//!   reaching these helpers; the default here is a safety floor, not a
//!   substitute for explicit consent.
//! - Prompts print to **stdout** (they are part of the command's
//!   normal output, like `worker.rs`'s pairing prompts) and flush
//!   before reading.
//!
//! `worker.rs` keeps its own inline prompts (pairing-code entry,
//! candidate picking) — refactoring it was explicitly out of scope;
//! these helpers serve `doctor --fix` and the fleet/skills commands.

use std::io::{IsTerminal, Write};

/// Ask `prompt` for a y/n answer.
///
/// - Interactive stdin: reads one line; empty input, EOF, or
///   unrecognized text re-prompts until a definitive `y/yes` or `n/no`
///   (bounded by stdin EOF, which falls back to `default_yes`).
/// - Non-TTY stdin: returns `default_yes` immediately without reading.
pub fn confirm(prompt: &str, default_yes: bool) -> bool {
    use std::io::BufRead;

    if !std::io::stdin().is_terminal() {
        // Non-interactive: honor the caller-declared default rather
        // than blocking on a stdin that will never answer.
        return default_yes;
    }

    let stdin = std::io::stdin();
    let mut lock = stdin.lock();
    loop {
        print!("{prompt} ");
        let _ = std::io::stdout().flush();
        let mut line = String::new();
        match lock.read_line(&mut line) {
            Ok(0) | Err(_) => return default_yes, // EOF / read error
            Ok(_) => {}
        }
        match line.trim().to_ascii_lowercase().as_str() {
            "" => return default_yes,
            "y" | "yes" => return true,
            "n" | "no" => return false,
            _ => {
                println!("please answer 'y' or 'n'");
            }
        }
    }
}

/// Ask `prompt` for a free-form line.
///
/// Returns the trimmed line, or `None` on empty input, EOF, read
/// error, or non-TTY stdin.
pub fn prompt_line(prompt: &str) -> Option<String> {
    use std::io::BufRead;

    if !std::io::stdin().is_terminal() {
        return None;
    }

    print!("{prompt} ");
    let _ = std::io::stdout().flush();

    let stdin = std::io::stdin();
    let mut lock = stdin.lock();
    let mut line = String::new();
    match lock.read_line(&mut line) {
        Ok(0) | Err(_) => None,
        Ok(_) => {
            let trimmed = line.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // Prompt helpers are interactive-only; the non-TTY contract is the
    // only piece unit-testable without a pty. In test harnesses stdin is
    // typically not a terminal, so confirm() must return the default and
    // prompt_line() must return None — assert exactly that contract.
    #[test]
    fn non_tty_confirm_returns_default() {
        use std::io::IsTerminal;
        if std::io::stdin().is_terminal() {
            // Running under a pty — skip; interactive behavior is
            // covered by manual/e2e flows.
            return;
        }
        assert!(crate::join_ux::confirm("ok? [y/N]", true));
        assert!(!crate::join_ux::confirm("ok? [y/N]", false));
    }

    #[test]
    fn non_tty_prompt_line_returns_none() {
        use std::io::IsTerminal;
        if std::io::stdin().is_terminal() {
            return;
        }
        assert!(crate::join_ux::prompt_line("value: ").is_none());
    }
}
