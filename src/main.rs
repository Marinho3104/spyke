mod network;
mod utils;

use std::io::IsTerminal;

fn main() {
    // Built-in format: LEVEL file:line: message
    tracing_subscriber::fmt()
        .without_time()
        .with_target(false)
        .with_file(true)
        .with_line_number(true)
        .with_writer(std::io::stderr)
        // No colour codes when the output is redirected to a file or pipe
        .with_ansi(std::io::stderr().is_terminal())
        .init();
}
