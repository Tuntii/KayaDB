pub(crate) fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(future)
}

pub(crate) fn remove_flag(args: &mut Vec<String>, flag: &str) -> bool {
    let before = args.len();
    args.retain(|arg| arg != flag);
    args.len() != before
}

pub(crate) fn remove_value_flag(args: &mut Vec<String>, flag: &str) -> Option<String> {
    let pos = args.iter().position(|arg| arg == flag)?;
    if pos + 1 < args.len() {
        args.remove(pos);
        Some(args.remove(pos))
    } else {
        None
    }
}

pub(crate) fn remove_all_value_flags(args: &mut Vec<String>, flag: &str) -> Vec<String> {
    let mut values = Vec::new();
    while let Some(v) = remove_value_flag(args, flag) {
        values.push(v);
    }
    values
}

pub(crate) fn print_usage() {
    println!("kayactl — KayaDB command-line tool");
    println!();
    println!("LOCAL ENGINE COMMANDS");
    println!("  kayactl [--data <dir>] [--durability strict|relaxed] [--json] put <key> <value>");
    println!("  kayactl [--data <dir>] [--json] get <key>");
    println!("  kayactl [--data <dir>] [--durability strict|relaxed] [--json] delete <key>");
    println!("  kayactl [--data <dir>] [--json] scan <prefix>");
    println!("  kayactl [--data <dir>] [--durability strict|relaxed] [--json] flush   (force memtable -> SSTable for observability)");
    println!();
    println!("OBSERVABILITY COMMANDS");
    println!("  kayactl [--data <dir>] [--json] [--latency] stats   (add --latency for focused durability + flush/compaction timers)");
    println!("  kayactl [--data <dir>] [--durability ...] [--json] flush   (force publish to see latency numbers move; pairs with --latency and ebpf probes)");
    println!("  kayactl [--data <dir>] [--json] recover --dry-run");
    println!("  kayactl ebpf [fsync-latency|...|list|status|help] [--pid <pid>] [--run] [--duration 30s]   (Linux eBPF experiments, Track A)");
    println!();
    println!("CLUSTER MODE (via running kayadb-server)");
    println!("  kayactl --server <addr> [--server <addr2> ...] [--timeout <ms>] [--operator-token <tok>] [--json] put <key> <value>");
    println!(
        "  kayactl --server <addr> [--server <addr2> ...] [--timeout <ms>] [--operator-token <tok>] [--json] get <key>"
    );
    println!(
        "  kayactl --server <addr> [--server <addr2> ...] [--timeout <ms>] [--operator-token <tok>] [--json] delete <key>"
    );
    println!(
        "  kayactl --server <addr> [--server <addr2> ...] [--timeout <ms>] [--operator-token <tok>] [--json] scan <prefix>"
    );
    println!("  kayactl --server <addr> [--server <addr2> ...] [--timeout <ms>] [--operator-token <tok>] [--json] health");
    println!("  kayactl --server <addr> [--server <addr2> ...] [--timeout <ms>] [--operator-token <tok>] [--json] [--latency] status");
    println!("  kayactl --server <addr> [--operator-token <tok>] add-node <id> <raft-addr> <client-addr>");
    println!("  kayactl --server <addr> [--operator-token <tok>] remove-node <id>");
    println!();
    println!("INSPECT COMMANDS");
    println!("  kayactl [--json] inspect wal <path>");
    println!("  kayactl [--json] inspect sstable <path>");
    println!("  kayactl [--json] inspect manifest <path>");
    println!();
    println!("DEFAULTS");
    println!("  --data ./data");
    println!("  --durability strict");
    println!("  --timeout (none)");
    println!("  --operator-token (none, or KAYA_OPERATOR_TOKEN env var)");
}

pub(crate) fn option_usize_json(value: Option<usize>) -> String {
    value.map_or_else(|| "null".to_owned(), |value| value.to_string())
}

pub(crate) fn json_string(value: &str) -> String {
    format!("\"{}\"", json_escape(value))
}

pub(crate) fn json_escape(value: &str) -> String {
    value
        .chars()
        .flat_map(|ch| match ch {
            '"' => "\\\"".chars().collect::<Vec<_>>(),
            '\\' => "\\\\".chars().collect::<Vec<_>>(),
            '\n' => "\\n".chars().collect::<Vec<_>>(),
            '\r' => "\\r".chars().collect::<Vec<_>>(),
            '\t' => "\\t".chars().collect::<Vec<_>>(),
            other => vec![other],
        })
        .collect()
}
