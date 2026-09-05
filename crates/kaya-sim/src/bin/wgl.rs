//! Offline WGL explorer: JSONL history → greedy counterexample and optional MUSs.
//!
//! ```text
//! kaya-wgl [--mus] [--mus-cap N] [--json] [FILE]
//! ```
//!
//! FILE omitted or `-` reads stdin. See `kaya_sim::wgl_jsonl` for the schema.

use kaya_sim::{
    parse_history_jsonl, report_json, LinearizabilityChecker, MinimalCounterexample, WGL_MAX_OPS,
};
use std::env;
use std::io::{self, Read, Write};
use std::process::ExitCode;

fn main() -> ExitCode {
    match run(env::args().skip(1).collect()) {
        Ok(code) => code,
        Err(err) => {
            let mut out = io::stderr();
            let _ = writeln!(out, "kaya-wgl: {err}");
            ExitCode::from(2)
        }
    }
}

fn run(mut args: Vec<String>) -> Result<ExitCode, String> {
    if args.iter().any(|a| a == "--help" || a == "-h") {
        print_help();
        return Ok(ExitCode::SUCCESS);
    }

    let want_mus = remove_flag(&mut args, "--mus");
    let want_json = remove_flag(&mut args, "--json");
    let mus_cap = match remove_value_flag(&mut args, "--mus-cap") {
        Some(s) => s
            .parse::<usize>()
            .map_err(|_| format!("--mus-cap expects a non-negative integer, got {s:?}"))?,
        None if args.iter().any(|a| a == "--mus-cap") => {
            return Err("--mus-cap requires a value".into());
        }
        None => WGL_MAX_OPS,
    };

    if let Some(unknown) = args
        .iter()
        .find(|a| a.starts_with('-') && a.as_str() != "-")
    {
        return Err(format!("unknown flag {unknown:?} (see --help)"));
    }
    if args.len() > 1 {
        return Err("expected at most one FILE (or omit / pass - for stdin)".into());
    }
    let path = args.first().map(String::as_str);
    let input = read_input(path)?;
    let checker = parse_history_jsonl(&input)?;
    let greedy = checker.minimal_counterexample();
    let muss = if want_mus {
        Some(checker.minimal_unsatisfiable_subsets(mus_cap))
    } else {
        None
    };

    if want_json {
        println!(
            "{}",
            report_json(checker.len(), greedy.as_ref(), muss.as_deref())
        );
    } else {
        print_human(&checker, greedy.as_ref(), muss.as_deref(), mus_cap);
    }

    if greedy.is_none() {
        Ok(ExitCode::SUCCESS)
    } else {
        Ok(ExitCode::from(1))
    }
}

fn print_human(
    checker: &LinearizabilityChecker,
    greedy: Option<&MinimalCounterexample>,
    muss: Option<&[MinimalCounterexample]>,
    mus_cap: usize,
) {
    println!("history: {} ops", checker.len());
    if checker.len() > WGL_MAX_OPS {
        println!(
            "note: concurrent check enumerates at most {WGL_MAX_OPS} ops; larger histories are partitioned by key / scan"
        );
    }
    match greedy {
        None => {
            println!("linearizable: yes");
        }
        Some(cex) => {
            println!("linearizable: no");
            println!();
            println!("greedy:");
            println!("{cex}");
        }
    }
    if let Some(list) = muss {
        println!();
        println!("MUSs: {} (cap {})", list.len(), mus_cap.min(WGL_MAX_OPS));
        for (i, mus) in list.iter().enumerate() {
            println!();
            println!("-- MUS {} --", i + 1);
            println!("{mus}");
        }
    }
}

fn read_input(path: Option<&str>) -> Result<String, String> {
    match path {
        None | Some("-") => {
            let mut buf = String::new();
            io::stdin()
                .read_to_string(&mut buf)
                .map_err(|e| format!("read stdin: {e}"))?;
            Ok(buf)
        }
        Some(p) => std::fs::read_to_string(p).map_err(|e| format!("read {p}: {e}")),
    }
}

fn remove_flag(args: &mut Vec<String>, flag: &str) -> bool {
    let before = args.len();
    args.retain(|arg| arg != flag);
    args.len() != before
}

fn remove_value_flag(args: &mut Vec<String>, flag: &str) -> Option<String> {
    let pos = args.iter().position(|arg| arg == flag)?;
    if pos + 1 < args.len() {
        args.remove(pos);
        Some(args.remove(pos))
    } else {
        None
    }
}

fn print_help() {
    println!(
        "\
kaya-wgl: WGL linearizability explorer

Usage:
  kaya-wgl [--mus] [--mus-cap N] [--json] [FILE]
  kaya-wgl --help

Read a JSONL history from FILE. FILE omitted or - reads stdin.
Always prints the greedy minimal counterexample. --mus also enumerates
inclusion-minimal unsatisfiable subsets (MUSs) for histories or per-key
/ scan partitions of at most --mus-cap ops (default {WGL_MAX_OPS}, the WGL bound).

Flags:
  --mus           enumerate MUSs (not only the greedy subset)
  --mus-cap N     max ops to brute-force (default {WGL_MAX_OPS})
  --json          machine JSON on stdout
  -h, --help      this help

JSONL (one object per op; unknown fields are errors):
  {{\"client\":0,\"start\":1,\"end\":2,\"op\":\"put\",\"key\":\"k\",\"value\":\"v\",\"result\":\"ok\"}}
  {{\"client\":1,\"start\":1,\"end\":3,\"op\":\"get\",\"key\":\"k\",\"result\":\"v\"}}
  {{\"op\":\"get\",\"key\":\"k\",\"result\":null}}
  {{\"op\":\"delete\",\"key\":\"k\",\"result\":\"ok\"}}
  {{\"op\":\"scan\",\"prefix\":\"a\",\"result\":[[\"a1\",\"v\"]]}}
  {{\"op\":\"put\",\"key\":\"0x6b\",\"value\":\"0x76\",\"result\":\"ok\"}}

  op is put | get | delete | scan (lowercase).
  Byte fields are UTF-8, or hex if prefixed 0x.
  start/end are a half-open tick interval; omit both to auto-assign.
  put/delete result: \"ok\" or {{\"error\":\"...\"}}.
  get result: string, null (miss), or {{\"error\":\"...\"}}.
  scan result: array of [key, value] pairs, or {{\"error\":\"...\"}}.

Exit status:
  0  linearizable
  1  not linearizable
  2  usage or parse error"
    );
}
