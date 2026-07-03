use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use kaya_core::{DurabilityMode, Result};
use kaya_ebpf::{
    filter_publish_events, filter_wal_events, MarkerPhase, MarkerSite, ProbeEvent, ProbeStatus,
};
use kaya_engine::EngineStats;

use crate::cli::block_on;

/// Userspace WAL fsync metrics from `EngineStats`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserspaceWalSummary {
    pub count: u64,
    pub avg_us: Option<u64>,
    pub max_us: u64,
}

/// Kernel-side WAL fsync metrics from `trace.jsonl` + `status.json`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KernelTraceSummary {
    pub events: u64,
    pub avg_us: Option<u64>,
    pub max_us: u64,
    pub backend: String,
}

/// Flush metrics paired with syscall-timeline hints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushSummary {
    pub count: u64,
    pub avg_us: Option<u64>,
}

/// USDT-shaped marker counts from `trace.jsonl`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MarkerSummary {
    pub wal_enter: u64,
    pub wal_exit: u64,
    pub flush_enter: u64,
    pub flush_exit: u64,
}

/// Publish-phase syscall events from `trace.jsonl`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishSummary {
    pub events: u64,
    pub kinds: Vec<String>,
}

/// Full userspace↔kernel correlation report (Track A).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorrelateReport {
    pub userspace: UserspaceWalSummary,
    pub kernel: Option<KernelTraceSummary>,
    pub delta_hint: String,
    pub flush: FlushSummary,
    pub markers: MarkerSummary,
    pub publish: PublishSummary,
    pub no_trace_hint: Option<String>,
}

/// Build a correlation report by opening the local engine and reading eBPF artifacts.
pub(crate) fn correlate_report(
    data_dir: &str,
    durability: DurabilityMode,
) -> Result<CorrelateReport> {
    let engine = block_on(crate::local::open_engine(data_dir.to_owned(), durability))?;
    let stats = engine.stats();
    build_correlate_report(Path::new(data_dir), &stats)
}

/// Build a correlation report from existing stats and `{data_dir}/ebpf/*` artifacts.
pub(crate) fn build_correlate_report(
    data_dir: &Path,
    stats: &EngineStats,
) -> Result<CorrelateReport> {
    let userspace = summarize_userspace_wal(stats);
    let flush = summarize_flush(stats);

    let trace_path = data_dir.join("ebpf/trace.jsonl");
    let status_path = data_dir.join("ebpf/status.json");

    let (markers, publish) = if trace_path.is_file() {
        let events = load_trace_events(&trace_path)?;
        (
            summarize_markers(&events),
            summarize_publish(filter_publish_events(&events)),
        )
    } else {
        (
            MarkerSummary::default(),
            PublishSummary {
                events: 0,
                kinds: Vec::new(),
            },
        )
    };

    let kernel = if trace_path.is_file() {
        let events = load_trace_events(&trace_path)?;
        let wal = filter_wal_events(&events);
        if wal.is_empty() {
            None
        } else {
            let (total_us, max_us) = wal_latency_totals(&wal);
            let events_count = wal.len() as u64;
            let backend = load_status_backend(&status_path).unwrap_or_else(|| "unknown".to_owned());
            Some(KernelTraceSummary {
                events: events_count,
                avg_us: total_us.checked_div(events_count),
                max_us,
                backend,
            })
        }
    } else {
        None
    };

    let delta_hint = match (userspace.avg_us, kernel.as_ref().and_then(|k| k.avg_us)) {
        (Some(u), Some(k)) => format_delta_hint(u, k),
        (Some(_), None) => "kernel trace avg unavailable (no WAL fsync events in trace)".to_owned(),
        (None, Some(k)) => {
            format!("userspace avg unavailable (wal_fsync_count=0); kernel trace avg_us={k}")
        }
        (None, None) => "userspace and kernel trace averages unavailable".to_owned(),
    };

    let no_trace_hint = if kernel.is_none() {
        Some(no_kernel_trace_hint(&trace_path))
    } else {
        None
    };

    Ok(CorrelateReport {
        userspace,
        kernel,
        delta_hint,
        flush,
        markers,
        publish,
        no_trace_hint,
    })
}

pub(crate) fn print_correlate_human(report: &CorrelateReport) {
    let _ = write_correlate_human(report, &mut std::io::stdout());
}

#[cfg(test)]
fn render_correlate_human(report: &CorrelateReport) -> String {
    let mut buf = Vec::new();
    write_correlate_human(report, &mut buf).expect("render correlate human output");
    String::from_utf8(buf).expect("correlate human output is utf-8")
}

fn write_correlate_human<W: Write>(report: &CorrelateReport, out: &mut W) -> std::io::Result<()> {
    writeln!(out, "=== eBPF Correlation (Track A) ===")?;
    write_userspace_line(out, &report.userspace)?;
    if let Some(kernel) = &report.kernel {
        write_kernel_line(out, kernel)?;
        writeln!(out, "Delta hint:     {}", report.delta_hint)?;
    } else if let Some(hint) = &report.no_trace_hint {
        writeln!(out, "Kernel trace:   (none)")?;
        writeln!(out, "Hint:           {hint}")?;
    }
    write_flush_line(out, &report.flush)?;
    write_marker_line(out, &report.markers)?;
    write_publish_line(out, &report.publish)?;
    if report.markers.wal_enter == 0
        && report.markers.flush_enter == 0
        && report.publish.events == 0
        && report.kernel.is_none()
    {
        writeln!(
            out,
            "Hint:           run kayadb-server --ebpf to record usdt_marker + publish_syscall events"
        )?;
    }
    writeln!(out, "========================================")?;
    Ok(())
}

fn summarize_userspace_wal(stats: &EngineStats) -> UserspaceWalSummary {
    UserspaceWalSummary {
        count: stats.wal_fsync_count,
        avg_us: stats.wal_fsync_total_us.checked_div(stats.wal_fsync_count),
        max_us: stats.wal_fsync_max_us,
    }
}

fn summarize_flush(stats: &EngineStats) -> FlushSummary {
    FlushSummary {
        count: stats.flush_count,
        avg_us: stats.flush_total_us.checked_div(stats.flush_count),
    }
}

fn summarize_markers(events: &[ProbeEvent]) -> MarkerSummary {
    let mut summary = MarkerSummary::default();
    for event in events {
        let ProbeEvent::UsdtMarker { site, phase, .. } = event else {
            continue;
        };
        match (site, phase) {
            (MarkerSite::WalFsync, MarkerPhase::Enter) => summary.wal_enter += 1,
            (MarkerSite::WalFsync, MarkerPhase::Exit) => summary.wal_exit += 1,
            (MarkerSite::Flush, MarkerPhase::Enter) => summary.flush_enter += 1,
            (MarkerSite::Flush, MarkerPhase::Exit) => summary.flush_exit += 1,
        }
    }
    summary
}

fn summarize_publish(publish: Vec<&ProbeEvent>) -> PublishSummary {
    let mut kinds = Vec::new();
    for event in &publish {
        let ProbeEvent::PublishSyscall { syscall, .. } = event else {
            continue;
        };
        let label = syscall.as_str().to_owned();
        if !kinds.contains(&label) {
            kinds.push(label);
        }
    }
    PublishSummary {
        events: publish.len() as u64,
        kinds,
    }
}

fn wal_latency_totals(wal: &[&ProbeEvent]) -> (u64, u64) {
    let mut total_us = 0u64;
    let mut max_us = 0u64;
    for event in wal {
        if let ProbeEvent::FsyncLatency { latency_us, .. } = event {
            total_us = total_us.saturating_add(*latency_us);
            max_us = max_us.max(*latency_us);
        }
    }
    (total_us, max_us)
}

fn load_trace_events(trace_path: &Path) -> Result<Vec<ProbeEvent>> {
    let file = File::open(trace_path).map_err(|e| kaya_core::KayaError::Io {
        message: e.to_string(),
    })?;
    let reader = BufReader::new(file);
    let mut events = Vec::new();
    for (idx, line) in reader.lines().enumerate() {
        let line = line.map_err(|e| kaya_core::KayaError::Io {
            message: e.to_string(),
        })?;
        if idx == 0 && line.contains("artifact") {
            continue;
        }
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(event) = serde_json::from_str::<ProbeEvent>(&line) {
            events.push(event);
        }
    }
    Ok(events)
}

fn load_status_backend(status_path: &Path) -> Option<String> {
    if !status_path.is_file() {
        return None;
    }
    let raw = std::fs::read_to_string(status_path).ok()?;
    let status: ProbeStatus = serde_json::from_str(&raw).ok()?;
    Some(status.backend)
}

fn format_delta_hint(userspace_avg: u64, kernel_avg: u64) -> String {
    if kernel_avg == 0 {
        return "kernel trace avg is zero; cannot compare".to_owned();
    }
    let diff = userspace_avg.abs_diff(kernel_avg);
    let pct = (diff as f64 / kernel_avg as f64) * 100.0;
    if pct <= 5.0 {
        format!("userspace avg within ~{pct:.0}% of kernel trace (rough)")
    } else {
        format!("userspace avg differs ~{pct:.0}% from kernel trace (rough)")
    }
}

fn no_kernel_trace_hint(trace_path: &Path) -> String {
    format!(
        "start kayadb-server --ebpf (populates {}) and/or bpftrace wrappers in scripts/ebpf/",
        trace_path.display()
    )
}

fn write_userspace_line<W: Write>(
    out: &mut W,
    userspace: &UserspaceWalSummary,
) -> std::io::Result<()> {
    match userspace.avg_us {
        Some(avg) => writeln!(
            out,
            "Userspace WAL:  count={}  avg_us={}  max_us={}",
            userspace.count, avg, userspace.max_us
        ),
        None => writeln!(
            out,
            "Userspace WAL:  count={}  avg_us=(n/a)  max_us={}",
            userspace.count, userspace.max_us
        ),
    }
}

fn write_kernel_line<W: Write>(out: &mut W, kernel: &KernelTraceSummary) -> std::io::Result<()> {
    match kernel.avg_us {
        Some(avg) => writeln!(
            out,
            "Kernel trace:   events={}  avg_us={}  max_us={}  backend={}",
            kernel.events, avg, kernel.max_us, kernel.backend
        ),
        None => writeln!(
            out,
            "Kernel trace:   events={}  avg_us=(n/a)  max_us={}  backend={}",
            kernel.events, kernel.max_us, kernel.backend
        ),
    }
}

fn write_marker_line<W: Write>(out: &mut W, markers: &MarkerSummary) -> std::io::Result<()> {
    writeln!(
        out,
        "USDT markers:   wal_enter={} wal_exit={} flush_enter={} flush_exit={}",
        markers.wal_enter, markers.wal_exit, markers.flush_enter, markers.flush_exit
    )
}

fn write_publish_line<W: Write>(out: &mut W, publish: &PublishSummary) -> std::io::Result<()> {
    if publish.events == 0 {
        return writeln!(out, "Publish trace:  events=0 kinds=(none)");
    }
    writeln!(
        out,
        "Publish trace:  events={} kinds={}",
        publish.events,
        publish.kinds.join(",")
    )
}

fn write_flush_line<W: Write>(out: &mut W, flush: &FlushSummary) -> std::io::Result<()> {
    match flush.avg_us {
        Some(avg) => writeln!(
            out,
            "Flush:          count={} avg_us={}  (pair with syscall-timeline rename/unlink)",
            flush.count, avg
        ),
        None => writeln!(
            out,
            "Flush:          count={} avg_us=(n/a)  (pair with syscall-timeline rename/unlink)",
            flush.count
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaya_ebpf::{seeded_fsync_events, seeded_mixed_durability_events, write_trace};
    use kaya_engine::EngineStats;
    use tempfile::tempdir;

    fn fixture_stats() -> EngineStats {
        EngineStats {
            wal_fsync_count: 42,
            wal_fsync_total_us: 42 * 380,
            wal_fsync_max_us: 1200,
            flush_count: 3,
            flush_total_us: 3 * 45_000,
            ..EngineStats::default()
        }
    }

    #[test]
    fn correlate_report_matches_fixture_trace_and_synthetic_stats() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path();
        let trace_path = data_dir.join("ebpf/trace.jsonl");
        std::fs::create_dir_all(trace_path.parent().unwrap()).unwrap();

        // 38 events with avg ~365 us and max 1150 (seeded distribution is close enough).
        let mut events = seeded_fsync_events(7, 38);
        if let ProbeEvent::FsyncLatency { latency_us, .. } = &mut events[0] {
            *latency_us = 1150;
        }
        write_trace(&trace_path, 7, "correlate-test", &events).unwrap();

        let status_path = data_dir.join("ebpf/status.json");
        std::fs::write(
            &status_path,
            r#"{"attached":true,"streaming":true,"backend":"kernel-simulated","events_collected":38,"seed":7,"trace_path":"ebpf/trace.jsonl"}"#,
        )
        .unwrap();

        let report = build_correlate_report(data_dir, &fixture_stats()).unwrap();

        assert_eq!(report.userspace.count, 42);
        assert_eq!(report.userspace.avg_us, Some(380));
        assert_eq!(report.userspace.max_us, 1200);

        let kernel = report.kernel.expect("expected kernel summary");
        assert_eq!(kernel.events, 38);
        assert!(kernel.avg_us.is_some());
        assert_eq!(kernel.max_us, 1150);
        assert_eq!(kernel.backend, "kernel-simulated");

        assert!(report.delta_hint.contains("kernel trace"));
        assert_eq!(report.flush.count, 3);
        assert_eq!(report.flush.avg_us, Some(45_000));
        assert!(report.no_trace_hint.is_none());
    }

    #[test]
    fn correlate_report_without_trace_emits_start_hint() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path();
        let report = build_correlate_report(data_dir, &fixture_stats()).unwrap();

        assert!(report.kernel.is_none());
        let hint = report.no_trace_hint.expect("expected hint");
        assert!(hint.contains("kayadb-server --ebpf"));
        assert!(hint.contains("bpftrace"));
    }

    #[test]
    fn delta_hint_within_five_percent_uses_within_wording() {
        assert!(format_delta_hint(380, 365).contains("within"));
        assert!(format_delta_hint(400, 365).contains("differs"));
    }

    #[test]
    fn correlate_human_output_names_marker_and_publish_kinds() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path();
        let trace_path = data_dir.join("ebpf/trace.jsonl");
        std::fs::create_dir_all(trace_path.parent().unwrap()).unwrap();
        write_trace(
            &trace_path,
            88,
            "correlate-human",
            &seeded_mixed_durability_events(88),
        )
        .unwrap();
        let report = build_correlate_report(data_dir, &fixture_stats()).unwrap();
        let rendered = render_correlate_human(&report);
        assert!(rendered.contains("USDT markers"));
        assert!(rendered.contains("wal_enter=1"));
        assert!(rendered.contains("Publish trace"));
        assert!(rendered.contains("rename"));
    }

    #[test]
    fn correlate_report_surfaces_mixed_marker_and_publish_events() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path();
        let trace_path = data_dir.join("ebpf/trace.jsonl");
        std::fs::create_dir_all(trace_path.parent().unwrap()).unwrap();
        let events = seeded_mixed_durability_events(88);
        write_trace(&trace_path, 88, "mixed-correlate", &events).unwrap();

        let report = build_correlate_report(data_dir, &fixture_stats()).unwrap();
        assert_eq!(report.markers.wal_enter, 1);
        assert_eq!(report.markers.wal_exit, 1);
        assert_eq!(report.markers.flush_enter, 1);
        assert_eq!(report.markers.flush_exit, 1);
        assert!(report.publish.events >= 2);
        assert!(report.publish.kinds.iter().any(|k| k == "rename"));
        assert!(report.publish.kinds.iter().any(|k| k == "fsync_dir"));
    }
}
