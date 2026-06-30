use std::path::PathBuf;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=bpf/fsync_latency.bpf.c");

    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let kernel_probes = std::env::var("CARGO_FEATURE_KERNEL_PROBES").is_ok();
    if target_os != "linux" || !kernel_probes {
        return;
    }

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"));
    let src = PathBuf::from("bpf/fsync_latency.bpf.c");
    let obj = out_dir.join("fsync_latency.bpf.o");
    if Command::new("clang")
        .args(["--version"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
    {
        let status = Command::new("clang")
            .args([
                "-g",
                "-O2",
                "-target",
                "bpf",
                "-c",
                src.to_str().expect("src path"),
                "-o",
                obj.to_str().expect("obj path"),
            ])
            .status();
        if status.is_ok_and(|s| s.success()) {
            println!("cargo:rustc-cfg=kaya_ebpf_bpf_built");
        }
    }
}