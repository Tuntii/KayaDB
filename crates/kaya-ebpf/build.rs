use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=bpf/fsync_latency.bpf.c");
    println!("cargo:rerun-if-changed=bpf/include");

    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let kernel_probes = std::env::var("CARGO_FEATURE_KERNEL_PROBES").is_ok();
    if target_os != "linux" || !kernel_probes {
        return;
    }

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"));
    let include_root = prepare_include_dir(&out_dir);
    let obj = out_dir.join("fsync_latency.bpf.o");
    let src = PathBuf::from("bpf/fsync_latency.bpf.c");

    if !clang_available() {
        eprintln!(
            "cargo:warning=kaya-ebpf: clang not found; kernel bpf object not built (userspace tap fallback)"
        );
        return;
    }

    let arch_define = match std::env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default().as_str() {
        "x86_64" => "-D__TARGET_ARCH_x86",
        "aarch64" => "-D__TARGET_ARCH_arm64",
        _ => {
            eprintln!(
                "cargo:warning=kaya-ebpf: unsupported BPF arch; kernel bpf object not built"
            );
            return;
        }
    };

    let mut cmd = Command::new("clang");
    cmd.args([
        "-g",
        "-O2",
        "-target",
        "bpf",
        arch_define,
        "-I",
        include_root.to_str().expect("include path"),
        "-c",
        src.to_str().expect("src path"),
        "-o",
        obj.to_str().expect("obj path"),
    ]);

    match cmd.status() {
        Ok(status) if status.success() => {
            println!("cargo:rustc-cfg=kaya_ebpf_bpf_built");
            println!("cargo:warning=kaya-ebpf: bpf object built at {}", obj.display());
        }
        Ok(status) => {
            eprintln!(
                "cargo:warning=kaya-ebpf: clang bpf compile failed (exit={}); userspace tap fallback",
                status.code().unwrap_or(-1)
            );
        }
        Err(e) => {
            eprintln!("cargo:warning=kaya-ebpf: clang bpf compile error: {e}");
        }
    }
}

fn prepare_include_dir(out_dir: &Path) -> PathBuf {
    let include_root = out_dir.join("bpf-include");
    let _ = fs::remove_dir_all(&include_root);
    copy_tree(Path::new("bpf/include"), &include_root).expect("copy bpf/include");

    let vmlinux_dest = include_root.join("vmlinux.h");
    if !try_generate_vmlinux(&vmlinux_dest) {
        // Bundled minimal header already copied from bpf/include/vmlinux.h.
    }
    include_root
}

fn copy_tree(src: &Path, dst: &Path) -> std::io::Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let path = entry.path();
        let target = dst.join(entry.file_name());
        if path.is_dir() {
            copy_tree(&path, &target)?;
        } else {
            fs::copy(&path, &target)?;
        }
    }
    Ok(())
}

fn try_generate_vmlinux(dest: &Path) -> bool {
    let btf = Path::new("/sys/kernel/btf/vmlinux");
    if !btf.exists() {
        return false;
    }
    let output = Command::new("bpftool")
        .args([
            "btf",
            "dump",
            "file",
            btf.to_str().expect("btf path"),
            "format",
            "c",
        ])
        .output();
    match output {
        Ok(out) if out.status.success() => {
            if let Ok(mut file) = fs::File::create(dest) {
                let _ = file.write_all(&out.stdout);
                return true;
            }
        }
        _ => {}
    }
    false
}

fn clang_available() -> bool {
    Command::new("clang")
        .args(["--version"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}