// SPDX-License-Identifier: GPL-2.0 OR MIT
// WAL durability syscall latency probe (fsync/fdatasync kretprobe path).
// Filters by target_pid map (index 0) set from userspace at attach time.
#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>

struct fsync_event {
    __u64 latency_us;
    __u8 syscall_kind; // 0 = fsync, 1 = fdatasync
};

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 256 * 1024);
} events SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 8192);
    __type(key, __u64);
    __type(value, __u64);
} start_ns SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u32);
} target_pid SEC(".maps");

static __always_inline int pid_allowed(void) {
    __u32 key = 0;
    __u32 *want = bpf_map_lookup_elem(&target_pid, &key);
    if (!want || *want == 0) {
        return 0;
    }
    __u32 tgid = bpf_get_current_pid_tgid() >> 32;
    return tgid == *want;
}

static __always_inline int trace_fsync_exit(struct pt_regs *ctx, __u8 kind) {
    if (!pid_allowed()) {
        return 0;
    }
    __u64 id = bpf_get_current_pid_tgid();
    __u64 *tsp = bpf_map_lookup_elem(&start_ns, &id);
    if (!tsp) {
        return 0;
    }
    __u64 delta_us = (bpf_ktime_get_ns() - *tsp) / 1000;
    bpf_map_delete_elem(&start_ns, &id);

    struct fsync_event *e = bpf_ringbuf_reserve(&events, sizeof(*e), 0);
    if (!e) {
        return 0;
    }
    e->latency_us = delta_us;
    e->syscall_kind = kind;
    bpf_ringbuf_submit(e, 0);
    return 0;
}

SEC("kprobe/__x64_sys_fsync")
int fsync_enter(struct pt_regs *ctx) {
    if (!pid_allowed()) {
        return 0;
    }
    __u64 id = bpf_get_current_pid_tgid();
    __u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&start_ns, &id, &ts, BPF_ANY);
    return 0;
}

SEC("kretprobe/__x64_sys_fsync")
int fsync_exit(struct pt_regs *ctx) {
    return trace_fsync_exit(ctx, 0);
}

SEC("kprobe/__x64_sys_fdatasync")
int fdatasync_enter(struct pt_regs *ctx) {
    if (!pid_allowed()) {
        return 0;
    }
    __u64 id = bpf_get_current_pid_tgid();
    __u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&start_ns, &id, &ts, BPF_ANY);
    return 0;
}

SEC("kretprobe/__x64_sys_fdatasync")
int fdatasync_exit(struct pt_regs *ctx) {
    return trace_fsync_exit(ctx, 1);
}

char LICENSE[] SEC("license") = "Dual MIT/GPL";