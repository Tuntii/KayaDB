/* Minimal x86_64 vmlinux types for BPF kprobe programs (bundled fallback).
 * On Linux, build.rs prefers bpftool-generated vmlinux.h when available. */
#ifndef __VMLINUX_H__
#define __VMLINUX_H__

typedef unsigned char __u8;
typedef signed char __s8;
typedef unsigned short __u16;
typedef signed short __s16;
typedef unsigned int __u32;
typedef signed int __s32;
typedef unsigned long long __u64;
typedef signed long long __s64;

struct pt_regs {
    __u64 r15;
    __u64 r14;
    __u64 r13;
    __u64 r12;
    __u64 bp;
    __u64 bx;
    __u64 r11;
    __u64 r10;
    __u64 r9;
    __u64 r8;
    __u64 ax;
    __u64 cx;
    __u64 dx;
    __u64 si;
    __u64 di;
    __u64 orig_ax;
    __u64 ip;
    __u64 cs;
    __u64 flags;
    __u64 sp;
    __u64 ss;
};

#endif /* __VMLINUX_H__ */