# KayaDB eBPF bpftrace helpers (Linux-only). Run from scripts/ebpf/.
PID ?= $(shell pgrep -f kayadb-server | head -1)
DURATION ?= 10
DATADIR ?= ./data

.PHONY: list fsync block timeline flamegraph datadir parallel verify help

list:
	pgrep -af kayadb-server

fsync:
	sudo bpftrace -p $(PID) fsync-latency.bt

block:
	sudo bpftrace -p $(PID) block-io-latency.bt

timeline:
	sudo bpftrace -p $(PID) syscall-timeline.bt

flamegraph:
	sudo bpftrace -f flamegraph -p $(PID) durability-flamegraph.bt

# Per-file / data-dir filtered syscall trace (durability-syscalls.bt takes a
# path-substring filter as $1). Set DATADIR to your node's data directory.
datadir:
	sudo bpftrace durability-syscalls.bt $(DATADIR)

# Run fsync + block + timeline probes in parallel for DURATION seconds, each
# writing to its own log file. Requires one PID (attaches all three to it).
parallel:
	@echo "Running fsync/block/timeline in parallel on PID=$(PID) for $(DURATION)s..."
	sudo timeout $(DURATION) bpftrace -p $(PID) fsync-latency.bt    > fsync.log    2>&1 & \
	sudo timeout $(DURATION) bpftrace -p $(PID) block-io-latency.bt > block.log    2>&1 & \
	sudo timeout $(DURATION) bpftrace -p $(PID) syscall-timeline.bt > timeline.log 2>&1 & \
	wait
	@echo "Done. See fsync.log, block.log, timeline.log."

verify:
	bash ../../scripts/linux_verify_ebpf_kernel.sh

help:
	@echo "KayaDB eBPF Makefile targets (Linux + bpftrace):"
	@echo ""
	@echo "  make list      - list kayadb-server PIDs (pgrep -af)"
	@echo "  make fsync     - attach fsync-latency.bt to PID (default: first kayadb-server)"
	@echo "  make block     - attach block-io-latency.bt to PID"
	@echo "  make timeline  - attach syscall-timeline.bt to PID"
	@echo "  make flamegraph - bpftrace -f flamegraph durability stacks (pipe to flamegraph.pl)"
	@echo "  make datadir   - per-file trace filtered to DATADIR (durability-syscalls.bt)"
	@echo "  make parallel  - run fsync+block+timeline concurrently for DURATION seconds"
	@echo "  make verify    - run Linux eBPF kernel gate (cargo test + bpf object)"
	@echo ""
	@echo "Variables:"
	@echo "  PID=$(PID)"
	@echo "  DATADIR=$(DATADIR)  (data-dir path filter for 'make datadir')"
	@echo "  DURATION=$(DURATION)  (seconds; for timed runs / kayactl --duration pairing)"