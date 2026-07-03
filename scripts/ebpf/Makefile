# KayaDB eBPF bpftrace helpers (Linux-only). Run from scripts/ebpf/.
PID ?= $(shell pgrep -f kayadb-server | head -1)
DURATION ?= 10

.PHONY: list fsync block timeline verify help

list:
	pgrep -af kayadb-server

fsync:
	sudo bpftrace -p $(PID) fsync-latency.bt

block:
	sudo bpftrace -p $(PID) block-io-latency.bt

timeline:
	sudo bpftrace -p $(PID) syscall-timeline.bt

verify:
	bash ../../scripts/linux_verify_ebpf_kernel.sh

help:
	@echo "KayaDB eBPF Makefile targets (Linux + bpftrace):"
	@echo ""
	@echo "  make list      - list kayadb-server PIDs (pgrep -af)"
	@echo "  make fsync     - attach fsync-latency.bt to PID (default: first kayadb-server)"
	@echo "  make block     - attach block-io-latency.bt to PID"
	@echo "  make timeline  - attach syscall-timeline.bt to PID"
	@echo "  make verify    - run Linux eBPF kernel gate (cargo test + bpf object)"
	@echo ""
	@echo "Variables:"
	@echo "  PID=$(PID)"
	@echo "  DURATION=$(DURATION)  (seconds; for timed runs / kayactl --duration pairing)"