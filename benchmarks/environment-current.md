# Current Benchmark Environment

Captured on 2026-09-23 from the development environment.

This is a Linux virtual machine, not a benchmark run on bare metal:

- virtualization: VMware full virtualization (`VMware Virtual Platform`);
- operating system: Ubuntu 26.04 LTS;
- kernel: Linux 7.0.0-31-generic, x86_64;
- presented CPUs: 12 logical CPUs;
- reported CPU model: Intel Core i9-14900HX;
- reported memory: 16 GiB RAM;
- swap: 4 GiB;
- hypervisor: VMware.

These specifications describe the guest environment only. CPU scheduling,
memory pressure, virtualization overhead, host contention, and power policy
can affect the results. Results from this environment must not be presented as
bare-metal performance or as universal hardware numbers.

Every generated benchmark result also records the environment metadata at the
time of the run. Prefer comparing results generated on the same VM profile.
