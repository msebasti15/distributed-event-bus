# Benchmark Results

Benchmark results are committed to this directory so readers can inspect the
project's performance without running it first.

Each result file must include the Go version, commit, operating system, CPU
context when available, timestamp, command, and complete benchmark output.
Results are only comparable when the environment and benchmark command are
similar.

The current reference environment is documented in
[environment-current.md](environment-current.md). It is a VMware Linux VM,
not a direct bare-metal installation. Benchmark numbers must always be
reported with that context.

## Record a result

From the repository root:

```bash
./scripts/record-benchmarks.sh
```

This creates a timestamped file such as:

```text
benchmarks/results-20260923T120000Z.txt
```

Review the file, commit it, and link it from the relevant release notes. Do not
replace older results: keeping history makes regressions and improvements
visible.

## Benchmark command

The recorded command is:

```bash
go test ./... -run '^$' -bench . -benchmem
```

The benchmark suite currently covers broker publish/fan-out/deduplication and
binary protocol framing. Network load tests and p99 latency measurements will
be added separately.
