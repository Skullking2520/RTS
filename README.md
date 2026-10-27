# Ground Control Station coursework

My GCS implementation for the **Real-Time System** team assignment at APU. The team project covers satellite–ground communication; this repository contains the ground-station part, not the complete onboard system.

The Rust application uses Tokio and RabbitMQ to receive telemetry, schedule commands, apply safety checks, and record timing metrics. It also starts simulated faults and commands in [src/main.rs](src/main.rs).

## Code

| File | Responsibility |
| --- | --- |
| [src/main.rs](src/main.rs) | Application startup, tasks, and built-in simulation |
| [src/amqp.rs](src/amqp.rs) | RabbitMQ exchanges, queues, and message transport |
| [src/telemetry.rs](src/telemetry.rs) | Telemetry reception and decoding |
| [src/uplink.rs](src/uplink.rs) | Command scheduling and dispatch |
| [src/safety.rs](src/safety.rs) | Interlocks and command validation |
| [src/metrics.rs](src/metrics.rs), [src/performance.rs](src/performance.rs) | Timing records and reporting |
| [src/config.rs](src/config.rs) | Environment variables and defaults |

The existing `src/` and `benches/` layout is retained. No separate Python integration-test script or complete OCS implementation is included.

## Build and run

Install Rust/Cargo and start an authorized local RabbitMQ instance. Run from the repository root:

```sh
cargo build
cargo run
```

`AMQP_URI` selects the broker. The code defaults to a local guest account for development. Set your own broker URI in the shell when needed; do not commit credentials. `.env.example` lists optional settings but is not loaded automatically.

The program creates queues/exchanges and sends simulated commands. A matching telemetry publisher and agreed message format are needed to check communication with the team's OCS.

## Timing settings and limits

The executable calls `Config::load()`. Its defaults are:

| Setting | Default |
| --- | --- |
| `EXPECTED_PERIOD_MS` | 20 ms |
| `DECODE_BUDGET_MS` | 5 ms |
| `DISPATCH_BUDGET_MS` | 10 ms |
| `METRICS_PERIOD_SEC` | 10 s |
| `LOC_GAP_THRESHOLD` | 3 |
| `LOC_TIMEOUT_MS` | Expected period × 10; normally 200 ms |

These are configuration values, not measured latency guarantees. The earlier 3 ms decoding, 2 ms urgent-dispatch, and 100 ms fault-response figures describe assignment goals. No reproducible results in this repository establish that all deadlines are met.

Some queue/exchange names remain hardcoded in the AMQP and task startup paths. Changing the similarly named fields in `Config` alone does not reconfigure the whole application.

## Checks

```sh
cargo test --offline
cargo bench --offline --no-run
```

Offline commands need cached dependencies. [benches/perf_bench.rs](benches/perf_bench.rs) compares message serialization; it does not measure end-to-end satellite communication. `cargo test` currently discovers no unit or documentation tests; compilation alone does not validate runtime behavior.
