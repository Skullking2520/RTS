// perf_bench.rs
use criterion::{criterion_group, criterion_main, Criterion, black_box};
use gcs_rts::types::{Telemetry, CommandWire, CommandKind};

fn bench_decode(c: &mut Criterion) {
    let t = Telemetry { seq: Some(42), ts_utc_ms: Some(1_723_531_234_567), sensor: Some("Thermal".into()), payload: Some(vec![0u8;64]), fault: None };
    let bin = bincode::serialize(&t).unwrap();
    let json = serde_json::to_vec(&t).unwrap();
    c.bench_function("decode_bincode", |b| b.iter(|| { let _: Telemetry = bincode::deserialize(black_box(&bin)).unwrap(); }));
    c.bench_function("decode_json", |b| b.iter(|| { let _: Telemetry = serde_json::from_slice(black_box(&json)).unwrap(); }));
}

fn bench_encode(c: &mut Criterion) {
    let cmd = CommandWire { id: 1, kind: CommandKind::ReRequest(99), urgent: true, deadline_ms: 2 };
    c.bench_function("encode_bincode", |b| b.iter(|| { let _ = bincode::serialize(black_box(&cmd)).unwrap(); }));
    c.bench_function("encode_json", |b| b.iter(|| { let _ = serde_json::to_vec(black_box(&cmd)).unwrap(); }));
}

criterion_group!(benches, bench_decode, bench_encode);
criterion_main!(benches);
