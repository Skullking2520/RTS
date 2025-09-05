// perf_bench.rs
use criterion::{criterion_group, criterion_main, Criterion, black_box};
use gcs_rts::types::{TelemetryMessage, CommandMessage, CommandType, CommandPriority, SensorStatus, MessageHeader};

fn bench_decode(c: &mut Criterion) {
    let t = TelemetryMessage {
        header: MessageHeader::new("ocs", "telemetry", 42),
        sensor_type: "Thermal".to_string(),
        value: 25.5,
        unit: "celsius".to_string(),
        status: SensorStatus::Normal,
    };
    let bin = bincode::serialize(&t).unwrap();
    let json = serde_json::to_vec(&t).unwrap();
    c.bench_function("decode_bincode", |b| b.iter(|| { let _: TelemetryMessage = bincode::deserialize(black_box(&bin)).unwrap(); }));
    c.bench_function("decode_json", |b| b.iter(|| { let _: TelemetryMessage = serde_json::from_slice(black_box(&json)).unwrap(); }));
}

fn bench_encode(c: &mut Criterion) {
    let cmd = CommandMessage::new(
        CommandType::SystemStatus,
        serde_json::json!({"urgent": true, "deadline_ms": 2}),
        CommandPriority::High
    );
    c.bench_function("encode_bincode", |b| b.iter(|| { let _ = bincode::serialize(black_box(&cmd)).unwrap(); }));
    c.bench_function("encode_json", |b| b.iter(|| { let _ = serde_json::to_vec(black_box(&cmd)).unwrap(); }));
}

criterion_group!(benches, bench_decode, bench_encode);
criterion_main!(benches);
