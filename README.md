# 위성·지상국 통신 시뮬레이션

Rust·RabbitMQ로 지상 관제 시스템의 텔레메트리 수신, 명령 전송, 안전 조건과 성능 로그를 다룹니다.

## 코드 안내

| 파일 | 역할 |
| --- | --- |
| [src/main.rs](src/main.rs) | 실행 진입점 |
| [src/config.rs](src/config.rs) | AMQP 주소·시간 기준 설정 |
| [src/telemetry.rs](src/telemetry.rs) | 텔레메트리 처리 |
| [src/uplink.rs](src/uplink.rs) | 명령 전송 |
| [src/safety.rs](src/safety.rs) | 안전 조건 |
| [src/metrics.rs](src/metrics.rs) · [src/performance.rs](src/performance.rs) | 성능 지표 |

## 실행 조건

Rust·Cargo와 RabbitMQ, 통신 상대의 메시지 형식·큐 설정이 필요합니다. `AMQP_URI` 등은 `src/config.rs`를 기준으로 지정합니다.

```bash
cargo build
cargo run
```

텔레메트리 발행 측이 없으면 전체 통신 동작을 재현할 수 없습니다. 시간 예산은 설정값이며 보장된 실측 성능이 아닙니다. 과거 설명의 3ms·2ms 등은 현 코드의 기본 설정값과 다를 수 있으므로 [현재 설정](src/config.rs)을 기준으로 확인합니다. [기존 상세 문서](docs/ORIGINAL_README.md)는 당시 설계·목표 자료로 보관합니다.
