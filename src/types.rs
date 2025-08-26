// types.rs - OCS-GCS 통신을 위한 표준화된 메시지 타입
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use std::fmt;

/// 공통 메시지 헤더
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageHeader {
    pub timestamp: DateTime<Utc>,
    pub source: String,        // "ocs" 또는 "gcs"
    pub sequence: u64,         // 메시지 순서 번호
    pub message_type: String,  // "alert", "command", "telemetry", "status"
}

/// OCS에서 발행하는 알림 메시지
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertMessage {
    pub header: MessageHeader,
    pub alert_type: String,    // "ThermalLoss", "SensorFailure", "SystemError"
    pub severity: AlertSeverity,
    pub description: String,
    pub sensor_id: Option<String>,
}

/// 알림 심각도 레벨
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

/// GCS에서 OCS로 보내는 명령 메시지
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandMessage {
    pub header: MessageHeader,
    pub command_type: CommandType,
    pub parameters: serde_json::Value,
    pub priority: CommandPriority,
}

/// 명령 타입
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CommandType {
    SystemStatus,
    SensorData,
    FaultInjection,
    SystemReset,
    Calibration,
    Custom(String),
}

impl fmt::Display for CommandType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommandType::SystemStatus => write!(f, "system_status"),
            CommandType::SensorData => write!(f, "sensor_data"),
            CommandType::FaultInjection => write!(f, "fault_injection"),
            CommandType::SystemReset => write!(f, "system_reset"),
            CommandType::Calibration => write!(f, "calibration"),
            CommandType::Custom(s) => write!(f, "{}", s),
        }
    }
}

/// 명령 우선순위
#[derive(Debug, Clone, Serialize, Deserialize, Copy)]
pub enum CommandPriority {
    Low,
    Normal,
    High,
    Emergency,
}

/// OCS에서 GCS로 보내는 텔레메트리 메시지
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryMessage {
    pub header: MessageHeader,
    pub sensor_type: String,   // "thermal", "gyro", "camera"
    pub value: f64,
    pub unit: String,
    pub status: SensorStatus,
}

/// 센서 상태
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SensorStatus {
    Normal,
    Warning,
    Error,
    Offline,
}

/// GCS에서 OCS로 보내는 응답 메시지
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseMessage {
    pub header: MessageHeader,
    pub original_message_id: u64,
    pub response_type: ResponseType,
    pub data: serde_json::Value,
    pub success: bool,
    pub error_message: Option<String>,
}

/// 응답 타입
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResponseType {
    Acknowledgment,
    Data,
    Error,
    Status,
}

/// 시스템 상태 정보
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStatus {
    pub header: MessageHeader,
    pub cpu_usage: f64,
    pub memory_usage: f64,
    pub temperature: f64,
    pub uptime: u64,
    pub active_sensors: Vec<String>,
    pub alerts: Vec<AlertMessage>,
}

impl Default for MessageHeader {
    fn default() -> Self {
        Self {
            timestamp: Utc::now(),
            source: "gcs".to_string(),
            sequence: 0,
            message_type: "unknown".to_string(),
        }
    }
}

impl MessageHeader {
    pub fn new(source: &str, message_type: &str, sequence: u64) -> Self {
        Self {
            timestamp: Utc::now(),
            source: source.to_string(),
            sequence,
            message_type: message_type.to_string(),
        }
    }
}

impl AlertMessage {
    pub fn new(alert_type: &str, severity: AlertSeverity, description: &str) -> Self {
        Self {
            header: MessageHeader::new("ocs", "alert", 0),
            alert_type: alert_type.to_string(),
            severity,
            description: description.to_string(),
            sensor_id: None,
        }
    }
}

impl CommandMessage {
    pub fn new(command_type: CommandType, parameters: serde_json::Value, priority: CommandPriority) -> Self {
        Self {
            header: MessageHeader::new("gcs", "command", 0),
            command_type,
            parameters,
            priority,
        }
    }
}

impl TelemetryMessage {
    pub fn new(sensor_type: &str, value: f64, unit: &str, status: SensorStatus) -> Self {
        Self {
            header: MessageHeader::new("ocs", "telemetry", 0),
            sensor_type: sensor_type.to_string(),
            value,
            unit: unit.to_string(),
            status,
        }
    }
}

impl ResponseMessage {
    pub fn new(original_message_id: u64, response_type: ResponseType, data: serde_json::Value, success: bool) -> Self {
        Self {
            header: MessageHeader::new("gcs", "response", 0),
            original_message_id,
            response_type,
            data,
            success,
            error_message: None,
        }
    }
}
