// types.rs - Standardized message types for OCS-GCS communication
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use std::fmt;

/// Common message header
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageHeader {
    pub timestamp: DateTime<Utc>,
    pub source: String,        // "ocs" or "gcs"
    pub sequence: u64,         // Message sequence number
    pub message_type: String,  // "alert", "command", "telemetry", "status"
}

/// Alert message published by OCS
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertMessage {
    pub header: MessageHeader,
    pub alert_type: String,    // "ThermalLoss", "SensorFailure", "SystemError"
    pub severity: AlertSeverity,
    pub description: String,
    pub sensor_id: Option<String>,
}

/// Alert severity level
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

/// Command message sent from GCS to OCS
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandMessage {
    pub header: MessageHeader,
    pub command_type: CommandType,
    pub parameters: serde_json::Value,
    pub priority: CommandPriority,
}

/// Command type
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

/// Command priority
#[derive(Debug, Clone, Serialize, Deserialize, Copy)]
pub enum CommandPriority {
    Low,
    Normal,
    High,
    Emergency,
}

/// Telemetry message sent from OCS to GCS
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryMessage {
    pub header: MessageHeader,
    pub sensor_type: String,   // "thermal", "gyro", "camera"
    pub value: f64,
    pub unit: String,
    pub status: SensorStatus,
}

/// Sensor status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SensorStatus {
    Normal,
    Warning,
    Error,
    Offline,
}

/// Response message sent from GCS to OCS
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseMessage {
    pub header: MessageHeader,
    pub original_message_id: u64,
    pub response_type: ResponseType,
    pub data: serde_json::Value,
    pub success: bool,
    pub error_message: Option<String>,
}

/// Response type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResponseType {
    Acknowledgment,
    Data,
    Error,
    Status,
}

/// System status information
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
