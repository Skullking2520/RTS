// metrics.rs - Metrics System for OCS-GCS Communication
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;
use serde_json::Value;

/// Communication metrics counters
#[derive(Debug, Clone)]
pub struct CommunicationMetrics {
    // Telemetry related
    pub telemetry_received: Arc<AtomicU64>,
    pub telemetry_processed: Arc<AtomicU64>,
    pub telemetry_errors: Arc<AtomicU64>,
    
    // Command related
    pub commands_received: Arc<AtomicU64>,
    pub commands_processed: Arc<AtomicU64>,
    pub command_errors: Arc<AtomicU64>,
    
    // Alert related
    pub alerts_received: Arc<AtomicU64>,
    pub alerts_critical: Arc<AtomicU64>,
    pub alerts_error: Arc<AtomicU64>,
    pub alerts_warning: Arc<AtomicU64>,
    pub alerts_info: Arc<AtomicU64>,
    
    // Response related
    pub responses_sent: Arc<AtomicU64>,
    pub responses_success: Arc<AtomicU64>,
    pub responses_error: Arc<AtomicU64>,
    
    // Other
    pub unknown_messages: Arc<AtomicU64>,
    pub connection_errors: Arc<AtomicU64>,
    pub reconnection_attempts: Arc<AtomicU64>,
}

/// Performance statistics
#[derive(Debug, Clone)]
pub struct PerformanceStats {
    pub message_processing_time_ns: Arc<AtomicU64>,
    pub message_count: Arc<AtomicU64>,
    pub last_report_time: Arc<Mutex<chrono::DateTime<chrono::Utc>>>,
}

/// Integrated metrics system
#[derive(Debug, Clone)]
pub struct Metrics {
    pub communication: CommunicationMetrics,
    pub performance: PerformanceStats,
    pub system_info: Arc<Mutex<SystemInfo>>,
}

/// System information
#[derive(Debug, Clone)]
pub struct SystemInfo {
    pub start_time: chrono::DateTime<chrono::Utc>,
    pub version: String,
    pub uptime: std::time::Duration,
    pub amqp_connected: bool,
    pub last_heartbeat: chrono::DateTime<chrono::Utc>,
}

impl Default for CommunicationMetrics {
    fn default() -> Self {
        Self {
            telemetry_received: Arc::new(AtomicU64::new(0)),
            telemetry_processed: Arc::new(AtomicU64::new(0)),
            telemetry_errors: Arc::new(AtomicU64::new(0)),
            
            commands_received: Arc::new(AtomicU64::new(0)),
            commands_processed: Arc::new(AtomicU64::new(0)),
            command_errors: Arc::new(AtomicU64::new(0)),
            
            alerts_received: Arc::new(AtomicU64::new(0)),
            alerts_critical: Arc::new(AtomicU64::new(0)),
            alerts_error: Arc::new(AtomicU64::new(0)),
            alerts_warning: Arc::new(AtomicU64::new(0)),
            alerts_info: Arc::new(AtomicU64::new(0)),
            
            responses_sent: Arc::new(AtomicU64::new(0)),
            responses_success: Arc::new(AtomicU64::new(0)),
            responses_error: Arc::new(AtomicU64::new(0)),
            
            unknown_messages: Arc::new(AtomicU64::new(0)),
            connection_errors: Arc::new(AtomicU64::new(0)),
            reconnection_attempts: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Default for PerformanceStats {
    fn default() -> Self {
        Self {
            message_processing_time_ns: Arc::new(AtomicU64::new(0)),
            message_count: Arc::new(AtomicU64::new(0)),
            last_report_time: Arc::new(Mutex::new(chrono::Utc::now())),
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            communication: CommunicationMetrics::default(),
            performance: PerformanceStats::default(),
            system_info: Arc::new(Mutex::new(SystemInfo {
                start_time: chrono::Utc::now(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                uptime: std::time::Duration::from_secs(0),
                amqp_connected: false,
                last_heartbeat: chrono::Utc::now(),
            })),
        }
    }
}

impl CommunicationMetrics {
    // Telemetry metrics
    pub fn increment_telemetry_received(&self) {
        self.telemetry_received.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn increment_telemetry_processed(&self) {
        self.telemetry_processed.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn increment_telemetry_errors(&self) {
        self.telemetry_errors.fetch_add(1, Ordering::Relaxed);
    }
    
    // Command metrics
    pub fn increment_commands_received(&self) {
        self.commands_received.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn increment_commands_processed(&self) {
        self.commands_processed.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn increment_command_errors(&self) {
        self.command_errors.fetch_add(1, Ordering::Relaxed);
    }
    
    // Alert metrics
    pub fn increment_alerts_received(&self) {
        self.alerts_received.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn increment_alerts_critical(&self) {
        self.alerts_critical.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn increment_alerts_error(&self) {
        self.alerts_error.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn increment_alerts_warning(&self) {
        self.alerts_warning.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn increment_alerts_info(&self) {
        self.alerts_info.fetch_add(1, Ordering::Relaxed);
    }
    
    // Response metrics
    pub fn increment_responses_sent(&self) {
        self.responses_sent.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn increment_responses_success(&self) {
        self.responses_success.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn increment_responses_error(&self) {
        self.responses_error.fetch_add(1, Ordering::Relaxed);
    }
    
    // Other metrics
    pub fn increment_unknown_messages(&self) {
        self.unknown_messages.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn increment_connection_errors(&self) {
        self.connection_errors.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn increment_reconnection_attempts(&self) {
        self.reconnection_attempts.fetch_add(1, Ordering::Relaxed);
    }
    
    // Get metric values
    pub fn get_telemetry_received(&self) -> u64 {
        self.telemetry_received.load(Ordering::Relaxed)
    }
    
    pub fn get_telemetry_processed(&self) -> u64 {
        self.telemetry_processed.load(Ordering::Relaxed)
    }
    
    pub fn get_telemetry_errors(&self) -> u64 {
        self.telemetry_errors.load(Ordering::Relaxed)
    }
    
    pub fn get_commands_received(&self) -> u64 {
        self.commands_received.load(Ordering::Relaxed)
    }
    
    pub fn get_commands_processed(&self) -> u64 {
        self.commands_processed.load(Ordering::Relaxed)
    }
    
    pub fn get_command_errors(&self) -> u64 {
        self.command_errors.load(Ordering::Relaxed)
    }
    
    pub fn get_alerts_received(&self) -> u64 {
        self.alerts_received.load(Ordering::Relaxed)
    }
    
    pub fn get_responses_sent(&self) -> u64 {
        self.responses_sent.load(Ordering::Relaxed)
    }
    
    pub fn get_unknown_messages(&self) -> u64 {
        self.unknown_messages.load(Ordering::Relaxed)
    }
}

impl PerformanceStats {
    pub fn record_processing_time(&self, time_ns: u64) {
        self.message_processing_time_ns.fetch_add(time_ns, Ordering::Relaxed);
        self.message_count.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn get_average_processing_time_ns(&self) -> u64 {
        let total_time = self.message_processing_time_ns.load(Ordering::Relaxed);
        let count = self.message_count.load(Ordering::Relaxed);
        
        if count > 0 {
            total_time / count
        } else {
            0
        }
    }
    
    pub fn get_message_count(&self) -> u64 {
        self.message_count.load(Ordering::Relaxed)
    }
}

impl Metrics {
    /// Generate report
    pub async fn generate_report(&self) -> Value {
        let mut report = serde_json::Map::new();
        
        // Communication metrics
        let mut comm_metrics = serde_json::Map::new();
        comm_metrics.insert("telemetry_received".to_string(), 
                           serde_json::Value::Number(self.communication.get_telemetry_received().into()));
        comm_metrics.insert("telemetry_processed".to_string(), 
                           serde_json::Value::Number(self.communication.get_telemetry_processed().into()));
        comm_metrics.insert("telemetry_errors".to_string(), 
                           serde_json::Value::Number(self.communication.get_telemetry_errors().into()));
        
        comm_metrics.insert("commands_received".to_string(), 
                           serde_json::Value::Number(self.communication.get_commands_received().into()));
        comm_metrics.insert("commands_processed".to_string(), 
                           serde_json::Value::Number(self.communication.get_commands_processed().into()));
        comm_metrics.insert("command_errors".to_string(), 
                           serde_json::Value::Number(self.communication.get_command_errors().into()));
        
        comm_metrics.insert("alerts_received".to_string(), 
                           serde_json::Value::Number(self.communication.get_alerts_received().into()));
        comm_metrics.insert("responses_sent".to_string(), 
                           serde_json::Value::Number(self.communication.get_responses_sent().into()));
        comm_metrics.insert("unknown_messages".to_string(), 
                           serde_json::Value::Number(self.communication.get_unknown_messages().into()));
        
        report.insert("communication".to_string(), serde_json::Value::Object(comm_metrics));
        
        // Performance metrics
        let mut perf_metrics = serde_json::Map::new();
        perf_metrics.insert("average_processing_time_ns".to_string(), 
                           serde_json::Value::Number(self.performance.get_average_processing_time_ns().into()));
        perf_metrics.insert("total_messages".to_string(), 
                           serde_json::Value::Number(self.performance.get_message_count().into()));
        
        report.insert("performance".to_string(), serde_json::Value::Object(perf_metrics));
        
        // System information
        let system_info = self.system_info.lock().await;
        let mut sys_info = serde_json::Map::new();
        sys_info.insert("start_time".to_string(), 
                       serde_json::Value::String(system_info.start_time.to_rfc3339()));
        sys_info.insert("version".to_string(), 
                       serde_json::Value::String(system_info.version.clone()));
        sys_info.insert("uptime_seconds".to_string(), 
                       serde_json::Value::Number(system_info.uptime.as_secs().into()));
        sys_info.insert("amqp_connected".to_string(), 
                       serde_json::Value::Bool(system_info.amqp_connected));
        sys_info.insert("last_heartbeat".to_string(), 
                       serde_json::Value::String(system_info.last_heartbeat.to_rfc3339()));
        
        report.insert("system".to_string(), serde_json::Value::Object(sys_info));
        
        // Timestamp
        report.insert("timestamp".to_string(), 
                     serde_json::Value::String(chrono::Utc::now().to_rfc3339()));
        
        serde_json::Value::Object(report)
    }
    
    /// Print report
    pub async fn report(&self) {
        let report = self.generate_report().await;
        info!("Metrics Report:\n{}", serde_json::to_string_pretty(&report).unwrap());
    }
    
    /// Update system information
    pub async fn update_system_info(&self, amqp_connected: bool) {
        let mut system_info = self.system_info.lock().await;
        system_info.amqp_connected = amqp_connected;
        system_info.last_heartbeat = chrono::Utc::now();
        system_info.uptime = chrono::Utc::now().signed_duration_since(system_info.start_time).to_std().unwrap_or_default();
    }
    
    /// Update connection status
    pub async fn update_connection_status(&self, connected: bool) {
        if !connected {
            self.communication.increment_connection_errors();
        }
        self.update_system_info(connected).await;
    }
}
