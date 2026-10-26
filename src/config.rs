// config.rs - Configuration for OCS-GCS Communication
use std::time::Duration;

/// GCS System Configuration
#[derive(Clone, Debug)]
pub struct Config {
    // AMQP connection settings
    pub amqp_uri: String,
    
    // Exchange and queue settings
    pub telem_exchange: String,
    pub telem_binding: String,
    pub telem_queue: String,
    pub cmd_queue: String,
    
    // Performance settings
    pub expected_period: Duration,
    pub metrics_period: Duration,
    pub decode_budget: Duration,
    pub dispatch_budget: Duration,
    
    // Connection settings
    pub loc_gap_threshold: u32,
    pub loc_timeout: Duration,
    
    // Logging settings
    pub log_level: String,
    pub log_file: Option<String>,
}

impl Config {
    /// Load configuration from environment variables
    pub fn load() -> Self {
        // AMQP settings
        let amqp_uri = std::env::var("AMQP_URI")
            .unwrap_or_else(|_| "amqp://guest:guest@127.0.0.1:5672/%2f".into());
        
        // Exchange and queue settings
        let telem_exchange = std::env::var("TELEM_EXCHANGE")
            .unwrap_or_else(|_| "telemetry".into());
        let telem_binding = std::env::var("TELEM_BINDING")
            .unwrap_or_else(|_| "ocs.*".into());
        let telem_queue = std::env::var("TELEM_QUEUE")
            .unwrap_or_else(|_| "gcs_telem".into());
        let cmd_queue = std::env::var("CMD_QUEUE")
            .unwrap_or_else(|_| "ocs_cmd".into());
        
        // Performance settings
        let expected_period = std::env::var("EXPECTED_PERIOD_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or(Duration::from_millis(20));
        
        let metrics_period = std::env::var("METRICS_PERIOD_SEC")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(10));
        
        let decode_budget = std::env::var("DECODE_BUDGET_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or(Duration::from_millis(5));
        
        let dispatch_budget = std::env::var("DISPATCH_BUDGET_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or(Duration::from_millis(10));
        
        // Connection settings
        let loc_gap_threshold = std::env::var("LOC_GAP_THRESHOLD")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(3);
        
        let loc_timeout = std::env::var("LOC_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or_else(|| expected_period * 10);
        
        // Logging settings
        let log_level = std::env::var("LOG_LEVEL")
            .unwrap_or_else(|_| "info".into());
        let log_file = std::env::var("LOG_FILE").ok();
        
        Self {
            amqp_uri,
            telem_exchange,
            telem_binding,
            telem_queue,
            cmd_queue,
            expected_period,
            metrics_period,
            decode_budget,
            dispatch_budget,
            loc_gap_threshold,
            loc_timeout,
            log_level,
            log_file,
        }
    }
    
    /// Print configuration summary
    pub fn print_summary(&self) {
        tracing::info!("GCS Configuration Summary:");
        tracing::info!("   AMQP URI configured (credentials omitted)");
        tracing::info!("   Telemetry Exchange: {}", self.telem_exchange);
        tracing::info!("   Binding Pattern: {}", self.telem_binding);
        tracing::info!("   Telemetry Queue: {}", self.telem_queue);
        tracing::info!("   Command Queue: {}", self.cmd_queue);
        tracing::info!("   Expected Period: {}ms", self.expected_period.as_millis());
        tracing::info!("   Metrics Period: {}s", self.metrics_period.as_secs());
        tracing::info!("   Decode Budget: {}ms", self.decode_budget.as_millis());
        tracing::info!("   Dispatch Budget: {}ms", self.dispatch_budget.as_millis());
        tracing::info!("   Disconnection Gap Threshold: {}", self.loc_gap_threshold);
        tracing::info!("   Disconnection Timeout: {}ms", self.loc_timeout.as_millis());
        tracing::info!("   Log Level: {}", self.log_level);
        if let Some(ref log_file) = self.log_file {
            tracing::info!("   Log File: {}", log_file);
        }
    }
    
    /// Default configuration for development
    pub fn development() -> Self {
        Self {
            amqp_uri: "amqp://guest:guest@127.0.0.1:5672/%2f".into(),
            telem_exchange: "telemetry".into(),
            telem_binding: "ocs.*".into(),
            telem_queue: "gcs_telem".into(),
            cmd_queue: "ocs_cmd".into(),
            expected_period: Duration::from_millis(20),
            metrics_period: Duration::from_secs(10),
            decode_budget: Duration::from_millis(5),
            dispatch_budget: Duration::from_millis(10),
            loc_gap_threshold: 3,
            loc_timeout: Duration::from_millis(200),
            log_level: "debug".into(),
            log_file: None,
        }
    }
    
    /// Default configuration for production
    pub fn production() -> Self {
        Self {
            amqp_uri: "amqp://guest:guest@127.0.0.1:5672/%2f".into(),
            telem_exchange: "telemetry".into(),
            telem_binding: "ocs.*".into(),
            telem_queue: "gcs_telem".into(),
            cmd_queue: "ocs_cmd".into(),
            expected_period: Duration::from_millis(20),
            metrics_period: Duration::from_secs(60),
            decode_budget: Duration::from_millis(3),
            dispatch_budget: Duration::from_millis(5),
            loc_gap_threshold: 5,
            loc_timeout: Duration::from_millis(500),
            log_level: "info".into(),
            log_file: Some("/var/log/gcs/gcs.log".into()),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::development()
    }
}
