// safety.rs - Advanced Safety Interlock System
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{info, warn, error, debug};
use serde::{Deserialize, Serialize};
use crate::performance::PerformanceMonitor;

/// Safety Interlock State
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum InterlockState {
    Safe,           // Safe state
    Warning,        // Warning state
    Critical,       // Critical state
    Emergency,      // Emergency state
    Locked,         // Complete lock state
}

/// Interlock Type
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum InterlockType {
    Thermal,        // Thermal management
    Power,          // Power system
    Communication,  // Communication system
    Attitude,       // Attitude control
    Propulsion,     // Propulsion system
    Sensor,         // Sensor system
    Custom(String), // Custom interlock
}

/// Interlock Information
#[derive(Debug, Clone)]
pub struct InterlockInfo {
    pub interlock_type: InterlockType,
    pub state: InterlockState,
    pub activation_time: Instant,
    pub recovery_time: Option<Duration>,
    pub fault_description: String,
    pub severity: u8, // 1-10, 10 is most severe
    pub affected_commands: Vec<String>,
    pub auto_recovery: bool,
}

/// Safety Interlock Manager
#[derive(Debug, Clone)]
pub struct SafetyInterlockManager {
    interlocks: Arc<RwLock<HashMap<InterlockType, InterlockInfo>>>,
    global_state: Arc<RwLock<InterlockState>>,
    command_history: Arc<RwLock<Vec<CommandSafetyRecord>>>,
    metrics: Arc<RwLock<SafetyMetrics>>,
    performance_monitor: Option<PerformanceMonitor>,
}

/// Command safety record
#[derive(Debug, Clone)]
pub struct CommandSafetyRecord {
    pub timestamp: Instant,
    pub command_type: String,
    pub command_data: String,
    pub interlock_triggered: Option<InterlockType>,
    pub action_taken: SafetyAction,
    pub response_time: Duration,
}

/// Safety action
#[derive(Debug, Clone)]
pub enum SafetyAction {
    Allowed,        // Allowed
    Blocked,        // Blocked
    Delayed,        // Delayed
    Modified,       // Modified
    Emergency,      // Emergency
}

/// Safety metrics
#[derive(Debug, Clone)]
pub struct SafetyMetrics {
    pub total_commands: u64,
    pub blocked_commands: u64,
    pub delayed_commands: u64,
    pub emergency_actions: u64,
    pub average_response_time: Duration,
    pub interlock_activations: u64,
    pub recovery_time_sum: Duration,
    pub recovery_count: u64,
}

impl Default for SafetyMetrics {
    fn default() -> Self {
        Self {
            total_commands: 0,
            blocked_commands: 0,
            delayed_commands: 0,
            emergency_actions: 0,
            average_response_time: Duration::from_millis(0),
            interlock_activations: 0,
            recovery_time_sum: Duration::from_millis(0),
            recovery_count: 0,
        }
    }
}

impl SafetyInterlockManager {
    /// Create new safety interlock manager
    pub fn new() -> Self {
        Self {
            interlocks: Arc::new(RwLock::new(HashMap::new())),
            global_state: Arc::new(RwLock::new(InterlockState::Safe)),
            command_history: Arc::new(RwLock::new(Vec::new())),
            metrics: Arc::new(RwLock::new(SafetyMetrics::default())),
            performance_monitor: None,
        }
    }
    
    /// Create new safety interlock manager with performance monitor
    pub fn new_with_performance_monitor(performance_monitor: Option<PerformanceMonitor>) -> Self {
        Self {
            interlocks: Arc::new(RwLock::new(HashMap::new())),
            global_state: Arc::new(RwLock::new(InterlockState::Safe)),
            command_history: Arc::new(RwLock::new(Vec::new())),
            metrics: Arc::new(RwLock::new(SafetyMetrics::default())),
            performance_monitor,
        }
    }

    /// Activate interlock
    pub async fn activate_interlock(
        &self,
        interlock_type: InterlockType,
        fault_description: String,
        severity: u8,
        affected_commands: Vec<String>,
    ) -> Result<()> {
        let start_time = Instant::now();
        
        let interlock_info = InterlockInfo {
            interlock_type: interlock_type.clone(),
            state: match severity {
                1..=3 => InterlockState::Warning,
                4..=6 => InterlockState::Critical,
                7..=8 => InterlockState::Emergency,
                9..=10 => InterlockState::Locked,
                _ => InterlockState::Warning,
            },
            activation_time: start_time,
            recovery_time: None,
            fault_description,
            severity,
            affected_commands,
            auto_recovery: severity < 7,
        };

        {
            let mut interlocks = self.interlocks.write().await;
            interlocks.insert(interlock_type.clone(), interlock_info);
        }

        // Update global state
        self.update_global_state().await?;
        
        // Update metrics
        {
            let mut metrics = self.metrics.write().await;
            metrics.interlock_activations += 1;
        }

        info!("Interlock activated: {:?} (Severity: {})", interlock_type, severity);
        
        Ok(())
    }

    /// Deactivate interlock
    pub async fn deactivate_interlock(&self, interlock_type: &InterlockType) -> Result<()> {
        let start_time = Instant::now();
        
        {
            let mut interlocks = self.interlocks.write().await;
            if let Some(interlock) = interlocks.get_mut(interlock_type) {
                let activation_duration = start_time.duration_since(interlock.activation_time);
                interlock.recovery_time = Some(activation_duration);
                interlock.state = InterlockState::Safe;
                
                // Update metrics
                {
                    let mut metrics = self.metrics.write().await;
                    metrics.recovery_count += 1;
                    metrics.recovery_time_sum += activation_duration;
                }
                
                info!("Interlock deactivated: {:?} (Recovery time: {:?})", interlock_type, activation_duration);
            }
        }

        // Update global state
        self.update_global_state().await?;
        
        Ok(())
    }

    /// Validate command safety
    pub async fn validate_command(
        &self,
        command_type: &str,
        command_data: &str,
    ) -> Result<(bool, SafetyAction, Option<InterlockType>)> {
        let start_time = Instant::now();
        let interlock_detection_start = Instant::now();
        
        // Check interlock status
        let interlock_check = self.check_interlocks_for_command(command_type).await;
        
        let (allowed, action, triggered_interlock) = match interlock_check {
            None => (true, SafetyAction::Allowed, None),
            Some(interlock) => {
                match interlock.state {
                    InterlockState::Safe => (true, SafetyAction::Allowed, None),
                    InterlockState::Warning => (true, SafetyAction::Delayed, Some(interlock.interlock_type.clone())),
                    InterlockState::Critical => (false, SafetyAction::Blocked, Some(interlock.interlock_type.clone())),
                    InterlockState::Emergency => (false, SafetyAction::Emergency, Some(interlock.interlock_type.clone())),
                    InterlockState::Locked => (false, SafetyAction::Blocked, Some(interlock.interlock_type.clone())),
                }
            }
        };

        let response_time = start_time.elapsed();
        let interlock_latency = interlock_detection_start.elapsed();
        
        // Log interlock latency (detection to command block)
        if matches!(action, SafetyAction::Blocked | SafetyAction::Emergency) {
            info!("METRICS: Interlock latency: {:?} (detection to command block)", interlock_latency);
        }
        
        // Check for critical ground alert (>100ms response time)
        if response_time > Duration::from_millis(100) {
            error!("CRITICAL GROUND ALERT: Fault response time exceeded 100ms threshold: {:?}", response_time);
            self.trigger_critical_ground_alert(response_time).await;
        }
        
        // Store command record
        let record = CommandSafetyRecord {
            timestamp: start_time,
            command_type: command_type.to_string(),
            command_data: command_data.to_string(),
            interlock_triggered: triggered_interlock.clone(),
            action_taken: action.clone(),
            response_time,
        };

        {
            let mut history = self.command_history.write().await;
            history.push(record);
            
            // Keep maximum 1000 records
            if history.len() > 1000 {
                history.remove(0);
            }
        }

        // Update metrics
        {
            let mut metrics = self.metrics.write().await;
            metrics.total_commands += 1;
            
            match action {
                SafetyAction::Blocked => metrics.blocked_commands += 1,
                SafetyAction::Delayed => metrics.delayed_commands += 1,
                SafetyAction::Emergency => metrics.emergency_actions += 1,
                _ => {}
            }
            
            // Update average response time
            let total_time = if metrics.total_commands > 1 {
                let prev_avg_nanos = metrics.average_response_time.as_nanos() as u64;
                let total_nanos = prev_avg_nanos * (metrics.total_commands - 1) + response_time.as_nanos() as u64;
                Duration::from_nanos(total_nanos / metrics.total_commands)
            } else {
                response_time
            };
            metrics.average_response_time = total_time;
        }

        // Record fault recovery time
        if let Some(performance_monitor) = &self.performance_monitor {
            if matches!(action, SafetyAction::Blocked | SafetyAction::Emergency) {
                performance_monitor.record_fault_recovery(response_time).await;
            }
        }
        
        // Logging
        match action {
            SafetyAction::Allowed => {
                debug!("Command allowed: {} (Response time: {:?})", command_type, response_time);
            }
            SafetyAction::Delayed => {
                warn!("Command delayed: {} (Interlock: {:?})", command_type, triggered_interlock);
            }
            SafetyAction::Blocked => {
                error!("Command blocked: {} (Interlock: {:?})", command_type, triggered_interlock);
            }
            SafetyAction::Emergency => {
                error!("Emergency command blocked: {} (Interlock: {:?})", command_type, triggered_interlock);
            }
            _ => {}
        }

        Ok((allowed, action, triggered_interlock))
    }

    /// Check interlocks for command
    async fn check_interlocks_for_command(&self, command_type: &str) -> Option<InterlockInfo> {
        let interlocks = self.interlocks.read().await;
        
        for (_, interlock) in interlocks.iter() {
            if interlock.affected_commands.iter().any(|cmd| cmd == command_type) {
                return Some(interlock.clone());
            }
        }
        
        None
    }

    /// Update global state
    async fn update_global_state(&self) -> Result<()> {
        let interlocks = self.interlocks.read().await;
        
        let mut highest_severity = 0u8;
        let mut has_emergency = false;
        let mut has_locked = false;
        
        for (_, interlock) in interlocks.iter() {
            if interlock.state == InterlockState::Emergency {
                has_emergency = true;
            }
            if interlock.state == InterlockState::Locked {
                has_locked = true;
            }
            highest_severity = highest_severity.max(interlock.severity);
        }
        
        let new_global_state = if has_locked {
            InterlockState::Locked
        } else if has_emergency {
            InterlockState::Emergency
        } else if highest_severity >= 7 {
            InterlockState::Critical
        } else if highest_severity >= 4 {
            InterlockState::Warning
        } else {
            InterlockState::Safe
        };
        
        {
            let mut global_state = self.global_state.write().await;
            *global_state = new_global_state;
        }
        
        Ok(())
    }

    /// Generate safety report
    pub async fn generate_safety_report(&self) -> serde_json::Value {
        let interlocks = self.interlocks.read().await;
        let global_state = self.global_state.read().await;
        let metrics = self.metrics.read().await;
        
        let mut report = serde_json::Map::new();
        
        // Global state
        report.insert("global_state".to_string(), 
                     serde_json::Value::String(format!("{:?}", *global_state)));
        
        // Interlock status
        let mut interlock_status = serde_json::Map::new();
        for (interlock_type, info) in interlocks.iter() {
            let mut status = serde_json::Map::new();
            status.insert("state".to_string(), 
                         serde_json::Value::String(format!("{:?}", info.state)));
            status.insert("severity".to_string(), 
                         serde_json::Value::Number(info.severity.into()));
            status.insert("activation_time".to_string(), 
                         serde_json::Value::String(info.activation_time.elapsed().as_millis().to_string()));
            status.insert("fault_description".to_string(), 
                         serde_json::Value::String(info.fault_description.clone()));
            
            interlock_status.insert(format!("{:?}", interlock_type), 
                                  serde_json::Value::Object(status));
        }
        report.insert("interlocks".to_string(), 
                     serde_json::Value::Object(interlock_status));
        
        // Metrics
        let mut metrics_data = serde_json::Map::new();
        metrics_data.insert("total_commands".to_string(), 
                           serde_json::Value::Number(metrics.total_commands.into()));
        metrics_data.insert("blocked_commands".to_string(), 
                           serde_json::Value::Number(metrics.blocked_commands.into()));
        metrics_data.insert("delayed_commands".to_string(), 
                           serde_json::Value::Number(metrics.delayed_commands.into()));
        metrics_data.insert("emergency_actions".to_string(), 
                           serde_json::Value::Number(metrics.emergency_actions.into()));
        metrics_data.insert("average_response_time_ms".to_string(), 
                           serde_json::Value::Number((metrics.average_response_time.as_millis() as u64).into()));
        metrics_data.insert("interlock_activations".to_string(), 
                           serde_json::Value::Number(metrics.interlock_activations.into()));
        
        if metrics.recovery_count > 0 {
            let avg_recovery_time = if metrics.recovery_count > 0 {
                let total_nanos = metrics.recovery_time_sum.as_nanos() as u64;
                Duration::from_nanos(total_nanos / metrics.recovery_count)
            } else {
                Duration::ZERO
            };
            metrics_data.insert("average_recovery_time_ms".to_string(), 
                               serde_json::Value::Number((avg_recovery_time.as_millis() as u64).into()));
        }
        
        report.insert("metrics".to_string(), 
                     serde_json::Value::Object(metrics_data));
        
        // Timestamp
        report.insert("timestamp".to_string(), 
                     serde_json::Value::String(chrono::Utc::now().to_rfc3339()));
        
        serde_json::Value::Object(report)
    }

    /// Auto recovery process
    pub async fn auto_recovery_process(&self) -> Result<()> {
        let mut to_recover = Vec::new();
        
        {
            let interlocks = self.interlocks.read().await;
            for (interlock_type, info) in interlocks.iter() {
                if info.auto_recovery && info.state != InterlockState::Safe {
                    let activation_duration = Instant::now().duration_since(info.activation_time);
                    if activation_duration > Duration::from_secs(60) { // Auto recovery after 60 seconds
                        to_recover.push(interlock_type.clone());
                    }
                }
            }
        }
        
        for interlock_type in to_recover {
            info!("Attempting auto-recovery for: {:?}", interlock_type);
            self.deactivate_interlock(&interlock_type).await?;
        }
        
        Ok(())
    }

    /// Safety state monitoring
    pub async fn safety_monitoring_loop(&self) -> Result<()> {
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        
        loop {
            tick.tick().await;
            
            // Auto recovery process
            self.auto_recovery_process().await?;
            
            // Check global state
            let global_state = self.global_state.read().await;
            match *global_state {
                InterlockState::Emergency | InterlockState::Locked => {
                    error!("Emergency state detected: {:?}", *global_state);
                }
                InterlockState::Critical => {
                    warn!("Critical state: {:?}", *global_state);
                }
                InterlockState::Warning => {
                    info!("Warning state: {:?}", *global_state);
                }
                InterlockState::Safe => {
                    debug!("Safe state: {:?}", *global_state);
                }
            }
        }
    }
}

impl Default for SafetyInterlockManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SafetyInterlockManager {
    /// Simulate real safety interlocks for testing
    pub async fn simulate_safety_interlocks(&self) -> Result<()> {
        let interlock_types = vec![
            (InterlockType::Thermal, InterlockState::Critical, "High temperature detected"),
            (InterlockType::Power, InterlockState::Warning, "Low power warning"),
            (InterlockType::Communication, InterlockState::Emergency, "Communication loss"),
            (InterlockType::Sensor, InterlockState::Critical, "Sensor failure detected"),
        ];
        
        for (interlock_type, state, description) in interlock_types {
            let interlock_info = InterlockInfo {
                interlock_type: interlock_type.clone(),
                state: state.clone(),
                activation_time: Instant::now(),
                recovery_time: None,
                fault_description: description.to_string(),
                severity: match state {
                    InterlockState::Warning => 3,
                    InterlockState::Critical => 7,
                    InterlockState::Emergency => 10,
                    _ => 1,
                },
                affected_commands: vec!["system_reset".to_string(), "fault_injection".to_string()],
                auto_recovery: false,
            };
            
            // Activate interlock
            {
                let mut interlocks = self.interlocks.write().await;
                interlocks.insert(interlock_type.clone(), interlock_info);
            }
            
            info!("Safety interlock activated: {:?} - {}", interlock_type, description);
            
            // Small delay between interlocks
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        
        Ok(())
    }
    
    /// Trigger critical ground alert for response time >100ms
    async fn trigger_critical_ground_alert(&self, response_time: Duration) {
        let alert_message = serde_json::json!({
            "alert": "CriticalGroundAlert",
            "description": format!("Fault response time exceeded 100ms threshold: {:?}", response_time),
            "severity": "Critical",
            "source": "gcs_safety",
            "timestamp": chrono::Utc::now(),
            "response_time_ms": response_time.as_millis(),
            "threshold_ms": 100,
            "action_required": "Immediate ground team response required"
        });
        
        // Log critical alert
        error!("CRITICAL GROUND ALERT: Response time {:?} exceeds 100ms threshold", response_time);
        
        // In a real system, this would trigger emergency procedures
        // For simulation, we just log the alert
        info!("Emergency procedures would be activated for response time: {:?}", response_time);
    }
}
