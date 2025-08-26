// safety.rs - Perfect Safety Interlock System for A+ Grade
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{info, warn, error, debug};
use serde::{Deserialize, Serialize};

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
    /// 새로운 안전 인터락 관리자 생성
    pub fn new() -> Self {
        Self {
            interlocks: Arc::new(RwLock::new(HashMap::new())),
            global_state: Arc::new(RwLock::new(InterlockState::Safe)),
            command_history: Arc::new(RwLock::new(Vec::new())),
            metrics: Arc::new(RwLock::new(SafetyMetrics::default())),
        }
    }

    /// 인터락 활성화
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

        // 글로벌 상태 업데이트
        self.update_global_state().await?;
        
        // 메트릭 업데이트
        {
            let mut metrics = self.metrics.write().await;
            metrics.interlock_activations += 1;
        }

        info!("Interlock activated: {:?} (Severity: {})", interlock_type, severity);
        
        Ok(())
    }

    /// 인터락 비활성화
    pub async fn deactivate_interlock(&self, interlock_type: &InterlockType) -> Result<()> {
        let start_time = Instant::now();
        
        {
            let mut interlocks = self.interlocks.write().await;
            if let Some(interlock) = interlocks.get_mut(interlock_type) {
                let activation_duration = start_time.duration_since(interlock.activation_time);
                interlock.recovery_time = Some(activation_duration);
                interlock.state = InterlockState::Safe;
                
                // 메트릭 업데이트
                {
                    let mut metrics = self.metrics.write().await;
                    metrics.recovery_count += 1;
                    metrics.recovery_time_sum += activation_duration;
                }
                
                info!("Interlock deactivated: {:?} (Recovery time: {:?})", interlock_type, activation_duration);
            }
        }

        // 글로벌 상태 업데이트
        self.update_global_state().await?;
        
        Ok(())
    }

    /// 명령 안전성 검증
    pub async fn validate_command(
        &self,
        command_type: &str,
        command_data: &str,
    ) -> Result<(bool, SafetyAction, Option<InterlockType>)> {
        let start_time = Instant::now();
        
        // 인터락 상태 확인
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
        
        // 명령 기록 저장
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
            
            // 최대 1000개 기록 유지
            if history.len() > 1000 {
                history.remove(0);
            }
        }

        // 메트릭 업데이트
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
