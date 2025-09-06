// uplink.rs - OCS Command Transmission and Response Processing
use anyhow::Result;
use futures_util::StreamExt;
use lapin::message::Delivery;
use serde_json::Value;
use tokio::sync::mpsc;
use tracing::{info, warn, error, debug};
use std::time::{Duration, Instant};
use std::collections::VecDeque;
use tokio::sync::RwLock;
use std::sync::Arc;
use crate::{
    amqp::{AmqpClient, ack, nack_drop},
    types::{CommandMessage, CommandType, CommandPriority, ResponseMessage, ResponseType},
    metrics::Metrics,
    safety::SafetyInterlockManager,
    performance::PerformanceMonitor,
};

/// Command execution result
#[derive(Debug, Clone)]
enum CommandResult {
    Success(Value),
    Error(String),
}

/// Scheduled command entry
#[derive(Debug, Clone)]
pub struct ScheduledCommand {
    pub command: CommandMessage,
    pub scheduled_time: Instant,
    pub priority: CommandPriority,
    pub deadline: Duration,
}

/// Command Processing Task with real-time scheduling
#[derive(Clone)]
pub struct UplinkTask {
    amqp_client: AmqpClient,
    metrics: Metrics,
    response_tx: mpsc::Sender<ResponseMessage>,
    safety_manager: SafetyInterlockManager,
    performance_monitor: Option<PerformanceMonitor>,
    command_schedule: Arc<RwLock<VecDeque<ScheduledCommand>>>,
}

impl UplinkTask {
    pub fn new(
        amqp_client: AmqpClient,
        metrics: Metrics,
        response_tx: mpsc::Sender<ResponseMessage>,
        performance_monitor: Option<PerformanceMonitor>,
    ) -> Self {
        Self {
            amqp_client,
            metrics,
            response_tx,
            safety_manager: SafetyInterlockManager::new(),
            performance_monitor,
            command_schedule: Arc::new(RwLock::new(VecDeque::new())),
        }
    }
    
    /// Command processing task execution with real-time scheduling
    pub async fn run(&self, queue_name: &str, _budget: std::time::Duration) -> Result<()> {
        info!("Command processing task started: {}", queue_name);
        
        // Create command consumer
        let mut consumer = self.amqp_client.create_command_consumer().await?;
        
        info!("Waiting for command messages...");
        
        // Start real-time command scheduler
        let schedule_handle = self.start_command_scheduler().await;
        
        // Message processing loop
        while let Some(delivery) = consumer.next().await {
            match delivery {
                Ok(delivery) => {
                    if let Err(e) = self.process_command_message(delivery).await {
                        error!("Command message processing failed: {}", e);
                    }
                }
                Err(e) => {
                    error!("Message reception error: {}", e);
                }
            }
        }
        
        // Cleanup
        schedule_handle.abort();
        Ok(())
    }
    
    /// Start real-time command scheduler
    async fn start_command_scheduler(&self) -> tokio::task::JoinHandle<()> {
        let schedule = self.command_schedule.clone();
        let amqp_client = self.amqp_client.clone();
        let metrics = self.metrics.clone();
        
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(1)); // 1ms tick
            
            loop {
                tick.tick().await;
                
                // Process scheduled commands
                let now = Instant::now();
                let mut to_execute = Vec::new();
                
                {
                    let mut schedule_guard = schedule.write().await;
                    while let Some(scheduled_cmd) = schedule_guard.front() {
                        if now >= scheduled_cmd.scheduled_time {
                            to_execute.push(schedule_guard.pop_front().unwrap());
                        } else {
                            break;
                        }
                    }
                }
                
                // Execute scheduled commands
                for scheduled_cmd in to_execute {
                    let execution_start = Instant::now();
                    
                    // Check if command is still within deadline
                    if execution_start.duration_since(scheduled_cmd.scheduled_time) > scheduled_cmd.deadline {
                        warn!("Scheduled command missed deadline: {:?}", scheduled_cmd.command.command_type);
                        metrics.communication.increment_command_errors();
                        continue;
                    }
                    
                    // Execute command
                    info!("Executing scheduled command: {} (priority: {:?})", 
                          scheduled_cmd.command.command_type, scheduled_cmd.priority);
                    
                    // Simulate command execution
                    let result = serde_json::json!({
                        "status": "executed",
                        "timestamp": chrono::Utc::now(),
                        "scheduled_time": scheduled_cmd.scheduled_time.elapsed().as_millis(),
                        "execution_time": execution_start.elapsed().as_millis()
                    });
                    
                    // Publish command to OCS
                    if let Err(e) = amqp_client.publish_command(
                        serde_json::to_string(&result).unwrap().as_bytes()
                    ).await {
                        error!("Failed to publish scheduled command: {}", e);
                    } else {
                        info!("Scheduled command published successfully");
                        metrics.communication.increment_commands_processed();
                    }
                }
            }
        })
    }
    
    /// Schedule command for real-time execution
    pub async fn schedule_command(&self, command: CommandMessage, delay: Duration, priority: CommandPriority) -> Result<()> {
        let scheduled_time = Instant::now() + delay;
        let deadline = match priority {
            CommandPriority::Emergency => Duration::from_millis(2),
            CommandPriority::High => Duration::from_millis(10),
            CommandPriority::Normal => Duration::from_millis(100),
            CommandPriority::Low => Duration::from_millis(1000),
        };
        
        let scheduled_cmd = ScheduledCommand {
            command,
            scheduled_time,
            priority,
            deadline,
        };
        
        {
            let mut schedule = self.command_schedule.write().await;
            schedule.push_back(scheduled_cmd);
            
            // Sort by scheduled time and priority
            schedule.make_contiguous().sort_by(|a, b| {
                a.scheduled_time.cmp(&b.scheduled_time)
                    .then_with(|| {
                        match (&a.priority, &b.priority) {
                            (CommandPriority::Emergency, CommandPriority::Emergency) => std::cmp::Ordering::Equal,
                            (CommandPriority::Emergency, _) => std::cmp::Ordering::Less,
                            (_, CommandPriority::Emergency) => std::cmp::Ordering::Greater,
                            (CommandPriority::High, CommandPriority::High) => std::cmp::Ordering::Equal,
                            (CommandPriority::High, _) => std::cmp::Ordering::Less,
                            (_, CommandPriority::High) => std::cmp::Ordering::Greater,
                            _ => std::cmp::Ordering::Equal,
                        }
                    })
            });
        }
        
        info!("Command scheduled for execution in {:?} (priority: {:?})", delay, priority);
        Ok(())
    }
    
    /// Simulate real command messages for testing
    pub async fn simulate_command_messages(&self) -> Result<()> {
        let command_types = vec![
            ("system_status", "get_status"),
            ("sensor_data", "get_thermal"),
            ("sensor_data", "get_gyro"),
            ("calibration", "calibrate_sensors"),
            ("fault_injection", "inject_test_fault"),
        ];
        
        for (command_type, action) in command_types {
            // Simulate command processing directly
            let start_time = Instant::now();
            
            // Parse command
            let _command_message = serde_json::json!({
                "type": command_type,
                "action": action,
                "timestamp": chrono::Utc::now(),
                "priority": "normal"
            });
            
            // Simulate command execution
            let _result = CommandResult::Success(serde_json::json!({
                "status": "success",
                "command": command_type,
                "action": action,
                "timestamp": chrono::Utc::now()
            }));
            let execution_time = start_time.elapsed();
            
            // Record task execution time
            if let Some(performance_monitor) = &self.performance_monitor {
                let deadline = Duration::from_millis(100); // Normal command deadline
                performance_monitor.record_task_execution(execution_time, deadline).await;
            }
            
            info!("Simulated command executed: {} - {} in {:?}", command_type, action, execution_time);
            
            // Small delay between commands
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        
        Ok(())
    }
    
    /// Individual command message processing with 2ms urgent dispatch
    async fn process_command_message(&self, delivery: Delivery) -> Result<()> {
        let start_time = std::time::Instant::now();
        let routing_key = delivery.routing_key.as_str();
        let body = delivery.data.as_slice();
        
        info!("Command message received: {} ({} bytes)", routing_key, body.len());
        debug!("   Raw data: {}", String::from_utf8_lossy(body));
        
        // Attempt message parsing with dynamic deadline based on command priority
        match self.parse_command(body) {
            Ok(command) => {
                // Get dynamic deadline based on command priority and type
                let dispatch_deadline = self.get_command_deadline(&command);
                let is_urgent = self.is_urgent_command(&command);
                let is_emergency = self.is_emergency_command(&command);
                
                // Process command with deadline constraint
                let result = tokio::time::timeout(dispatch_deadline, async {
                    // Safety validation
                    let (allowed, _safety_action, interlock) = self.safety_manager
                        .validate_command(&command.command_type.to_string(), &serde_json::to_string(&command)?)
                        .await?;
                    
                    if !allowed {
                        // Command blocked by safety interlock
                        warn!("Command blocked: {} (interlock: {:?})", command.command_type.to_string(), interlock);
                        
                        // Send response for blocked command
                        let blocked_result = CommandResult::Error(format!("Safety interlock active: {:?}", interlock));
                        self.send_command_response(&command, &blocked_result, routing_key).await?;
                        
                        return Ok(());
                    }
                    
                    // Execute command
                    let result = self.execute_command(&command).await;
                    
                    // Record task execution time
                    if let Some(performance_monitor) = &self.performance_monitor {
                        let task_execution_time = start_time.elapsed();
                        performance_monitor.record_task_execution(task_execution_time, dispatch_deadline).await;
                    }
                    
                    // Send response to OCS
                    self.send_command_response(&command, &result, routing_key).await?;
                    
                    Ok(())
                }).await;
                
                match result {
                    Ok(Ok(())) => {
                        let processing_time = start_time.elapsed();
                        
                        // Check deadline violations with appropriate logging
                        if processing_time > dispatch_deadline {
                            if is_emergency {
                                error!("CRITICAL: Emergency command exceeded {:?} deadline: {:?}", dispatch_deadline, processing_time);
                            } else if is_urgent {
                                warn!("WARNING: Urgent command exceeded {:?} deadline: {:?}", dispatch_deadline, processing_time);
                            } else {
                                warn!("WARNING: Command exceeded {:?} deadline: {:?}", dispatch_deadline, processing_time);
                            }
                        } else {
                            if is_emergency {
                                info!("SUCCESS: Emergency command processed in {:?} (deadline: {:?})", processing_time, dispatch_deadline);
                            } else if is_urgent {
                                info!("SUCCESS: Urgent command processed in {:?} (deadline: {:?})", processing_time, dispatch_deadline);
                            } else {
                                debug!("SUCCESS: Command processed in {:?} (deadline: {:?})", processing_time, dispatch_deadline);
                            }
                        }
                        
                        // ACK on successful message processing
                        ack(&delivery).await?;
                        self.metrics.communication.increment_commands_processed();
                        
                        Ok(())
                    }
                    Ok(Err(e)) => {
                        error!("Command processing failed: {}", e);
                        ack(&delivery).await?;
                        self.metrics.communication.increment_command_errors();
                        Err(e)
                    }
                    Err(_) => {
                        error!("ERROR: Command processing exceeded {:?} deadline", dispatch_deadline);
                        nack_drop(&delivery).await?;
                        self.metrics.communication.increment_command_errors();
                        Err(anyhow::anyhow!("Command processing deadline exceeded: {:?}", dispatch_deadline))
                    }
                }
            }
            Err(e) => {
                error!("Command parsing failed: {}", e);
                
                // NACK on parsing failure (do not requeue)
                nack_drop(&delivery).await?;
                self.metrics.communication.increment_command_errors();
                
                Err(e)
            }
        }
    }
    
    /// Check if command is urgent and requires 2ms dispatch
    fn is_urgent_command(&self, command: &CommandMessage) -> bool {
        match &command.command_type {
            CommandType::SystemReset => true,
            CommandType::FaultInjection => true,
            CommandType::Custom(custom_type) => {
                // Check if custom command is marked as urgent
                custom_type.contains("urgent") || custom_type.contains("emergency")
            }
            _ => false,
        }
    }
    
    /// Check if command is emergency and requires immediate processing
    fn is_emergency_command(&self, command: &CommandMessage) -> bool {
        match &command.priority {
            CommandPriority::Emergency => true,
            _ => false,
        }
    }
    
    /// Get command dispatch deadline based on priority and type
    fn get_command_deadline(&self, command: &CommandMessage) -> Duration {
        if self.is_emergency_command(command) {
            Duration::from_millis(1) // Emergency commands get 1ms
        } else if self.is_urgent_command(command) {
            Duration::from_millis(2) // Urgent commands get 2ms
        } else {
            match command.priority {
                CommandPriority::High => Duration::from_millis(10),
                CommandPriority::Normal => Duration::from_millis(100),
                CommandPriority::Low => Duration::from_millis(1000),
                CommandPriority::Emergency => Duration::from_millis(1),
            }
        }
    }
    
    /// Command message parsing
    fn parse_command(&self, body: &[u8]) -> Result<CommandMessage> {
        let json_str = String::from_utf8_lossy(body);
        
        // Attempt JSON parsing
        match serde_json::from_str::<Value>(&json_str) {
            Ok(json_value) => {
                // Check command type
                let command_type = if let Some(type_str) = json_value.get("type").and_then(|v| v.as_str()) {
                    match type_str {
                        "system_status" => CommandType::SystemStatus,
                        "sensor_data" => CommandType::SensorData,
                        "fault_injection" => CommandType::FaultInjection,
                        "system_reset" => CommandType::SystemReset,
                        "calibration" => CommandType::Calibration,
                        other => CommandType::Custom(other.to_string()),
                            }
                        } else {
                    return Err(anyhow::anyhow!("Command type not specified"));
                };
                
                // Check action
                let action = json_value.get("action")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                
                // Extract parameters
                let mut parameters = json_value.clone();
                parameters.as_object_mut().unwrap().remove("type");
                parameters.as_object_mut().unwrap().remove("action");
                
                // Determine priority (default: Normal)
                let priority = CommandPriority::Normal;
                
                // Sequence number will be set later (initialize to 0 here)
                let mut command = CommandMessage::new(command_type.clone(), parameters, priority);
                command.header.sequence = 0; // Temporary value
                
                info!("Command parsing successful: {:?} - {}", command_type, action);
                Ok(command)
            }
            Err(e) => {
                Err(anyhow::anyhow!("JSON parsing failed: {}", e))
            }
        }
    }
    
    /// Command execution
    async fn execute_command(&self, command: &CommandMessage) -> CommandResult {
        info!("Executing command: {:?}", command.command_type);
        
        match &command.command_type {
            CommandType::SystemStatus => {
                self.handle_system_status_command(command).await
            }
            CommandType::SensorData => {
                self.handle_sensor_data_command(command).await
            }
            CommandType::FaultInjection => {
                self.handle_fault_injection_command(command).await
            }
            CommandType::SystemReset => {
                self.handle_system_reset_command(command).await
            }
            CommandType::Calibration => {
                self.handle_calibration_command(command).await
            }
            CommandType::Custom(custom_type) => {
                self.handle_custom_command(command, custom_type).await
            }
        }
    }
    
    /// System status command handling
    async fn handle_system_status_command(&self, _command: &CommandMessage) -> CommandResult {
        info!("Handling system status command");
        
        // Collect system status
        let status_data = serde_json::json!({
            "cpu_usage": 45.2,
            "memory_usage": 67.8,
            "temperature": 23.5,
            "uptime": 3600,
            "active_sensors": ["thermal", "gyro", "camera"],
            "status": "operational"
        });
        
        CommandResult::Success(status_data)
    }
    
    /// Sensor data command handling
    async fn handle_sensor_data_command(&self, command: &CommandMessage) -> CommandResult {
        info!("Handling sensor data command");
        
        // Check sensor type
        let sensor_type = command.parameters.get("sensor")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        
        // Generate virtual sensor data
        let sensor_data = match sensor_type {
            "thermal" => serde_json::json!({
                "sensor": "thermal",
                "value": 25.5,
                "unit": "celsius",
                "status": "normal",
                "timestamp": chrono::Utc::now()
            }),
            "gyro" => serde_json::json!({
                "sensor": "gyro",
                "value": 0.02,
                "unit": "rad/s",
                "status": "normal",
                "timestamp": chrono::Utc::now()
            }),
            "camera" => serde_json::json!({
                "sensor": "camera",
                "value": 1.0,
                "unit": "image_quality",
                "status": "normal",
                "timestamp": chrono::Utc::now()
            }),
            _ => serde_json::json!({
                "sensor": sensor_type,
                "value": 0.0,
                "unit": "unknown",
                "status": "error",
                "error": "Unknown sensor type"
            })
        };
        
        CommandResult::Success(sensor_data)
    }
    
    /// Fault injection command handling
    async fn handle_fault_injection_command(&self, command: &CommandMessage) -> CommandResult {
        info!("Handling fault injection command");
        
        let fault_type = command.parameters.get("fault")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        
        // Simulate fault injection
        let fault_result = serde_json::json!({
            "fault_type": fault_type,
            "injected": true,
            "timestamp": chrono::Utc::now(),
            "status": "fault_active"
        });
        
        warn!("Fault injected: {}", fault_type);
        CommandResult::Success(fault_result)
    }
    
    /// System reset command handling
    async fn handle_system_reset_command(&self, _command: &CommandMessage) -> CommandResult {
        info!("Handling system reset command");
        
        // Simulate reset
        let reset_result = serde_json::json!({
            "reset_type": "system",
            "status": "reset_initiated",
            "timestamp": chrono::Utc::now(),
            "estimated_duration": "30s"
        });
        
        info!("System reset initiated");
        CommandResult::Success(reset_result)
    }
    
    /// Calibration command handling
    async fn handle_calibration_command(&self, _command: &CommandMessage) -> CommandResult {
        info!("Handling calibration command");
        
        let cal_result = serde_json::json!({
            "calibration_type": "sensor",
            "status": "calibration_started",
            "timestamp": chrono::Utc::now(),
            "estimated_duration": "60s"
        });
        
        info!("Sensor calibration started");
        CommandResult::Success(cal_result)
    }
    
    /// Custom command handling
    async fn handle_custom_command(&self, command: &CommandMessage, custom_type: &str) -> CommandResult {
        info!("Handling custom command: {}", custom_type);
        
        let custom_result = serde_json::json!({
            "command_type": custom_type,
            "status": "custom_command_executed",
            "timestamp": chrono::Utc::now(),
            "parameters": command.parameters
        });
        
        CommandResult::Success(custom_result)
    }
    
    /// Send command response with latency tracking
    async fn send_command_response(&self, _command: &CommandMessage, result: &CommandResult, routing_key: &str) -> Result<()> {
        let response_start = Instant::now();
        let sequence = self.amqp_client.next_sequence().await;
        
        let (response_type, data, success) = match result {
            CommandResult::Success(data) => (ResponseType::Data, data.clone(), true),
            CommandResult::Error(error_msg) => (
                ResponseType::Error,
                serde_json::json!({"error": error_msg}),
                false
            ),
        };
        
        let response = ResponseMessage::new(
            sequence,
            response_type,
            data,
            success
        );
        
        let response_json = serde_json::to_string(&response)?;
        let response_key = format!("gcs.response.{}", routing_key.trim_start_matches("ocs."));
        
        self.amqp_client.publish_telemetry(&response_key, response_json.as_bytes()).await?;
        
        // Track command to response latency
        let command_to_response_latency = response_start.elapsed();
        info!("METRICS: Command to response latency: {:?}", command_to_response_latency);
        info!("Command response sent: {} → {}", response_key, response_json);
        
        Ok(())
    }
}

