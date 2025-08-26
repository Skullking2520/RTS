// uplink.rs - OCS Command Transmission and Response Processing
use anyhow::Result;
use futures_util::StreamExt;
use lapin::message::Delivery;
use serde_json::Value;
use tokio::sync::mpsc;
use tracing::{info, warn, error, debug};
use crate::{
    amqp::{AmqpClient, ack, nack_drop},
    types::{CommandMessage, CommandType, CommandPriority, ResponseMessage, ResponseType},
    metrics::Metrics,
    safety::{SafetyInterlockManager, InterlockType, InterlockState},
};

/// Command execution result
#[derive(Debug, Clone)]
enum CommandResult {
    Success(Value),
    Error(String),
}

/// Command Processing Task
pub struct UplinkTask {
    amqp_client: AmqpClient,
    metrics: Metrics,
    response_tx: mpsc::Sender<ResponseMessage>,
    safety_manager: SafetyInterlockManager,
}

impl UplinkTask {
    pub fn new(
        amqp_client: AmqpClient,
        metrics: Metrics,
        response_tx: mpsc::Sender<ResponseMessage>,
    ) -> Self {
        Self {
            amqp_client,
            metrics,
            response_tx,
            safety_manager: SafetyInterlockManager::new(),
        }
    }
    
    /// Command processing task execution
    pub async fn run(&self, queue_name: &str, _budget: std::time::Duration) -> Result<()> {
        info!("Command processing task started: {}", queue_name);
        
        // Create command consumer
        let mut consumer = self.amqp_client.create_command_consumer().await?;
        
        info!("Waiting for command messages...");
        
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
        
        Ok(())
    }
    
    /// Individual command message processing
    async fn process_command_message(&self, delivery: Delivery) -> Result<()> {
        let routing_key = delivery.routing_key.as_str();
        let body = delivery.data.as_slice();
        
        info!("Command message received: {} ({} bytes)", routing_key, body.len());
        debug!("   Raw data: {}", String::from_utf8_lossy(body));
        
        // Attempt message parsing
        match self.parse_command(body) {
            Ok(command) => {
                // Safety validation
                let (allowed, safety_action, interlock) = self.safety_manager
                    .validate_command(&command.command_type.to_string(), &serde_json::to_string(&command)?)
                    .await?;
                
                if !allowed {
                    // Command blocked by safety interlock
                    warn!("Command blocked: {} (interlock: {:?})", command.command_type.to_string(), interlock);
                    
                    // Send response for blocked command
                    let blocked_result = CommandResult::Error(format!("Safety interlock active: {:?}", interlock));
                    self.send_command_response(&command, &blocked_result, routing_key).await?;
                    
                    // Record message processing failure
                    ack(&delivery).await?;
                    self.metrics.communication.increment_command_errors();
                    
                    return Ok(());
                }
                
                // Execute command
                let result = self.execute_command(&command).await;
                
                // Send response to OCS
                self.send_command_response(&command, &result, routing_key).await?;
                
                // ACK on successful message processing
                ack(&delivery).await?;
                self.metrics.communication.increment_commands_processed();
                
                Ok(())
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
    
    /// Send command response
    async fn send_command_response(&self, _command: &CommandMessage, result: &CommandResult, routing_key: &str) -> Result<()> {
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
        info!("Command response sent: {} → {}", response_key, response_json);
        
        Ok(())
    }
}

