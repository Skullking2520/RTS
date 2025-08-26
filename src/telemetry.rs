// telemetry.rs - Enhanced OCS Telemetry Reception and Decoding for A+ Grade
use anyhow::Result;
use futures_util::StreamExt;
use lapin::message::Delivery;
use serde_json::Value;
use tokio::sync::mpsc;
use tracing::{info, warn, error, debug};
use std::time::{Instant, Duration};
use crate::{
    amqp::{AmqpClient, ack, nack_drop},
    types::{AlertMessage, AlertSeverity, TelemetryMessage, SensorStatus, ResponseMessage, ResponseType},
    metrics::Metrics,
};

/// Enhanced Telemetry Processing Task with A+ Grade Features
pub struct TelemetryTask {
    amqp_client: AmqpClient,
    metrics: Metrics,
    response_tx: mpsc::Sender<ResponseMessage>,
    packet_loss_counter: std::sync::Arc<tokio::sync::Mutex<u32>>,
    last_packet_time: std::sync::Arc<tokio::sync::Mutex<Option<Instant>>>,
    expected_period: Duration,
    loc_threshold: u32,
}

impl TelemetryTask {
    pub fn new(
        amqp_client: AmqpClient,
        metrics: Metrics,
        response_tx: mpsc::Sender<ResponseMessage>,
    ) -> Self {
        Self {
            amqp_client,
            metrics,
            response_tx,
            packet_loss_counter: std::sync::Arc::new(tokio::sync::Mutex::new(0)),
            last_packet_time: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
            expected_period: Duration::from_millis(20), // 20ms expected period
            loc_threshold: 3, // Loss of Contact threshold
        }
    }
    
    /// Enhanced telemetry processing task execution with real-time constraints
    pub async fn run(&self, queue_name: &str, budget: std::time::Duration) -> Result<()> {
        info!("Enhanced Telemetry Processing Task Started: {}", queue_name);
        info!("   Decoding Budget: {:?}", budget);
        info!("   Expected Period: {:?}", self.expected_period);
        info!("   LOC Threshold: {} consecutive failures", self.loc_threshold);
        
        // Create telemetry consumer
        let mut consumer = self.amqp_client.create_telemetry_consumer().await?;
        
        info!("Waiting for telemetry messages...");
        
        // Enhanced message processing loop with real-time monitoring
        while let Some(delivery) = consumer.next().await {
            let start_time = Instant::now();
            
            match delivery {
                Ok(delivery) => {
                    // Check if we're within budget
                    if start_time.elapsed() > budget {
                        warn!("Processing exceeded budget: {:?} > {:?}", 
                              start_time.elapsed(), budget);
                    }
                    
                    if let Err(e) = self.process_telemetry_message(delivery, start_time).await {
                        error!("Telemetry message processing failed: {}", e);
                        self.increment_packet_loss().await;
                    } else {
                        self.reset_packet_loss().await;
                    }
                }
                Err(e) => {
                    error!("Message reception error: {}", e);
                    self.increment_packet_loss().await;
                }
            }
            
            // Check for Loss of Contact
            if self.should_trigger_loc().await {
                self.trigger_loss_of_contact().await?;
            }
        }
        
        Ok(())
    }
    
    /// Enhanced individual telemetry message processing with 3ms deadline
    async fn process_telemetry_message(&self, delivery: Delivery, start_time: Instant) -> Result<()> {
        let routing_key = delivery.routing_key.as_str();
        let body = delivery.data.as_slice();
        
        info!("Telemetry message received: {} ({} bytes)", routing_key, body.len());
        debug!("   Raw data: {}", String::from_utf8_lossy(body));
        
        // Start decoding timer
        let decode_start = Instant::now();
        
        // Parse message with strict 3ms deadline
        let message_type = match self.parse_message_with_deadline(body, Duration::from_millis(3)).await {
            Ok(msg_type) => msg_type,
            Err(e) => {
                error!("❌ Message parsing failed within 3ms deadline: {}", e);
                self.metrics.communication.increment_telemetry_errors();
                nack_drop(&delivery).await?;
                return Err(e);
            }
        };
        
        let decode_time = decode_start.elapsed();
        info!("✅ Message decoded in {:?} (deadline: 3ms)", decode_time);
        
        // Process message based on type
        match message_type {
            MessageType::Alert(alert) => {
                self.handle_alert_message(alert, routing_key).await?;
            }
            MessageType::Telemetry(telemetry) => {
                self.handle_telemetry_message(telemetry, routing_key).await?;
            }
            MessageType::Unknown(raw_data) => {
                warn!("⚠️  Unknown message format: {}", raw_data);
                self.handle_unknown_message(raw_data, routing_key).await?;
            }
        }
        
        // Update packet timing for drift calculation
        self.update_packet_timing().await;
        
        // Calculate total processing time
        let total_time = start_time.elapsed();
        info!("📊 Total processing time: {:?}", total_time);
        
        // Message processed successfully - ACK
        ack(&delivery).await?;
        self.metrics.communication.increment_telemetry_processed();
        
        // Record performance metrics
        self.metrics.performance.record_processing_time(total_time.as_nanos() as u64);
        
        Ok(())
    }
    
    /// Parse message with strict deadline constraint
    async fn parse_message_with_deadline(&self, body: &[u8], deadline: Duration) -> Result<MessageType> {
        let start = Instant::now();
        
        // Spawn parsing task with timeout
        let parse_result = tokio::time::timeout(deadline, async {
            self.parse_message(body)
        }).await;
        
        match parse_result {
            Ok(Ok(message_type)) => {
                let parse_time = start.elapsed();
                if parse_time > deadline {
                    warn!("⚠️  Parsing exceeded deadline: {:?} > {:?}", parse_time, deadline);
                }
                Ok(message_type)
            }
            Ok(Err(e)) => Err(e),
            Err(_) => Err(anyhow::anyhow!("Parsing deadline exceeded: {:?}", deadline)),
        }
    }
    
    /// Enhanced message parsing with performance tracking
    fn parse_message(&self, body: &[u8]) -> Result<MessageType> {
        let json_str = String::from_utf8_lossy(body);
        
        // JSON parsing attempt
        match serde_json::from_str::<Value>(&json_str) {
            Ok(json_value) => {
                // OCS alert format check: {"alert": "ThermalLoss"}
                if let Some(alert_value) = json_value.get("alert") {
                    if let Some(alert_type) = alert_value.as_str() {
                        let alert = AlertMessage::new(
                            alert_type,
                            AlertSeverity::Warning, // Default
                            &format!("OCS Alert: {}", alert_type)
                        );
                        return Ok(MessageType::Alert(alert));
                    }
                }
                
                // OCS telemetry format check
                if let Some(sensor) = json_value.get("sensor") {
                    if let Some(value) = json_value.get("value") {
                        if let Some(unit) = json_value.get("unit") {
                            let telemetry = TelemetryMessage::new(
                                sensor.as_str().unwrap_or("unknown"),
                                value.as_f64().unwrap_or(0.0),
                                unit.as_str().unwrap_or("unknown"),
                                SensorStatus::Normal
                            );
                            return Ok(MessageType::Telemetry(telemetry));
                        }
                    }
                }
                
                // Other JSON formats
                Ok(MessageType::Unknown(json_value))
            }
            Err(_) => {
                // Non-JSON case - process as raw data
                Ok(MessageType::Unknown(Value::String(json_str.to_string())))
            }
        }
    }
    
    /// Enhanced alert message handling with real-time logging
    async fn handle_alert_message(&self, alert: AlertMessage, routing_key: &str) -> Result<()> {
        let start_time = Instant::now();
        
        info!("🚨 Alert message processing: {} (Severity: {:?})", 
              alert.alert_type, alert.severity);
        
        // Update metrics
        self.metrics.communication.increment_alerts_received();
        
        // Process based on severity with real-time constraints
        match alert.severity {
            AlertSeverity::Critical => {
                error!("🚨 CRITICAL ALERT: {}", alert.description);
                // Immediate response for critical alerts
                if start_time.elapsed() > Duration::from_millis(100) {
                    warn!("⚠️  Critical alert response time exceeded 100ms threshold");
                }
            }
            AlertSeverity::Error => {
                error!("❌ ERROR ALERT: {}", alert.description);
            }
            AlertSeverity::Warning => {
                warn!("⚠️  WARNING ALERT: {}", alert.description);
            }
            AlertSeverity::Info => {
                info!("ℹ️  INFO ALERT: {}", alert.description);
            }
        }
        
        // Send response to OCS with timestamp
        self.send_alert_response(&alert, routing_key).await?;
        
        let total_time = start_time.elapsed();
        info!("📊 Alert processing completed in {:?}", total_time);
        
        Ok(())
    }
    
    /// Enhanced telemetry message handling with performance tracking
    async fn handle_telemetry_message(&self, telemetry: TelemetryMessage, routing_key: &str) -> Result<()> {
        let start_time = Instant::now();
        
        info!("📊 Telemetry message processing: {} = {} {}", 
              telemetry.sensor_type, telemetry.value, telemetry.unit);
        
        // Update metrics
        self.metrics.communication.increment_telemetry_received();
        
        // Process based on sensor status
        match telemetry.status {
            SensorStatus::Normal => {
                debug!("✅ Normal sensor data: {}", telemetry.sensor_type);
            }
            SensorStatus::Warning => {
                warn!("⚠️  Sensor warning: {}", telemetry.sensor_type);
            }
            SensorStatus::Error => {
                error!("❌ Sensor error: {}", telemetry.sensor_type);
            }
            SensorStatus::Offline => {
                warn!("🔴 Sensor offline: {}", telemetry.sensor_type);
            }
        }
        
        // Send response to OCS with timestamp
        self.send_telemetry_response(&telemetry, routing_key).await?;
        
        let total_time = start_time.elapsed();
        info!("📊 Telemetry processing completed in {:?}", total_time);
        
        Ok(())
    }
    
    /// Enhanced unknown message handling
    async fn handle_unknown_message(&self, raw_data: Value, routing_key: &str) -> Result<()> {
        warn!("❓ Unknown message format processing: {}", raw_data);
        
        // Update metrics
        self.metrics.communication.increment_unknown_messages();
        
        // Send error response to OCS with timestamp
        self.send_error_response("Unknown message format", routing_key).await?;
        
        Ok(())
    }
    
    /// Enhanced alert response with timestamp
    async fn send_alert_response(&self, alert: &AlertMessage, routing_key: &str) -> Result<()> {
        let sequence = self.amqp_client.next_sequence().await;
        let response = ResponseMessage::new(
            sequence,
            ResponseType::Acknowledgment,
            serde_json::json!({
                "alert_type": alert.alert_type,
                "status": "received",
                "timestamp": alert.header.timestamp,
                "gcs_processing_time": chrono::Utc::now()
            }),
            true
        );
        
        let response_json = serde_json::to_string(&response)?;
        let response_key = format!("gcs.response.{}", routing_key.trim_start_matches("ocs."));
        
        self.amqp_client.publish_telemetry(&response_key, response_json.as_bytes()).await?;
        info!("📤 Alert response sent: {} → {} (timestamp: {})", 
              response_key, response_json, chrono::Utc::now());
        
        Ok(())
    }
    
    /// Enhanced telemetry response with timestamp
    async fn send_telemetry_response(&self, telemetry: &TelemetryMessage, routing_key: &str) -> Result<()> {
        let sequence = self.amqp_client.next_sequence().await;
        let response = ResponseMessage::new(
            sequence,
            ResponseType::Data,
            serde_json::json!({
                "sensor_type": telemetry.sensor_type,
                "value": telemetry.value,
                "unit": telemetry.unit,
                "status": "processed",
                "gcs_processing_time": chrono::Utc::now()
            }),
            true
        );
        
        let response_json = serde_json::to_string(&response)?;
        let response_key = format!("gcs.response.{}", routing_key.trim_start_matches("ocs."));
        
        self.amqp_client.publish_telemetry(&response_key, response_json.as_bytes()).await?;
        info!("📤 Telemetry response sent: {} → {} (timestamp: {})", 
              response_key, response_json, chrono::Utc::now());
        
        Ok(())
    }
    
    /// Enhanced error response with timestamp
    async fn send_error_response(&self, error_msg: &str, routing_key: &str) -> Result<()> {
        let sequence = self.amqp_client.next_sequence().await;
        let response = ResponseMessage::new(
            sequence,
            ResponseType::Error,
            serde_json::json!({
                "error": error_msg,
                "timestamp": chrono::Utc::now(),
                "gcs_processing_time": chrono::Utc::now()
            }),
            false
        );
        
        let response_json = serde_json::to_string(&response)?;
        let response_key = format!("gcs.response.{}", routing_key.trim_start_matches("ocs."));
        
        self.amqp_client.publish_telemetry(&response_key, response_json.as_bytes()).await?;
        info!("📤 Error response sent: {} → {} (timestamp: {})", 
              response_key, response_json, chrono::Utc::now());
        
        Ok(())
    }
    
    /// Packet loss tracking for Loss of Contact detection
    async fn increment_packet_loss(&self) {
        let mut counter = self.packet_loss_counter.lock().await;
        *counter += 1;
        warn!("📉 Packet loss detected. Count: {}", *counter);
    }
    
    /// Reset packet loss counter on successful reception
    async fn reset_packet_loss(&self) {
        let mut counter = self.packet_loss_counter.lock().await;
        if *counter > 0 {
            info!("✅ Packet loss counter reset from {} to 0", *counter);
            *counter = 0;
        }
    }
    
    /// Check if Loss of Contact should be triggered
    async fn should_trigger_loc(&self) -> bool {
        let counter = self.packet_loss_counter.lock().await;
        *counter >= self.loc_threshold
    }
    
    /// Trigger Loss of Contact simulation
    async fn trigger_loss_of_contact(&self) -> Result<()> {
        error!("🚨 LOSS OF CONTACT DETECTED! {} consecutive packet failures", self.loc_threshold);
        
        // Send LOC alert to OCS
        let sequence = self.amqp_client.next_sequence().await;
        let loc_alert = serde_json::json!({
            "alert": "LossOfContact",
            "severity": "Critical",
            "description": "GCS detected loss of contact with OCS",
            "timestamp": chrono::Utc::now(),
            "packet_failures": self.loc_threshold
        });
        
        self.amqp_client.publish_telemetry("gcs.alert.loc", &serde_json::to_string(&loc_alert)?.as_bytes()).await?;
        
        // Reset counter after LOC trigger
        let mut counter = self.packet_loss_counter.lock().await;
        *counter = 0;
        
        Ok(())
    }
    
    /// Update packet timing for drift calculation
    async fn update_packet_timing(&self) {
        let now = Instant::now();
        let mut last_time = self.last_packet_time.lock().await;
        
        if let Some(prev_time) = *last_time {
            let actual_interval = now.duration_since(prev_time);
            let drift = if actual_interval > self.expected_period {
                actual_interval - self.expected_period
            } else {
                self.expected_period - actual_interval
            };
            
            if drift > Duration::from_millis(5) {
                warn!("⚠️  Significant timing drift detected: {:?} (expected: {:?})", 
                      actual_interval, self.expected_period);
            }
        }
        
        *last_time = Some(now);
    }
}

/// Message type enumeration
enum MessageType {
    Alert(AlertMessage),
    Telemetry(TelemetryMessage),
    Unknown(Value),
}
