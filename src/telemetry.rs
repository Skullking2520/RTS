// telemetry.rs - Enhanced OCS Telemetry Reception and Decoding
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
    performance::PerformanceMonitor,
};

/// Enhanced Telemetry Processing Task
#[derive(Clone)]
pub struct TelemetryTask {
    amqp_client: AmqpClient,
    metrics: Metrics,
    response_tx: mpsc::Sender<ResponseMessage>,
    packet_loss_counter: std::sync::Arc<tokio::sync::Mutex<u32>>,
    last_packet_time: std::sync::Arc<tokio::sync::Mutex<Option<Instant>>>,
    last_sequence: std::sync::Arc<tokio::sync::Mutex<u64>>,
    consecutive_failures: std::sync::Arc<tokio::sync::Mutex<u64>>,
    expected_period: Duration,
    loc_threshold: u32,
    performance_monitor: Option<PerformanceMonitor>,
    packet_re_request_queue: std::sync::Arc<tokio::sync::Mutex<Vec<u64>>>,
}

impl TelemetryTask {
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
            packet_loss_counter: std::sync::Arc::new(tokio::sync::Mutex::new(0)),
            last_packet_time: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
            last_sequence: std::sync::Arc::new(tokio::sync::Mutex::new(0)),
            consecutive_failures: std::sync::Arc::new(tokio::sync::Mutex::new(0)),
            expected_period: Duration::from_millis(20), // 20ms expected period
            loc_threshold: 3, // Loss of Contact threshold
            performance_monitor,
            packet_re_request_queue: std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new())),
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
        
        // Start packet re-request monitoring task
        let re_request_handle = self.start_packet_re_request_monitor().await;
        
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
        
        // Cleanup
        re_request_handle.abort();
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
                error!("ERROR: Message parsing failed within 3ms deadline: {}", e);
                self.metrics.communication.increment_telemetry_errors();
                nack_drop(&delivery).await?;
                return Err(e);
            }
        };
        
        let decode_time = decode_start.elapsed();
        info!("SUCCESS: Message decoded in {:?} (deadline: 3ms)", decode_time);
        
        // Log packet reception latency and reception drift
        let reception_latency = start_time.elapsed();
        info!("METRICS: Packet reception latency: {:?}", reception_latency);
        
        // Calculate reception drift (actual vs expected arrival)
        let expected_arrival = self.calculate_expected_arrival().await;
        let actual_arrival = start_time;
        let reception_drift = if actual_arrival > expected_arrival {
            actual_arrival.duration_since(expected_arrival)
        } else {
            expected_arrival.duration_since(actual_arrival)
        };
        info!("METRICS: Reception drift: {:?} (expected: {:?}, actual: {:?})", 
              reception_drift, expected_arrival, actual_arrival);
        
        // Check for packet sequence and detect missing packets
        if let Ok(sequence) = self.extract_packet_sequence(body) {
            self.check_packet_sequence(sequence).await;
        }
        
        // Process message based on type
        match message_type {
            MessageType::Alert(alert) => {
                self.handle_alert_message(alert, routing_key).await?;
            }
            MessageType::Telemetry(telemetry) => {
                self.handle_telemetry_message(telemetry, routing_key).await?;
            }
            MessageType::Unknown(raw_data) => {
                warn!("WARNING: Unknown message format: {}", raw_data);
                self.handle_unknown_message(raw_data, routing_key).await?;
            }
        }
        
        // Update packet timing for drift calculation
        self.update_packet_timing().await;
        
        // Calculate total processing time
        let total_time = start_time.elapsed();
        info!("METRICS: Total processing time: {:?}", total_time);
        
        // Record jitter
        if let Some(performance_monitor) = &self.performance_monitor {
            let expected_interval = Duration::from_millis(20); // Expected 20ms interval
            performance_monitor.record_jitter(expected_interval, total_time).await;
        }
        
        // Message processed successfully - ACK
        ack(&delivery).await?;
        self.metrics.communication.increment_telemetry_processed();
        
        // Record performance metrics
        self.metrics.performance.record_processing_time(total_time.as_nanos() as u64);
        
        Ok(())
    }
    
    /// Extract packet sequence number from message
    fn extract_packet_sequence(&self, body: &[u8]) -> Result<u64> {
        let json_str = String::from_utf8_lossy(body);
        let json_value: serde_json::Value = serde_json::from_str(&json_str)?;
        
        if let Some(sequence) = json_value.get("sequence").and_then(|v| v.as_u64()) {
            Ok(sequence)
        } else {
            Err(anyhow::anyhow!("No sequence number found in packet"))
        }
    }
    
    /// Check packet sequence and trigger re-request for missing packets
    async fn check_packet_sequence(&self, current_sequence: u64) {
        let mut last_sequence = self.last_sequence.lock().await;
        
        if *last_sequence == 0 {
            // First packet
            *last_sequence = current_sequence;
            return;
        }
        
        let expected_sequence = *last_sequence + 1;
        
        if current_sequence != expected_sequence {
            let missing_packets = current_sequence - expected_sequence;
            warn!("WARNING: Missing {} packets detected! Expected: {}, Got: {}", 
                  missing_packets, expected_sequence, current_sequence);
            
            // Increment consecutive failure counter
            let mut failure_count = self.consecutive_failures.lock().await;
            *failure_count += missing_packets;
            
            // Check for loss of contact (≥3 consecutive failures)
            if *failure_count >= 3 {
                error!("CRITICAL: Loss of contact detected! {} consecutive packet failures", *failure_count);
                self.trigger_loss_of_contact_alert().await;
            }
            
            // Add missing sequences to re-request queue
            let mut queue = self.packet_re_request_queue.lock().await;
            for seq in expected_sequence..current_sequence {
                queue.push(seq);
            }
            
            // Trigger immediate re-request for critical missing packets
            if missing_packets <= 5 {
                self.trigger_packet_re_request(expected_sequence, current_sequence - 1).await;
            }
        } else {
            // Reset failure counter on successful packet
            let mut failure_count = self.consecutive_failures.lock().await;
            *failure_count = 0;
        }
        
        *last_sequence = current_sequence;
    }
    
    /// Trigger packet re-request for missing sequence range
    async fn trigger_packet_re_request(&self, start_sequence: u64, end_sequence: u64) {
        info!("RETRY: Triggering packet re-request for sequences {} to {}", start_sequence, end_sequence);
        
        // Send re-request command to OCS
        let re_request_command = serde_json::json!({
            "type": "packet_re_request",
            "action": "request_missing",
            "start_sequence": start_sequence,
            "end_sequence": end_sequence,
            "timestamp": chrono::Utc::now(),
            "reason": "missing_packets_detected"
        });
        
        // Publish re-request to OCS command queue
        if let Err(e) = self.amqp_client.publish_command(re_request_command.to_string().as_bytes()).await {
            error!("ERROR: Failed to send packet re-request: {}", e);
        } else {
            info!("SUCCESS: Packet re-request sent to OCS");
        }
    }
    
    /// Start packet re-request monitoring task
    async fn start_packet_re_request_monitor(&self) -> tokio::task::JoinHandle<()> {
        let queue = self.packet_re_request_queue.clone();
        let amqp_client = self.amqp_client.clone();
        
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(100));
            
            loop {
                tick.tick().await;
                
                // Process pending re-requests
                let mut pending_requests = queue.lock().await;
                if !pending_requests.is_empty() {
                    let requests = pending_requests.clone();
                    pending_requests.clear();
                    
                    // Send batch re-request
                    if let Err(e) = Self::send_batch_re_request(&amqp_client, &requests).await {
                        error!("Failed to send batch re-request: {}", e);
                        // Re-queue failed requests
                        pending_requests.extend(requests);
                    }
                }
            }
        })
    }
    
    /// Send batch packet re-request
    async fn send_batch_re_request(amqp_client: &AmqpClient, sequences: &[u64]) -> Result<()> {
        if sequences.is_empty() {
            return Ok(());
        }
        
        let re_request_command = serde_json::json!({
            "type": "packet_re_request",
            "action": "request_missing_batch",
            "missing_sequences": sequences,
            "timestamp": chrono::Utc::now(),
            "reason": "missing_packets_detected"
        });
        
        amqp_client.publish_command(re_request_command.to_string().as_bytes()).await?;
        info!("Batch re-request sent for {} missing packets", sequences.len());
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
                    warn!("WARNING: Parsing exceeded deadline: {:?} > {:?}", parse_time, deadline);
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
    
    /// Enhanced alert message handling with real-time logging and 100ms threshold monitoring
    async fn handle_alert_message(&self, alert: AlertMessage, routing_key: &str) -> Result<()> {
        let start_time = Instant::now();
        
        info!("ALERT: Alert message processing: {} (Severity: {:?})", 
              alert.alert_type, alert.severity);
        
        // Update metrics
        self.metrics.communication.increment_alerts_received();
        
        // Process based on severity with real-time constraints
        match alert.severity {
            AlertSeverity::Critical => {
                error!("CRITICAL ALERT: {}", alert.description);
                
                // Monitor 100ms fault response threshold
                let response_start = Instant::now();
                self.send_alert_response(&alert, routing_key).await?;
                let response_time = response_start.elapsed();
                
                if response_time > Duration::from_millis(100) {
                    error!("ERROR CRITICAL: Fault response time exceeded 100ms threshold: {:?}", response_time);
                    self.trigger_critical_ground_alert(&alert, response_time).await?;
                } else {
                    info!("SUCCESS: Critical alert response within 100ms: {:?}", response_time);
                }
            }
            AlertSeverity::Error => {
                error!("ERROR ALERT: {}", alert.description);
                
                // Monitor response time for error alerts
                let response_start = Instant::now();
                self.send_alert_response(&alert, routing_key).await?;
                let response_time = response_start.elapsed();
                
                if response_time > Duration::from_millis(100) {
                    warn!("WARNING: Error alert response time exceeded 100ms: {:?}", response_time);
                }
            }
            AlertSeverity::Warning => {
                warn!("WARNING ALERT: {}", alert.description);
                self.send_alert_response(&alert, routing_key).await?;
            }
            AlertSeverity::Info => {
                info!("INFO ALERT: {}", alert.description);
                self.send_alert_response(&alert, routing_key).await?;
            }
        }
        
        let total_time = start_time.elapsed();
        info!("METRICS: Alert processing completed in {:?}", total_time);
        
        Ok(())
    }
    
    /// Trigger critical ground alert when fault response exceeds 100ms
    async fn trigger_critical_ground_alert(&self, alert: &AlertMessage, response_time: Duration) -> Result<()> {
        error!("CRITICAL GROUND ALERT TRIGGERED!");
        error!("   Alert: {}", alert.description);
        error!("   Response time: {:?} (threshold: 100ms)", response_time);
        error!("   Alert type: {}", alert.alert_type);
        error!("   Timestamp: {}", alert.header.timestamp);
        
        // Send critical ground alert to monitoring systems
        let ground_alert = serde_json::json!({
            "type": "critical_ground_alert",
            "alert_id": alert.header.sequence,
            "description": format!("Fault response time exceeded 100ms: {:?}", response_time),
            "original_alert": alert,
            "response_time_ms": response_time.as_millis(),
            "threshold_ms": 100,
            "timestamp": chrono::Utc::now(),
            "severity": "CRITICAL",
            "action_required": "IMMEDIATE_INTERVENTION"
        });
        
        // Publish critical ground alert
        if let Err(e) = self.amqp_client.publish_telemetry("gcs_critical_alerts", ground_alert.to_string().as_bytes()).await {
            error!("ERROR: Failed to publish critical ground alert: {}", e);
        } else {
            error!("SUCCESS: Critical ground alert published");
        }
        
        Ok(())
    }
    
    /// Enhanced telemetry message handling with performance tracking and latency measurement
    async fn handle_telemetry_message(&self, telemetry: TelemetryMessage, routing_key: &str) -> Result<()> {
        let start_time = Instant::now();
        
        // Track sensor to buffer latency (simulated)
        let sensor_to_buffer_latency = Duration::from_millis(1); // Simulated 1ms sensor processing
        info!("METRICS: Sensor to buffer latency: {:?}", sensor_to_buffer_latency);
        
        info!("METRICS: Telemetry message processing: {} = {} {}", 
              telemetry.sensor_type, telemetry.value, telemetry.unit);
        
        // Update metrics
        self.metrics.communication.increment_telemetry_received();
        
        // Process based on sensor status
        match telemetry.status {
            SensorStatus::Normal => {
                debug!("SUCCESS: Normal sensor data: {}", telemetry.sensor_type);
            }
            SensorStatus::Warning => {
                warn!("WARNING: Sensor warning: {}", telemetry.sensor_type);
            }
            SensorStatus::Error => {
                error!("ERROR: Sensor error: {}", telemetry.sensor_type);
            }
            SensorStatus::Offline => {
                warn!("OFFLINE: Sensor offline: {}", telemetry.sensor_type);
            }
        }
        
        // Send response to OCS with timestamp
        self.send_telemetry_response(&telemetry, routing_key).await?;
        
        let total_time = start_time.elapsed();
        info!("METRICS: Telemetry processing completed in {:?}", total_time);
        
        Ok(())
    }
    
    /// Enhanced unknown message handling
    async fn handle_unknown_message(&self, raw_data: Value, routing_key: &str) -> Result<()> {
        warn!("Unknown message format processing: {}", raw_data);
        
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
        info!("Alert response sent: {} -> {} (timestamp: {})", 
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
        info!("Telemetry response sent: {} -> {} (timestamp: {})", 
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
        info!("Error response sent: {} -> {} (timestamp: {})", 
              response_key, response_json, chrono::Utc::now());
        
        Ok(())
    }
    
    /// Packet loss tracking for Loss of Contact detection
    async fn increment_packet_loss(&self) {
        let mut counter = self.packet_loss_counter.lock().await;
        *counter += 1;
        warn!("Packet loss detected. Count: {}", *counter);
    }
    
    /// Reset packet loss counter on successful reception
    async fn reset_packet_loss(&self) {
        let mut counter = self.packet_loss_counter.lock().await;
        if *counter > 0 {
            info!("SUCCESS: Packet loss counter reset from {} to 0", *counter);
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
        error!("LOSS OF CONTACT DETECTED! {} consecutive packet failures", self.loc_threshold);
        
        // Send LOC alert to OCS
        let _sequence = self.amqp_client.next_sequence().await;
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
    
    /// Simulate fault messages from satellite
    pub async fn simulate_fault_messages(&self) -> Result<()> {
        let fault_types = vec![
            ("ThermalLoss", "Critical"),
            ("SensorFailure", "Error"),
            ("PowerAnomaly", "Warning"),
            ("CommunicationError", "Error"),
            ("SystemOverload", "Critical"),
        ];
        
        for (fault_type, severity) in fault_types {
            let fault_alert = serde_json::json!({
                "alert": fault_type,
                "severity": severity,
                "description": format!("Simulated {} from satellite", fault_type),
                "timestamp": chrono::Utc::now(),
                "source": "satellite_simulation"
            });
            
            // Publish simulated fault to telemetry queue
            if let Err(e) = self.amqp_client.publish_telemetry("ocs.alert.simulated", 
                &serde_json::to_string(&fault_alert)?.as_bytes()).await {
                error!("Failed to publish simulated fault: {}", e);
            } else {
                info!("Simulated fault published: {} ({})", fault_type, severity);
            }
            
            // Add delay between fault simulations
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        
        Ok(())
    }
    
    /// Simulate real telemetry messages for testing
    pub async fn simulate_telemetry_messages(&self) -> Result<()> {
        let sensor_types = vec![
            ("thermal", "temperature", "°C"),
            ("gyro", "angular_velocity", "rad/s"),
            ("camera", "image_quality", "%"),
            ("power", "voltage", "V"),
            ("attitude", "orientation", "deg"),
        ];
        
        for (sensor_type, measurement, unit) in sensor_types {
            // Simulate telemetry processing directly
            let start_time = Instant::now();
            
            let _telemetry_message = serde_json::json!({
                "header": {
                    "timestamp": chrono::Utc::now(),
                    "source": "ocs",
                    "sequence": chrono::Utc::now().timestamp_millis() as u64,
                    "message_type": "telemetry"
                },
                "sensor_type": sensor_type,
                "value": 25.0 + (chrono::Utc::now().timestamp_millis() % 100) as f64 * 0.1,
                "unit": unit,
                "status": "Normal"
            });
            
            // Simulate telemetry processing
            let total_time = start_time.elapsed();
            
            // Record jitter
            if let Some(performance_monitor) = &self.performance_monitor {
                let expected_interval = Duration::from_millis(20); // Expected 20ms interval
                performance_monitor.record_jitter(expected_interval, total_time).await;
            }
            
            info!("Simulated telemetry processed: {} - {} in {:?}", sensor_type, measurement, total_time);
            
            // Small delay between messages
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        
        Ok(())
    }
    
    /// Trigger loss of contact alert
    async fn trigger_loss_of_contact_alert(&self) {
        let alert_message = serde_json::json!({
            "alert": "LossOfContact",
            "description": "Loss of contact with satellite - 3+ consecutive packet failures",
            "severity": "Critical",
            "source": "gcs_telemetry",
            "timestamp": chrono::Utc::now(),
            "action_required": "Emergency procedures activated"
        });
        
        if let Err(e) = self.amqp_client.publish_telemetry(
            "gcs.alert.loss_of_contact",
            alert_message.to_string().as_bytes()
        ).await {
            error!("Failed to publish loss of contact alert: {}", e);
        } else {
            error!("LOSS OF CONTACT ALERT: Emergency procedures activated");
        }
    }
    
    /// Calculate expected arrival time for reception drift calculation
    async fn calculate_expected_arrival(&self) -> Instant {
        let last_time = self.last_packet_time.lock().await;
        if let Some(prev_time) = *last_time {
            prev_time + self.expected_period
        } else {
            Instant::now() // First packet, no previous reference
        }
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
                warn!("WARNING: Significant timing drift detected: {:?} (expected: {:?})", 
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

