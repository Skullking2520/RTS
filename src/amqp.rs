// amqp.rs - Enhanced AMQP Client for OCS-GCS Communication
use anyhow::Result;
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions, BasicQosOptions, ExchangeDeclareOptions,
        QueueBindOptions, QueueDeclareOptions,
    },
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn, error};

/// AMQP Client for OCS-GCS Communication
#[derive(Clone)]
pub struct AmqpClient {
    pub connection: Arc<Connection>,
    pub telemetry_channel: Arc<Mutex<Channel>>,
    pub command_channel: Arc<Mutex<Channel>>,
    pub sequence_counter: Arc<Mutex<u64>>,
}

impl AmqpClient {
    /// Connect to AMQP server and create client
    pub async fn connect(uri: &str) -> Result<Self> {
        info!("Attempting to connect to AMQP server: {}", uri);
        
        let connection = connect_with_retry(uri).await?;
        let telemetry_channel = connection.create_channel().await?;
        let command_channel = connection.create_channel().await?;
        
        info!("AMQP connection successful");
        
        Ok(Self {
            connection: Arc::new(connection),
            telemetry_channel: Arc::new(Mutex::new(telemetry_channel)),
            command_channel: Arc::new(Mutex::new(command_channel)),
            sequence_counter: Arc::new(Mutex::new(0)),
        })
    }
    
    /// Setup Exchange and queues
    pub async fn setup(&self) -> Result<()> {
        info!("Setting up AMQP Exchange and queues...");
        
        let telemetry_ch = self.telemetry_channel.lock().await;
        let command_ch = self.command_channel.lock().await;
        
        // Exchange setup - must match assignment A_final configuration
        let mut exchange_options = ExchangeDeclareOptions::default();
        exchange_options.durable = true;
        
        telemetry_ch.exchange_declare(
            "telemetry", 
            ExchangeKind::Topic, 
            exchange_options, 
            FieldTable::default()
        ).await?;
        
        // Telemetry queue setup (OCS → GCS) - must match assignment A_final configuration
        let mut telemetry_queue_options = QueueDeclareOptions::default();
        telemetry_queue_options.durable = true;
        
        telemetry_ch.queue_declare(
            "gcs_telem", 
            telemetry_queue_options, 
            FieldTable::default()
        ).await?;
        
        // Command queue setup (GCS → OCS) - must match assignment A_final configuration
        let mut command_queue_options = QueueDeclareOptions::default();
        command_queue_options.durable = true;
        
        command_ch.queue_declare(
            "ocs_cmd", 
            command_queue_options, 
            FieldTable::default()
        ).await?;
        
        // Binding setup: gcs_telem ↔ telemetry (ocs.*)
        telemetry_ch.queue_bind(
            "gcs_telem", 
            "telemetry", 
            "ocs.*", 
            QueueBindOptions::default(),
            FieldTable::default()
        ).await?;
        
        // QoS setup
        telemetry_ch.basic_qos(100, BasicQosOptions::default()).await?;
        command_ch.basic_qos(100, BasicQosOptions::default()).await?;
        
        info!("AMQP setup complete");
        info!("   Telemetry: gcs_telem ↔ telemetry (ocs.*)");
        info!("   Commands: ocs_cmd");
        
        Ok(())
    }
    
    /// Publish telemetry message (GCS → OCS)
    pub async fn publish_telemetry(&self, routing_key: &str, message: &[u8]) -> Result<()> {
        let channel = self.telemetry_channel.lock().await;
        
        channel.basic_publish(
            "telemetry",
            routing_key,
            BasicPublishOptions::default(),
            message,
            BasicProperties::default(),
        ).await?.await?;
        
        info!("Published telemetry: {} → {}", routing_key, String::from_utf8_lossy(message));
        Ok(())
    }
    
    /// Publish command message (GCS → OCS)
    pub async fn publish_command(&self, message: &[u8]) -> Result<()> {
        let channel = self.command_channel.lock().await;
        
        channel.basic_publish(
            "",
            "ocs_cmd",
            BasicPublishOptions::default(),
            message,
            BasicProperties::default(),
        ).await?.await?;
        
        info!("Published command: ocs_cmd → {}", String::from_utf8_lossy(message));
        Ok(())
    }
    
    /// Create telemetry consumer (OCS → GCS)
    pub async fn create_telemetry_consumer(&self) -> Result<lapin::Consumer> {
        let channel = self.telemetry_channel.lock().await;
        
        let consumer = channel.basic_consume(
            "gcs_telem",
            "gcs_telemetry_consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        ).await?;
        
        info!("Telemetry consumer created");
        Ok(consumer)
    }
    
    /// Create command consumer (OCS → GCS)
    pub async fn create_command_consumer(&self) -> Result<lapin::Consumer> {
        let channel = self.command_channel.lock().await;
        
        let consumer = channel.basic_consume(
            "ocs_cmd",
            "gcs_command_consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        ).await?;
        
        info!("Command consumer created");
        Ok(consumer)
    }
    
    /// Get next sequence number
    pub async fn next_sequence(&self) -> u64 {
        let mut counter = self.sequence_counter.lock().await;
        *counter += 1;
        *counter
    }
    
    /// Check connection status
    pub fn is_connected(&self) -> bool {
        self.connection.status().connected()
    }
}

/// Connection function with retry logic
pub async fn connect_with_retry(uri: &str) -> Result<Connection> {
    let mut delay_ms = 500u64;
    let max_attempts = 10;
    let mut attempts = 0;
    
    loop {
        attempts += 1;
        info!("Attempting connection {}/{}: {}", attempts, max_attempts, uri);
        
        match Connection::connect(uri, ConnectionProperties::default()).await {
            Ok(connection) => {
                info!("AMQP connection successful (attempt {})", attempts);
                return Ok(connection);
            }
            Err(e) => {
                warn!("Connection failed (attempt {}): {}", attempts, e);
                
                if attempts >= max_attempts {
                    error!("Maximum retry attempts exceeded");
                    return Err(e.into());
                }
                
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                delay_ms = (delay_ms.saturating_mul(2)).min(8_000);
            }
        }
    }
}

/// Message acknowledgement (ACK)
pub async fn ack(delivery: &lapin::message::Delivery) -> Result<()> {
    delivery.ack(BasicAckOptions::default()).await?;
    Ok(())
}

/// Message rejection and deletion (NACK)
pub async fn nack_drop(delivery: &lapin::message::Delivery) -> Result<()> {
    delivery.nack(BasicNackOptions { requeue: false, multiple: false }).await?;
    Ok(())
}

/// Message rejection and requeue (NACK with requeue)
pub async fn nack_requeue(delivery: &lapin::message::Delivery) -> Result<()> {
    delivery.nack(BasicNackOptions { requeue: true, multiple: false }).await?;
    Ok(())
}
