// main.rs - Enhanced GCS Main Function for OCS-GCS Communication
use anyhow::Result;
use tokio::sync::mpsc;
use tracing::{info, warn, error, debug};
use tracing_subscriber::{EnvFilter, prelude::*};
use std::time::Duration;

mod config;
mod types;
mod amqp;
mod metrics;
mod uplink;
mod telemetry;
mod safety;
mod performance;

use config::Config;
use metrics::Metrics;
use amqp::AmqpClient;
use telemetry::TelemetryTask;
use uplink::UplinkTask;
use types::ResponseMessage;
use safety::SafetyInterlockManager;
use performance::PerformanceMonitor;

#[tokio::main]
async fn main() -> Result<()> {
    // Enhanced logging setup
    setup_logging()?;
    
    info!("Enhanced GCS (Ground Control Station) System Starting");
    info!("========================================================");
    info!("System Requirements:");
    info!("   - 3ms telemetry decoding deadline");
    info!("   - 2ms urgent command dispatch");
    info!("   - 100ms fault response threshold");
    info!("   - Real-time performance monitoring");
    info!("   - Advanced safety interlocks");
    info!("========================================================");
    
    // Load configuration
    let config = Config::load();
    config.print_summary();
    
    // Initialize AMQP client and connection
    let amqp_client = AmqpClient::connect(&config.amqp_uri).await?;
    info!("AMQP server connection successful");
    
    // Setup exchanges and queues
    amqp_client.setup().await?;
    info!("AMQP configuration complete");
    
    // Initialize enhanced metrics system
    let metrics = Metrics::default();
    metrics.update_connection_status(true).await;
    
    // Initialize Performance Monitor
    let performance_monitor = PerformanceMonitor::new(Duration::from_secs(5), 1000);
    info!("Performance monitoring system initialized");
    
    // Start performance monitoring
    performance_monitor.start_monitoring().await;
    info!("Continuous performance monitoring started");
    
    // Initialize Safety Interlock Manager
    let safety_manager = SafetyInterlockManager::new();
    
    // Activate test interlock (simulation)
    safety_manager.activate_interlock(
        safety::InterlockType::Thermal,
        "Thermal system fault detected".to_string(),
        7,
        vec!["system_reset".to_string(), "calibration".to_string()],
    ).await?;
    
    // Create response message channel
    let (response_tx, mut response_rx) = mpsc::channel::<ResponseMessage>(100);
    
    // Initialize enhanced telemetry processing task
    let telemetry_task = TelemetryTask::new(
        amqp_client.clone(),
        metrics.clone(),
        response_tx.clone(),
        Some(performance_monitor.clone()),
    );
    
    // Clone for fault simulation and telemetry simulation
    let telemetry_task_clone = telemetry_task.clone();
    let telemetry_task_sim_clone = telemetry_task.clone();

    let telemetry_handle = tokio::spawn(async move {
        if let Err(e) = telemetry_task.run("gcs_telem", config.decode_budget).await {
            error!("Telemetry task failed: {}", e);
        }
    });
    
    // Initialize enhanced command processing task
    let uplink_task = UplinkTask::new(
        amqp_client.clone(),
        metrics.clone(),
        response_tx.clone(),
        Some(performance_monitor.clone()),
    );
    
    // Clone for command simulation
    let uplink_task_sim_clone = uplink_task.clone();
    
    let uplink_handle = tokio::spawn(async move {
        if let Err(e) = uplink_task.run("ocs_cmd", config.dispatch_budget).await {
            error!("Command processing task failed: {}", e);
        }
    });
    
    // Enhanced metrics reporting task with scheduling drift logging
    let _metrics_clone = metrics.clone();
    let performance_monitor_clone = performance_monitor.clone();
    let metrics_period = config.metrics_period;
    let metrics_handle = tokio::spawn(async move {
        let mut tick = tokio::time::interval(metrics_period);
        let mut last_tick_time = std::time::Instant::now();
        
        loop {
            let scheduled_time = last_tick_time + metrics_period;
            tick.tick().await;
            let actual_time = std::time::Instant::now();
            
            // Record scheduling drift for metrics reporting task
            performance_monitor_clone.record_drift(scheduled_time, actual_time).await;
            
            // Generate comprehensive performance report
            let benchmark = performance_monitor_clone.generate_benchmark().await;
            info!("Performance Benchmark Report:");
            info!("   Jitter: {:?} ± {:?} (samples: {})", 
                  benchmark.jitter_stats.average_jitter, 
                  benchmark.jitter_stats.jitter_std_dev,
                  benchmark.jitter_stats.samples);
            info!("   Drift: {:?} ± {:?} (violations: {})", 
                  benchmark.drift_stats.average_drift,
                  benchmark.drift_stats.drift_std_dev,
                  benchmark.drift_stats.deadline_violations);
            info!("   Tasks: {}/{} completed, {:.1}% deadline adherence", 
                  benchmark.task_stats.completed_tasks,
                  benchmark.task_stats.total_tasks,
                  if benchmark.task_stats.total_tasks > 0 {
                      (benchmark.task_stats.completed_tasks as f64 / benchmark.task_stats.total_tasks as f64) * 100.0
                  } else {
                      0.0
                  });
            info!("   System Load: {:.1}%", benchmark.system_stats.average_load);
            info!("   Fault Recovery: {:?} avg, {} total", 
                  benchmark.fault_stats.average_recovery_time,
                  benchmark.fault_stats.total_faults);
            
            // Log scheduling drift details
            let drift = if actual_time > scheduled_time {
                actual_time.duration_since(scheduled_time)
            } else {
                scheduled_time.duration_since(actual_time)
            };
            
            if drift > Duration::from_millis(5) {
                warn!("WARNING: Metrics task scheduling drift: {:?} (scheduled: {:?}, actual: {:?})", 
                      drift, scheduled_time, actual_time);
            } else {
                debug!("SUCCESS: Metrics task on time: drift {:?}", drift);
            }
            
            last_tick_time = actual_time;
        }
    });
    
    // Enhanced safety monitoring task
    let safety_manager = SafetyInterlockManager::new_with_performance_monitor(Some(performance_monitor.clone()));
    let safety_manager_clone = safety_manager.clone();
    let safety_handle = tokio::spawn(async move {
        if let Err(e) = safety_manager_clone.safety_monitoring_loop().await {
            error!("Safety monitoring task failed: {}", e);
        }
    });
    
    // Enhanced connection health check task
    let amqp_client_clone = amqp_client.clone();
    let metrics_clone = metrics.clone();
    let health_handle = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(30));
        loop {
            tick.tick().await;
            
            // Check AMQP connection health
            if amqp_client_clone.is_connected() {
                metrics_clone.update_connection_status(true).await;
                info!("AMQP connection healthy");
            } else {
                metrics_clone.update_connection_status(false).await;
                warn!("AMQP connection unhealthy, attempting reconnection");
                // if let Err(e) = amqp_client_clone.reconnect().await {
                //     error!("Reconnection failed: {}", e);
                // }
            }
        }
    });
    
    // Response handling task
    let response_handle = tokio::spawn(async move {
        while let Some(response) = response_rx.recv().await {
            info!("Processing response: {:?}", response.header.message_type);
            // Handle response message
        }
    });
    
    // System load monitoring task
    let performance_monitor_clone = performance_monitor.clone();
    let load_handle = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        loop {
            tick.tick().await;
            
            // Simulate system load monitoring
            let cpu_util = std::thread::available_parallelism()
                .map(|n| n.get() as f64)
                .unwrap_or(1.0) * 0.7; // Simulate 70% CPU load
            let memory_util = 0.6; // Simulate 60% memory usage
            let active_threads = 8; // Simulate 8 active threads
            
            performance_monitor_clone.update_system_load(cpu_util, memory_util, active_threads).await;
            
            // Update communication metrics
            performance_monitor_clone.update_communication_metrics(100, 50).await;
        }
    });
    
    // Fault simulation task
    let fault_handle = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(30)); // Simulate faults every 30 seconds
        loop {
            tick.tick().await;
            
            if let Err(e) = telemetry_task_clone.simulate_fault_messages().await {
                error!("Fault simulation failed: {}", e);
            }
        }
    });
    
    // Telemetry simulation task
    let telemetry_sim_handle = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(10)); // Simulate telemetry every 10 seconds
        loop {
            tick.tick().await;
            
            if let Err(e) = telemetry_task_sim_clone.simulate_telemetry_messages().await {
                error!("Telemetry simulation failed: {}", e);
            }
        }
    });
    
    // Command simulation task
    let command_sim_handle = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(15)); // Simulate commands every 15 seconds
        loop {
            tick.tick().await;
            
            if let Err(e) = uplink_task_sim_clone.simulate_command_messages().await {
                error!("Command simulation failed: {}", e);
            }
        }
    });
    
    // Safety interlock simulation task
    let safety_manager_sim_clone = safety_manager.clone();
    let safety_sim_handle = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(45)); // Simulate interlocks every 45 seconds
        loop {
            tick.tick().await;
            
            if let Err(e) = safety_manager_sim_clone.simulate_safety_interlocks().await {
                error!("Safety interlock simulation failed: {}", e);
            }
        }
    });
    
    info!("All GCS tasks started successfully");
    info!("Waiting for shutdown signal...");
    
    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    info!("Shutdown signal received, terminating tasks...");
    
    // Graceful shutdown
    telemetry_handle.abort();
    uplink_handle.abort();
    metrics_handle.abort();
    safety_handle.abort();
    health_handle.abort();
    response_handle.abort();
    load_handle.abort();
    fault_handle.abort();
    telemetry_sim_handle.abort();
    command_sim_handle.abort();
    safety_sim_handle.abort();
    
    info!("GCS system shutdown complete");
    Ok(())
}

/// Enhanced logging setup with structured logging and file output
fn setup_logging() -> Result<()> {
    use tracing_appender::rolling;
    use std::fs;
    
    // Create logs directory if it doesn't exist
    if let Err(_) = fs::create_dir_all("logs") {
        eprintln!("Warning: Could not create logs directory");
    }
    
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));
    
    // Create file appender for logs
    let file_appender = rolling::daily("logs", "gcs.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    
    // Setup console logging
    let console_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .with_ansi(true);
    
    // Setup file logging
    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking)
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .with_ansi(false);
    
    // Combine console and file logging
    tracing_subscriber::registry()
        .with(filter)
        .with(console_layer)
        .with(file_layer)
        .init();
    
    // Keep the guard alive
    std::mem::forget(_guard);
    
    Ok(())
}
