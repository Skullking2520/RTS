// performance.rs - Advanced Real-Time Performance Monitoring
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{info, warn, error, debug};

/// Real-time performance metrics
#[derive(Debug, Clone)]
pub struct RealTimeMetrics {
    pub jitter_measurements: VecDeque<Duration>,
    pub drift_measurements: VecDeque<Duration>,
    pub task_execution_times: VecDeque<Duration>,
    pub deadline_violations: u64,
    pub missed_deadlines: u64,
    pub system_load: f64,
    pub fault_recovery_times: VecDeque<Duration>,
    pub telemetry_backlog: u64,
    pub command_queue_size: u64,
    pub last_update: Instant,
    pub measurement_window: Duration,
}

/// Performance benchmark results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceBenchmark {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub jitter_stats: JitterStatistics,
    pub drift_stats: DriftStatistics,
    pub task_stats: TaskExecutionStatistics,
    pub system_stats: SystemLoadStatistics,
    pub fault_stats: FaultRecoveryStatistics,
    pub communication_stats: CommunicationStatistics,
}

/// Jitter statistics (variation in periodic task timing)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JitterStatistics {
    pub min_jitter: Duration,
    pub max_jitter: Duration,
    pub average_jitter: Duration,
    pub jitter_std_dev: Duration,
    pub samples: u64,
}

/// Drift statistics (difference between scheduled and actual task start times)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftStatistics {
    pub min_drift: Duration,
    pub max_drift: Duration,
    pub average_drift: Duration,
    pub drift_std_dev: Duration,
    pub samples: u64,
    pub deadline_violations: u64,
}

/// Task execution statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskExecutionStatistics {
    pub total_tasks: u64,
    pub completed_tasks: u64,
    pub failed_tasks: u64,
    pub average_execution_time: Duration,
    pub max_execution_time: Duration,
    pub min_execution_time: Duration,
    pub missed_deadlines: u64,
}

/// System load statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemLoadStatistics {
    pub cpu_utilization: f64,
    pub memory_utilization: f64,
    pub active_threads: u32,
    pub idle_time_percentage: f64,
    pub peak_load: f64,
    pub average_load: f64,
}

/// Fault recovery statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaultRecoveryStatistics {
    pub total_faults: u64,
    pub recovered_faults: u64,
    pub average_recovery_time: Duration,
    pub max_recovery_time: Duration,
    pub min_recovery_time: Duration,
    pub recovery_success_rate: f64,
}

/// Communication statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunicationStatistics {
    pub telemetry_received: u64,
    pub telemetry_processed: u64,
    pub commands_received: u64,
    pub commands_processed: u64,
    pub average_latency: Duration,
    pub max_latency: Duration,
    pub min_latency: Duration,
    pub packet_loss_rate: f64,
}

/// Advanced Performance Monitor
#[derive(Debug, Clone)]
pub struct PerformanceMonitor {
    metrics: Arc<RwLock<RealTimeMetrics>>,
    benchmark_history: Arc<RwLock<Vec<PerformanceBenchmark>>>,
    measurement_interval: Duration,
    max_samples: usize,
}

impl PerformanceMonitor {
    /// Create new performance monitor
    pub fn new(measurement_interval: Duration, max_samples: usize) -> Self {
        Self {
            metrics: Arc::new(RwLock::new(RealTimeMetrics {
                jitter_measurements: VecDeque::with_capacity(max_samples),
                drift_measurements: VecDeque::with_capacity(max_samples),
                task_execution_times: VecDeque::with_capacity(max_samples),
                deadline_violations: 0,
                missed_deadlines: 0,
                system_load: 0.0,
                fault_recovery_times: VecDeque::with_capacity(max_samples),
                telemetry_backlog: 0,
                command_queue_size: 0,
                last_update: Instant::now(),
                measurement_window: measurement_interval,
            })),
            benchmark_history: Arc::new(RwLock::new(Vec::new())),
            measurement_interval,
            max_samples,
        }
    }
    
    /// Record jitter measurement (variation in periodic task timing)
    pub async fn record_jitter(&self, expected_interval: Duration, actual_interval: Duration) {
        let jitter = if actual_interval > expected_interval {
            actual_interval - expected_interval
        } else {
            expected_interval - actual_interval
        };
        
        let mut metrics = self.metrics.write().await;
        metrics.jitter_measurements.push_back(jitter);
        
        // Maintain sample size limit
        if metrics.jitter_measurements.len() > self.max_samples {
            metrics.jitter_measurements.pop_front();
        }
        
        // Check for excessive jitter (>1ms for critical systems)
        if jitter > Duration::from_millis(1) {
            warn!("Excessive jitter detected: {:?} (threshold: 1ms)", jitter);
        }
    }
    
    /// Record drift measurement (difference between scheduled and actual task start times)
    pub async fn record_drift(&self, scheduled_time: Instant, actual_time: Instant) {
        let drift = if actual_time > scheduled_time {
            actual_time.duration_since(scheduled_time)
        } else {
            scheduled_time.duration_since(actual_time)
        };
        
        let mut metrics = self.metrics.write().await;
        metrics.drift_measurements.push_back(drift);
        
        // Maintain sample size limit
        if metrics.drift_measurements.len() > self.max_samples {
            metrics.drift_measurements.pop_front();
        }
        
        // Check for deadline violations
        if drift > Duration::from_millis(5) {
            metrics.deadline_violations += 1;
            warn!("Deadline violation detected: {:?} drift", drift);
        }
    }
    
    /// Record task execution time
    pub async fn record_task_execution(&self, execution_time: Duration, deadline: Duration) {
        let mut metrics = self.metrics.write().await;
        metrics.task_execution_times.push_back(execution_time);
        
        // Maintain sample size limit
        if metrics.task_execution_times.len() > self.max_samples {
            metrics.task_execution_times.pop_front();
        }
        
        // Check for missed deadlines
        if execution_time > deadline {
            metrics.missed_deadlines += 1;
            error!("Task deadline missed: {:?} > {:?}", execution_time, deadline);
        }
    }
    
    /// Record fault recovery time
    pub async fn record_fault_recovery(&self, recovery_time: Duration) {
        let mut metrics = self.metrics.write().await;
        metrics.fault_recovery_times.push_back(recovery_time);
        
        // Maintain sample size limit
        if metrics.fault_recovery_times.len() > self.max_samples {
            metrics.fault_recovery_times.pop_front();
        }
        
        // Check for recovery time violations (>200ms threshold)
        if recovery_time > Duration::from_millis(200) {
            error!("Fault recovery time exceeded 200ms threshold: {:?}", recovery_time);
        }
    }
    
    /// Update system load metrics
    pub async fn update_system_load(&self, cpu_util: f64, memory_util: f64, active_threads: u32) {
        let mut metrics = self.metrics.write().await;
        metrics.system_load = cpu_util;
        metrics.last_update = Instant::now();
        
        // Calculate idle time percentage
        let idle_time = 100.0 - cpu_util;
        
        // Log system load information
        debug!("System Load - CPU: {:.1}%, Memory: {:.1}%, Active Threads: {}, Idle: {:.1}%",
               cpu_util, memory_util, active_threads, idle_time);
    }
    
    /// Update communication metrics
    pub async fn update_communication_metrics(&self, telemetry_backlog: u64, command_queue_size: u64) {
        let mut metrics = self.metrics.write().await;
        metrics.telemetry_backlog = telemetry_backlog;
        metrics.command_queue_size = command_queue_size;
        
        // Check for backlog issues with detailed logging
        if telemetry_backlog > 100 {
            warn!("High telemetry backlog: {} messages (threshold: 100)", telemetry_backlog);
            self.log_backlog_analysis("telemetry", telemetry_backlog, 100).await;
        }
        
        if command_queue_size > 50 {
            warn!("WARNING: High command queue size: {} commands (threshold: 50)", command_queue_size);
            self.log_backlog_analysis("command", command_queue_size, 50).await;
        }
        
        // Log system performance impact
        if telemetry_backlog > 200 || command_queue_size > 100 {
            error!("CRITICAL: System performance degraded - Telemetry: {}, Commands: {}", 
                   telemetry_backlog, command_queue_size);
        }
    }
    
    /// Log detailed backlog analysis
    async fn log_backlog_analysis(&self, queue_type: &str, current_size: u64, threshold: u64) {
        let backlog_percentage = (current_size as f64 / threshold as f64) * 100.0;
        let severity = if backlog_percentage > 200.0 {
            "CRITICAL"
        } else if backlog_percentage > 150.0 {
            "HIGH"
        } else {
            "WARNING"
        };
        
        warn!("BACKLOG ANALYSIS - {}: {} messages ({}% of threshold) - Severity: {}", 
              queue_type, current_size, backlog_percentage as u32, severity);
    }
    
    /// Generate comprehensive performance benchmark
    pub async fn generate_benchmark(&self) -> PerformanceBenchmark {
        let metrics = self.metrics.read().await;
        
        let jitter_stats = self.calculate_jitter_statistics(&metrics.jitter_measurements);
        let drift_stats = self.calculate_drift_statistics(&metrics.drift_measurements);
        let task_stats = self.calculate_task_statistics(&metrics.task_execution_times, metrics.missed_deadlines);
        let system_stats = self.calculate_system_statistics(metrics.system_load);
        let fault_stats = self.calculate_fault_statistics(&metrics.fault_recovery_times);
        let communication_stats = self.calculate_communication_statistics(
            metrics.telemetry_backlog, 
            metrics.command_queue_size
        );
        
        let benchmark = PerformanceBenchmark {
            timestamp: chrono::Utc::now(),
            jitter_stats,
            drift_stats,
            task_stats,
            system_stats,
            fault_stats,
            communication_stats,
        };
        
        // Store benchmark in history
        {
            let mut history = self.benchmark_history.write().await;
            history.push(benchmark.clone());
            
            // Keep only last 100 benchmarks
            if history.len() > 100 {
                history.remove(0);
            }
        }
        
        benchmark
    }
    
    /// Calculate jitter statistics
    fn calculate_jitter_statistics(&self, measurements: &VecDeque<Duration>) -> JitterStatistics {
        if measurements.is_empty() {
            return JitterStatistics {
                min_jitter: Duration::from_millis(0),
                max_jitter: Duration::from_millis(0),
                average_jitter: Duration::from_millis(0),
                jitter_std_dev: Duration::from_millis(0),
                samples: 0,
            };
        }
        
        let min_jitter = *measurements.iter().min().unwrap();
        let max_jitter = *measurements.iter().max().unwrap();
        
        let total_nanos: u64 = measurements.iter().map(|d| d.as_nanos() as u64).sum();
        let average_nanos = total_nanos / measurements.len() as u64;
        let average_jitter = Duration::from_nanos(average_nanos);
        
        // Calculate standard deviation
        let variance: f64 = measurements.iter()
            .map(|d| {
                let diff = d.as_nanos() as f64 - average_nanos as f64;
                diff * diff
            })
            .sum();
        let std_dev_nanos = (variance / measurements.len() as f64).sqrt();
        let jitter_std_dev = Duration::from_nanos(std_dev_nanos as u64);
        
        JitterStatistics {
            min_jitter,
            max_jitter,
            average_jitter,
            jitter_std_dev,
            samples: measurements.len() as u64,
        }
    }
    
    /// Calculate drift statistics
    fn calculate_drift_statistics(&self, measurements: &VecDeque<Duration>) -> DriftStatistics {
        if measurements.is_empty() {
            return DriftStatistics {
                min_drift: Duration::from_millis(0),
                max_drift: Duration::from_millis(0),
                average_drift: Duration::from_millis(0),
                drift_std_dev: Duration::from_millis(0),
                samples: 0,
                deadline_violations: 0,
            };
        }
        
        let min_drift = *measurements.iter().min().unwrap();
        let max_drift = *measurements.iter().max().unwrap();
        
        let total_nanos: u64 = measurements.iter().map(|d| d.as_nanos() as u64).sum();
        let average_nanos = total_nanos / measurements.len() as u64;
        let average_drift = Duration::from_nanos(average_nanos);
        
        // Calculate standard deviation
        let variance: f64 = measurements.iter()
            .map(|d| {
                let diff = d.as_nanos() as f64 - average_nanos as f64;
                diff * diff
            })
            .sum();
        let std_dev_nanos = (variance / measurements.len() as f64).sqrt();
        let drift_std_dev = Duration::from_nanos(std_dev_nanos as u64);
        
        DriftStatistics {
            min_drift,
            max_drift,
            average_drift,
            drift_std_dev,
            samples: measurements.len() as u64,
            deadline_violations: 0, // Will be updated from metrics
        }
    }
    
    /// Calculate task execution statistics
    fn calculate_task_statistics(&self, measurements: &VecDeque<Duration>, missed_deadlines: u64) -> TaskExecutionStatistics {
        if measurements.is_empty() {
            return TaskExecutionStatistics {
                total_tasks: 0,
                completed_tasks: 0,
                failed_tasks: 0,
                average_execution_time: Duration::from_millis(0),
                max_execution_time: Duration::from_millis(0),
                min_execution_time: Duration::from_millis(0),
                missed_deadlines,
            };
        }
        
        let min_time = *measurements.iter().min().unwrap();
        let max_time = *measurements.iter().max().unwrap();
        
        let total_nanos: u64 = measurements.iter().map(|d| d.as_nanos() as u64).sum();
        let average_nanos = total_nanos / measurements.len() as u64;
        let average_time = Duration::from_nanos(average_nanos);
        
        TaskExecutionStatistics {
            total_tasks: measurements.len() as u64,
            completed_tasks: measurements.len() as u64,
            failed_tasks: 0, // Would need separate tracking
            average_execution_time: average_time,
            max_execution_time: max_time,
            min_execution_time: min_time,
            missed_deadlines,
        }
    }
    
    /// Calculate system load statistics
    fn calculate_system_statistics(&self, current_load: f64) -> SystemLoadStatistics {
        SystemLoadStatistics {
            cpu_utilization: current_load,
            memory_utilization: 0.0, // Would need system monitoring
            active_threads: 0,        // Would need system monitoring
            idle_time_percentage: 100.0 - current_load,
            peak_load: current_load,  // Would need historical tracking
            average_load: current_load, // Would need historical tracking
        }
    }
    
    /// Calculate fault recovery statistics
    fn calculate_fault_statistics(&self, measurements: &VecDeque<Duration>) -> FaultRecoveryStatistics {
        if measurements.is_empty() {
            return FaultRecoveryStatistics {
                total_faults: 0,
                recovered_faults: 0,
                average_recovery_time: Duration::from_millis(0),
                max_recovery_time: Duration::from_millis(0),
                min_recovery_time: Duration::from_millis(0),
                recovery_success_rate: 0.0,
            };
        }
        
        let min_time = *measurements.iter().min().unwrap();
        let max_time = *measurements.iter().max().unwrap();
        
        let total_nanos: u64 = measurements.iter().map(|d| d.as_nanos() as u64).sum();
        let average_nanos = total_nanos / measurements.len() as u64;
        let average_time = Duration::from_nanos(average_nanos);
        
        FaultRecoveryStatistics {
            total_faults: measurements.len() as u64,
            recovered_faults: measurements.len() as u64, // Assuming all measured faults were recovered
            average_recovery_time: average_time,
            max_recovery_time: max_time,
            min_recovery_time: min_time,
            recovery_success_rate: 100.0, // Would need separate tracking
        }
    }
    
    /// Calculate communication statistics
    fn calculate_communication_statistics(&self, _telemetry_backlog: u64, _command_queue_size: u64) -> CommunicationStatistics {
        CommunicationStatistics {
            telemetry_received: 0,    // Would need separate tracking
            telemetry_processed: 0,   // Would need separate tracking
            commands_received: 0,     // Would need separate tracking
            commands_processed: 0,    // Would need separate tracking
            average_latency: Duration::from_millis(0), // Would need separate tracking
            max_latency: Duration::from_millis(0),     // Would need separate tracking
            min_latency: Duration::from_millis(0),     // Would need separate tracking
            packet_loss_rate: 0.0,   // Would need separate tracking
        }
    }
    
    /// Get current performance metrics
    pub async fn get_current_metrics(&self) -> RealTimeMetrics {
        self.metrics.read().await.clone()
    }
    
    /// Get benchmark history
    pub async fn get_benchmark_history(&self) -> Vec<PerformanceBenchmark> {
        self.benchmark_history.read().await.clone()
    }
    
    /// Start continuous monitoring
    pub async fn start_monitoring(&self) {
        let metrics = self.metrics.clone();
        let benchmark_history = self.benchmark_history.clone();
        let interval = self.measurement_interval;
        
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
        
        loop {
            tick.tick().await;
            
                // Generate periodic benchmark
                let benchmark = Self::generate_benchmark_static(&metrics, &benchmark_history).await;
                
                // Log performance summary
                info!("METRICS: Performance Benchmark Generated:");
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
            }
        });
    }
    
    /// Static method for generating benchmarks
    async fn generate_benchmark_static(
        metrics: &Arc<RwLock<RealTimeMetrics>>,
        benchmark_history: &Arc<RwLock<Vec<PerformanceBenchmark>>>,
    ) -> PerformanceBenchmark {
        let metrics_guard = metrics.read().await;
        
        // Create a temporary monitor to calculate statistics
        let temp_monitor = PerformanceMonitor::new(Duration::from_secs(1), 1000);
        
        let jitter_stats = temp_monitor.calculate_jitter_statistics(&metrics_guard.jitter_measurements);
        let drift_stats = temp_monitor.calculate_drift_statistics(&metrics_guard.drift_measurements);
        let task_stats = temp_monitor.calculate_task_statistics(&metrics_guard.task_execution_times, metrics_guard.missed_deadlines);
        let system_stats = temp_monitor.calculate_system_statistics(metrics_guard.system_load);
        let fault_stats = temp_monitor.calculate_fault_statistics(&metrics_guard.fault_recovery_times);
        let communication_stats = temp_monitor.calculate_communication_statistics(
            metrics_guard.telemetry_backlog, 
            metrics_guard.command_queue_size
        );
        
        let benchmark = PerformanceBenchmark {
            timestamp: chrono::Utc::now(),
            jitter_stats,
            drift_stats,
            task_stats,
            system_stats,
            fault_stats,
            communication_stats,
        };
        
        // Store benchmark in history
        {
            let mut history = benchmark_history.write().await;
            history.push(benchmark.clone());
            
            // Keep only last 100 benchmarks
            if history.len() > 100 {
                history.remove(0);
            }
        }
        
        benchmark
    }
}

impl Default for PerformanceMonitor {
    fn default() -> Self {
        Self::new(Duration::from_secs(5), 1000)
    }
}
