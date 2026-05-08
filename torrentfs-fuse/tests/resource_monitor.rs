use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use sysinfo::System;

const MEMORY_WARNING_THRESHOLD: f64 = 80.0;
const MEMORY_CRITICAL_THRESHOLD: f64 = 90.0;
const CPU_WARNING_THRESHOLD: f64 = 80.0;

#[derive(Debug, Clone)]
pub struct ResourceSample {
    pub timestamp: Duration,
    pub memory_used_mb: f64,
    pub memory_total_mb: f64,
    pub memory_percent: f64,
    pub cpu_percent: f64,
    pub disk_read_bytes: u64,
    pub disk_write_bytes: u64,
}

#[allow(dead_code)]
impl ResourceSample {}

#[derive(Debug, Clone)]
pub struct ResourceThresholdAlert {
    pub timestamp: Duration,
    pub alert_type: AlertType,
    pub message: String,
    pub value: f64,
    pub threshold: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertType {
    MemoryWarning,
    MemoryCritical,
    CpuWarning,
    MemoryLeakSuspected,
}

impl std::fmt::Display for AlertType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertType::MemoryWarning => write!(f, "MEMORY_WARNING"),
            AlertType::MemoryCritical => write!(f, "MEMORY_CRITICAL"),
            AlertType::CpuWarning => write!(f, "CPU_WARNING"),
            AlertType::MemoryLeakSuspected => write!(f, "MEMORY_LEAK_SUSPECTED"),
        }
    }
}

#[derive(Debug)]
pub struct ResourceReport {
    pub samples: Vec<ResourceSample>,
    pub alerts: Vec<ResourceThresholdAlert>,
    pub test_duration: Duration,
    pub avg_memory_mb: f64,
    pub max_memory_mb: f64,
    pub min_memory_mb: f64,
    pub avg_cpu_percent: f64,
    pub max_cpu_percent: f64,
    pub memory_growth_mb: f64,
    pub potential_memory_leak: bool,
    pub total_disk_read_mb: f64,
    pub total_disk_write_mb: f64,
}

impl ResourceReport {
    pub fn print_summary(&self) {
        println!("\n=== Resource Monitoring Report ===");
        println!("Test duration: {:?}", self.test_duration);
        println!("\nMemory Statistics:");
        println!("  Average: {:.2} MB", self.avg_memory_mb);
        println!("  Minimum: {:.2} MB", self.min_memory_mb);
        println!("  Maximum: {:.2} MB", self.max_memory_mb);
        println!("  Growth: {:.2} MB", self.memory_growth_mb);
        
        println!("\nCPU Statistics:");
        println!("  Average: {:.2}%", self.avg_cpu_percent);
        println!("  Maximum: {:.2}%", self.max_cpu_percent);
        
        println!("\nDisk I/O:");
        println!("  Total Read: {:.2} MB", self.total_disk_read_mb);
        println!("  Total Write: {:.2} MB", self.total_disk_write_mb);
        
        if self.potential_memory_leak {
            println!("\n⚠️  WARNING: Potential memory leak detected!");
            println!("   Memory grew by {:.2} MB over the test duration.", self.memory_growth_mb);
        }
        
        if !self.alerts.is_empty() {
            println!("\nAlerts ({} total):", self.alerts.len());
            for alert in &self.alerts {
                println!("  [{:.1}s] {}: {} (value: {:.2}, threshold: {:.2})",
                    alert.timestamp.as_secs_f64(),
                    alert.alert_type,
                    alert.message,
                    alert.value,
                    alert.threshold
                );
            }
        } else {
            println!("\nNo threshold alerts triggered.");
        }
    }
}

pub struct ResourceMonitor {
    sys: System,
    pid: sysinfo::Pid,
    samples: Arc<Mutex<Vec<ResourceSample>>>,
    alerts: Arc<Mutex<Vec<ResourceThresholdAlert>>>,
    running: Arc<AtomicBool>,
    start_time: Instant,
    initial_memory_mb: f64,
    _last_disk_read: u64,
    _last_disk_write: u64,
}

impl ResourceMonitor {
    pub fn new() -> Self {
        let mut sys = System::new_all();
        sys.refresh_all();
        
        let pid = sysinfo::get_current_pid().expect("Failed to get current PID");
        let initial_memory_mb = Self::get_process_memory_mb(&sys, pid);
        
        let _disk_usage = 0u64;
        
        ResourceMonitor {
            sys,
            pid,
            samples: Arc::new(Mutex::new(Vec::new())),
            alerts: Arc::new(Mutex::new(Vec::new())),
            running: Arc::new(AtomicBool::new(false)),
            start_time: Instant::now(),
            initial_memory_mb,
            _last_disk_read: 0,
            _last_disk_write: 0,
        }
    }
    
    fn get_process_memory_mb(sys: &System, pid: sysinfo::Pid) -> f64 {
        sys.process(pid)
            .map(|p| p.memory() as f64 / 1_048_576.0)
            .unwrap_or(0.0)
    }
    
    fn get_process_cpu_percent(sys: &System, pid: sysinfo::Pid) -> f64 {
        sys.process(pid)
            .map(|p| p.cpu_usage() as f64)
            .unwrap_or(0.0)
    }
    
    fn get_total_memory_mb(sys: &System) -> f64 {
        sys.total_memory() as f64 / 1_048_576.0
    }
    
    fn get_available_memory_mb(sys: &System) -> f64 {
        sys.available_memory() as f64 / 1_048_576.0
    }
    
    pub fn start(&mut self, sample_interval: Duration) -> thread::JoinHandle<()> {
        self.running.store(true, Ordering::SeqCst);
        self.start_time = Instant::now();
        
        let samples = Arc::clone(&self.samples);
        let alerts = Arc::clone(&self.alerts);
        let running = Arc::clone(&self.running);
        let pid = self.pid;
        
        let initial_memory_mb = self.initial_memory_mb;
        let monitor_start_time = self.start_time;
        
        let handle = thread::spawn(move || {
            let mut sys = System::new_all();
            let mut last_memory_mb = initial_memory_mb;
            let mut increasing_count = 0usize;
            let sample_count_for_leak_detection = 10;
            
            while running.load(Ordering::SeqCst) {
                sys.refresh_all();
                
                let timestamp = monitor_start_time.elapsed();
                
                let memory_used_mb = Self::get_process_memory_mb(&sys, pid);
                let memory_total_mb = Self::get_total_memory_mb(&sys);
                let available_mb = Self::get_available_memory_mb(&sys);
                let memory_percent = if memory_total_mb > 0.0 {
                    ((memory_total_mb - available_mb) / memory_total_mb) * 100.0
                } else {
                    0.0
                };
                
                let cpu_percent = Self::get_process_cpu_percent(&sys, pid);
                
                let disk_read_bytes: u64 = sys.process(pid)
                    .map(|p| p.disk_usage().read_bytes)
                    .unwrap_or(0);
                let disk_write_bytes: u64 = sys.process(pid)
                    .map(|p| p.disk_usage().written_bytes)
                    .unwrap_or(0);
                
                let sample = ResourceSample {
                    timestamp,
                    memory_used_mb,
                    memory_total_mb,
                    memory_percent,
                    cpu_percent,
                    disk_read_bytes,
                    disk_write_bytes,
                };
                
                {
                    let mut samples_lock = samples.lock().unwrap();
                    samples_lock.push(sample.clone());
                }
                
                if memory_percent >= MEMORY_CRITICAL_THRESHOLD {
                    let alert = ResourceThresholdAlert {
                        timestamp: sample.timestamp,
                        alert_type: AlertType::MemoryCritical,
                        message: format!("System memory usage critical: {:.1}%", memory_percent),
                        value: memory_percent,
                        threshold: MEMORY_CRITICAL_THRESHOLD,
                    };
                    alerts.lock().unwrap().push(alert);
                } else if memory_percent >= MEMORY_WARNING_THRESHOLD {
                    let alert = ResourceThresholdAlert {
                        timestamp: sample.timestamp,
                        alert_type: AlertType::MemoryWarning,
                        message: format!("System memory usage high: {:.1}%", memory_percent),
                        value: memory_percent,
                        threshold: MEMORY_WARNING_THRESHOLD,
                    };
                    alerts.lock().unwrap().push(alert);
                }
                
                if cpu_percent >= CPU_WARNING_THRESHOLD {
                    let alert = ResourceThresholdAlert {
                        timestamp: sample.timestamp,
                        alert_type: AlertType::CpuWarning,
                        message: format!("CPU usage high: {:.1}%", cpu_percent),
                        value: cpu_percent,
                        threshold: CPU_WARNING_THRESHOLD,
                    };
                    alerts.lock().unwrap().push(alert);
                }
                
                if memory_used_mb > last_memory_mb {
                    increasing_count += 1;
                    if increasing_count >= sample_count_for_leak_detection {
                        let growth = memory_used_mb - initial_memory_mb;
                        if growth > 50.0 {
                            let alert = ResourceThresholdAlert {
                                timestamp: sample.timestamp,
                                alert_type: AlertType::MemoryLeakSuspected,
                                message: format!(
                                    "Memory consistently increasing. Growth: {:.2} MB",
                                    growth
                                ),
                                value: growth,
                                threshold: 50.0,
                            };
                            alerts.lock().unwrap().push(alert);
                            increasing_count = 0;
                        }
                    }
                } else {
                    increasing_count = increasing_count.saturating_sub(1);
                }
                last_memory_mb = memory_used_mb;
                
                thread::sleep(sample_interval);
            }
        });
        
        handle
    }
    
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }
    
    pub fn generate_report(&self) -> ResourceReport {
        let samples = self.samples.lock().unwrap().clone();
        let alerts = self.alerts.lock().unwrap().clone();
        
        if samples.is_empty() {
            return ResourceReport {
                samples: vec![],
                alerts,
                test_duration: Duration::ZERO,
                avg_memory_mb: 0.0,
                max_memory_mb: 0.0,
                min_memory_mb: 0.0,
                avg_cpu_percent: 0.0,
                max_cpu_percent: 0.0,
                memory_growth_mb: 0.0,
                potential_memory_leak: false,
                total_disk_read_mb: 0.0,
                total_disk_write_mb: 0.0,
            };
        }
        
        let test_duration = samples.last().map(|s| s.timestamp).unwrap_or(Duration::ZERO);
        
        let memory_values: Vec<f64> = samples.iter().map(|s| s.memory_used_mb).collect();
        let cpu_values: Vec<f64> = samples.iter().map(|s| s.cpu_percent).collect();
        
        let avg_memory_mb = memory_values.iter().sum::<f64>() / memory_values.len() as f64;
        let max_memory_mb = memory_values.iter().cloned().fold(0.0, f64::max);
        let min_memory_mb = memory_values.iter().cloned().fold(f64::INFINITY, f64::min);
        
        let avg_cpu_percent = cpu_values.iter().sum::<f64>() / cpu_values.len() as f64;
        let max_cpu_percent = cpu_values.iter().cloned().fold(0.0, f64::max);
        
        let initial_memory = samples.first().map(|s| s.memory_used_mb).unwrap_or(0.0);
        let final_memory = samples.last().map(|s| s.memory_used_mb).unwrap_or(0.0);
        let memory_growth_mb = final_memory - initial_memory;
        
        let potential_memory_leak = memory_growth_mb > 100.0 && {
            let third_count = samples.len() / 3;
            if third_count < 2 {
                false
            } else {
                let first_third_avg: f64 = samples[..third_count]
                    .iter()
                    .map(|s| s.memory_used_mb)
                    .sum::<f64>() / third_count as f64;
                let last_third_avg: f64 = samples[samples.len() - third_count..]
                    .iter()
                    .map(|s| s.memory_used_mb)
                    .sum::<f64>() / third_count as f64;
                last_third_avg > first_third_avg * 1.5
            }
        };
        
        let total_disk_read_mb = samples.last()
            .map(|s| s.disk_read_bytes as f64 / 1_048_576.0)
            .unwrap_or(0.0);
        let total_disk_write_mb = samples.last()
            .map(|s| s.disk_write_bytes as f64 / 1_048_576.0)
            .unwrap_or(0.0);
        
        ResourceReport {
            samples,
            alerts,
            test_duration,
            avg_memory_mb,
            max_memory_mb,
            min_memory_mb,
            avg_cpu_percent,
            max_cpu_percent,
            memory_growth_mb,
            potential_memory_leak,
            total_disk_read_mb,
            total_disk_write_mb,
        }
    }
    
    pub fn get_current_memory_mb(&mut self) -> f64 {
        self.sys.refresh_all();
        Self::get_process_memory_mb(&self.sys, self.pid)
    }
    
    #[allow(dead_code)]
    pub fn get_sample_count(&self) -> usize {
        self.samples.lock().unwrap().len()
    }
}

pub fn print_resource_chart(report: &ResourceReport, width: usize) {
    if report.samples.is_empty() {
        println!("No samples collected.");
        return;
    }
    
    println!("\n=== Memory Usage Chart ===");
    
    let max_memory = report.max_memory_mb.max(1.0);
    let chart_height = 10;
    
    for row in (0..chart_height).rev() {
        let threshold = (row as f64 / chart_height as f64) * max_memory;
        print!("{:8.1} MB |", threshold);
        
        for sample in &report.samples {
            let bar_height = (sample.memory_used_mb / max_memory * chart_height as f64) as usize;
            if bar_height > row {
                print!("█");
            } else {
                print!(" ");
            }
        }
        println!();
    }
    
    print!("          +");
    for _ in 0..report.samples.len().min(width) {
        print!("-");
    }
    println!(">");
    
    let step = report.samples.len() / 5;
    print!("           ");
    for i in 0..5 {
        if i * step < report.samples.len() {
            let sample = &report.samples[i * step];
            print!("{:5.0}s", sample.timestamp.as_secs());
        }
    }
    println!();
    
    println!("\n=== CPU Usage Chart ===");
    
    let max_cpu = report.max_cpu_percent.max(1.0);
    
    for row in (0..chart_height).rev() {
        let threshold = (row as f64 / chart_height as f64) * max_cpu;
        print!("{:6.1}% |", threshold);
        
        for sample in &report.samples {
            let bar_height = (sample.cpu_percent / max_cpu * chart_height as f64) as usize;
            if bar_height > row {
                print!("█");
            } else {
                print!(" ");
            }
        }
        println!();
    }
    
    print!("       +");
    for _ in 0..report.samples.len().min(width) {
        print!("-");
    }
    println!(">");
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_resource_monitor_basic() {
        let mut monitor = ResourceMonitor::new();
        let initial_memory = monitor.get_current_memory_mb();
        assert!(initial_memory >= 0.0);
    }
    
    #[test]
    fn test_resource_monitoring_duration() {
        let mut monitor = ResourceMonitor::new();
        let handle = monitor.start(Duration::from_millis(100));
        
        thread::sleep(Duration::from_millis(500));
        
        monitor.stop();
        handle.join().expect("Monitor thread should join");
        
        let report = monitor.generate_report();
        assert!(report.samples.len() >= 3, "Should have at least 3 samples");
        assert!(report.test_duration >= Duration::from_millis(400));
    }
}
