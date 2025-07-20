package message

import (
	"context"
	"runtime"
	"sync"
	"time"

	"go.uber.org/zap"
)

// -------------------- 监控指标 --------------------

// SystemMetrics 系统级监控指标
type SystemMetrics struct {
	// 连接池指标
	PoolMetrics PoolMetrics

	// 订阅者指标
	SubscriberMetrics SubscriberMetrics

	// 流式处理指标
	StreamMetrics StreamMetrics

	// 系统资源指标
	ResourceMetrics ResourceMetrics
}

// SubscriberMetrics 订阅者指标
type SubscriberMetrics struct {
	ActiveSubscriptions   int64 // 活跃订阅数
	TotalMessagesReceived int64 // 总接收消息数
	MessagesProcessed     int64 // 已处理消息数
	MessagesFailed        int64 // 处理失败消息数
	MessagesDropped       int64 // 丢弃消息数
	ProcessingLatency     int64 // 平均处理延迟（纳秒）
	QueueDepth            int64 // 队列深度
}

// StreamMetrics 流式处理指标
type StreamMetrics struct {
	ActiveStreams       int64 // 活跃流数量
	TotalStreamRequests int64 // 总流式请求数
	StreamsCompleted    int64 // 已完成流数量
	StreamsFailed       int64 // 失败流数量
	StreamsTimeout      int64 // 超时流数量
	AvgStreamDuration   int64 // 平均流持续时间（毫秒）
	DataThroughput      int64 // 数据吞吐量（字节/秒）
}

// ResourceMetrics 资源使用指标
type ResourceMetrics struct {
	MemoryUsage    int64 // 内存使用量（字节）
	GoroutineCount int64 // 协程数量
	CPUUsage       int64 // CPU使用率（千分比）
	NetworkIn      int64 // 网络入流量（字节）
	NetworkOut     int64 // 网络出流量（字节）
	LastGCDuration int64 // 上次GC耗时（纳秒）
}

// -------------------- 监控器 --------------------

// Monitor 系统监控器
type Monitor struct {
	pool       *Pool
	subscriber *Subscriber

	// 指标存储
	poolMetrics       *PoolMetrics
	subscriberMetrics *SubscriberMetrics
	streamMetrics     *StreamMetrics
	resourceMetrics   *ResourceMetrics

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.RWMutex

	// 配置
	collectInterval time.Duration
	alertThresholds AlertThresholds
}

// AlertThresholds 告警阈值配置
type AlertThresholds struct {
	MaxConnectionLeaks int64         // 最大连接泄露数
	MaxMessageLatency  time.Duration // 最大消息处理延迟
	MaxQueueDepth      int64         // 最大队列深度
	MaxMemoryUsage     int64         // 最大内存使用量（字节）
	MaxGoroutineCount  int64         // 最大协程数
	MinThroughput      int64         // 最小吞吐量（消息/秒）
}

// DefaultAlertThresholds 默认告警阈值
func DefaultAlertThresholds() AlertThresholds {
	return AlertThresholds{
		MaxConnectionLeaks: 10,
		MaxMessageLatency:  5 * time.Second,
		MaxQueueDepth:      100000,
		MaxMemoryUsage:     1 * 1024 * 1024 * 1024, // 1GB
		MaxGoroutineCount:  10000,
		MinThroughput:      100, // 100 msg/s
	}
}

// NewMonitor 创建新的监控器
func NewMonitor(pool *Pool, subscriber *Subscriber) *Monitor {
	ctx, cancel := context.WithCancel(context.Background())
	return &Monitor{
		pool:              pool,
		subscriber:        subscriber,
		poolMetrics:       &PoolMetrics{},
		subscriberMetrics: &SubscriberMetrics{},
		streamMetrics:     &StreamMetrics{},
		resourceMetrics:   &ResourceMetrics{},
		ctx:               ctx,
		cancel:            cancel,
		collectInterval:   10 * time.Second,
		alertThresholds:   DefaultAlertThresholds(),
	}
}

// Start 启动监控
func (m *Monitor) Start() {
	go m.collectMetrics()
	go m.checkAlerts()
	zap.S().Info("监控器已启动")
}

// Stop 停止监控
func (m *Monitor) Stop() {
	m.cancel()
	zap.S().Info("监控器已停止")
}

// GetSystemMetrics 获取系统指标
func (m *Monitor) GetSystemMetrics() SystemMetrics {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return SystemMetrics{
		PoolMetrics:       *m.poolMetrics,
		SubscriberMetrics: *m.subscriberMetrics,
		StreamMetrics:     *m.streamMetrics,
		ResourceMetrics:   *m.resourceMetrics,
	}
}

// collectMetrics 收集指标
func (m *Monitor) collectMetrics() {
	ticker := time.NewTicker(m.collectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.updateMetrics()
		}
	}
}

// updateMetrics 更新指标
func (m *Monitor) updateMetrics() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 更新连接池指标
	if m.pool != nil {
		*m.poolMetrics = m.pool.GetMetrics()
	}

	// 更新订阅者指标
	if m.subscriber != nil {
		m.updateSubscriberMetrics()
	}

	// 更新资源指标
	m.updateResourceMetrics()
}

// updateSubscriberMetrics 更新订阅者指标
func (m *Monitor) updateSubscriberMetrics() {
	// 这里可以添加订阅者的具体指标收集逻辑
	// 目前的实现中，Subscriber没有内置指标，可以扩展
}

// updateResourceMetrics 更新资源指标
func (m *Monitor) updateResourceMetrics() {
	// 简化的资源指标收集
	// 在实际生产环境中，可以集成更详细的系统监控
	var m_stats runtime.MemStats
	runtime.ReadMemStats(&m_stats)

	m.resourceMetrics.MemoryUsage = int64(m_stats.Alloc)
	m.resourceMetrics.GoroutineCount = int64(runtime.NumGoroutine())
	m.resourceMetrics.LastGCDuration = int64(m_stats.PauseNs[(m_stats.NumGC+255)%256])
}

// checkAlerts 检查告警
func (m *Monitor) checkAlerts() {
	ticker := time.NewTicker(30 * time.Second) // 每30秒检查一次告警
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.evaluateAlerts()
		}
	}
}

// evaluateAlerts 评估告警条件
func (m *Monitor) evaluateAlerts() {
	metrics := m.GetSystemMetrics()
	thresholds := m.alertThresholds

	// 检查连接泄露
	if metrics.PoolMetrics.ConnectionLeaks > thresholds.MaxConnectionLeaks {
		zap.S().Warnf("连接泄露告警: 当前泄露=%d, 阈值=%d",
			metrics.PoolMetrics.ConnectionLeaks, thresholds.MaxConnectionLeaks)
	}

	// 检查内存使用
	if metrics.ResourceMetrics.MemoryUsage > thresholds.MaxMemoryUsage {
		zap.S().Warnf("内存使用告警: 当前使用=%d MB, 阈值=%d MB",
			metrics.ResourceMetrics.MemoryUsage/1024/1024,
			thresholds.MaxMemoryUsage/1024/1024)
	}

	// 检查协程数量
	if metrics.ResourceMetrics.GoroutineCount > thresholds.MaxGoroutineCount {
		zap.S().Warnf("协程数量告警: 当前数量=%d, 阈值=%d",
			metrics.ResourceMetrics.GoroutineCount, thresholds.MaxGoroutineCount)
	}

	// 检查连接失败率
	if metrics.PoolMetrics.FailedDials > 0 && metrics.PoolMetrics.SuccessfulDials > 0 {
		failureRate := float64(metrics.PoolMetrics.FailedDials) /
			float64(metrics.PoolMetrics.FailedDials+metrics.PoolMetrics.SuccessfulDials)
		if failureRate > 0.1 { // 10%失败率告警
			zap.S().Warnf("连接失败率告警: 失败率=%.2f%%, 失败=%d, 成功=%d",
				failureRate*100, metrics.PoolMetrics.FailedDials, metrics.PoolMetrics.SuccessfulDials)
		}
	}
}

// -------------------- 健康检查 --------------------

// HealthStatus 健康状态
type HealthStatus struct {
	Overall   string                 `json:"overall"` // HEALTHY, DEGRADED, UNHEALTHY
	Timestamp time.Time              `json:"timestamp"`
	Details   map[string]interface{} `json:"details"`
}

// HealthChecker 健康检查器
type HealthChecker struct {
	monitor *Monitor
}

// NewHealthChecker 创建健康检查器
func NewHealthChecker(monitor *Monitor) *HealthChecker {
	return &HealthChecker{monitor: monitor}
}

// CheckHealth 执行健康检查
func (hc *HealthChecker) CheckHealth() HealthStatus {
	metrics := hc.monitor.GetSystemMetrics()
	details := make(map[string]interface{})
	issues := 0

	// 检查连接池健康状态
	if metrics.PoolMetrics.TotalConnections == 0 {
		details["pool"] = "No active connections"
		issues++
	} else if metrics.PoolMetrics.ConnectionLeaks > 5 {
		details["pool"] = "Connection leaks detected"
		issues++
	} else {
		details["pool"] = "Healthy"
	}

	// 检查内存状态
	memUsageMB := metrics.ResourceMetrics.MemoryUsage / 1024 / 1024
	if memUsageMB > 500 { // 500MB
		details["memory"] = map[string]interface{}{
			"status":   "High usage",
			"usage_mb": memUsageMB,
		}
		issues++
	} else {
		details["memory"] = map[string]interface{}{
			"status":   "Normal",
			"usage_mb": memUsageMB,
		}
	}

	// 检查协程状态
	goroutineCount := metrics.ResourceMetrics.GoroutineCount
	if goroutineCount > 1000 {
		details["goroutines"] = map[string]interface{}{
			"status": "High count",
			"count":  goroutineCount,
		}
		issues++
	} else {
		details["goroutines"] = map[string]interface{}{
			"status": "Normal",
			"count":  goroutineCount,
		}
	}

	// 确定整体健康状态
	overall := "HEALTHY"
	if issues > 2 {
		overall = "UNHEALTHY"
	} else if issues > 0 {
		overall = "DEGRADED"
	}

	return HealthStatus{
		Overall:   overall,
		Timestamp: time.Now(),
		Details:   details,
	}
}

// -------------------- 性能分析器 --------------------

// PerformanceProfiler 性能分析器
type PerformanceProfiler struct {
	monitor *Monitor

	// 性能历史数据
	throughputHistory []float64
	latencyHistory    []time.Duration
	mu                sync.RWMutex
}

// NewPerformanceProfiler 创建性能分析器
func NewPerformanceProfiler(monitor *Monitor) *PerformanceProfiler {
	return &PerformanceProfiler{
		monitor:           monitor,
		throughputHistory: make([]float64, 0, 100), // 保留最近100个数据点
		latencyHistory:    make([]time.Duration, 0, 100),
	}
}

// RecordThroughput 记录吞吐量
func (pp *PerformanceProfiler) RecordThroughput(msgPerSec float64) {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	pp.throughputHistory = append(pp.throughputHistory, msgPerSec)
	if len(pp.throughputHistory) > 100 {
		pp.throughputHistory = pp.throughputHistory[1:]
	}
}

// RecordLatency 记录延迟
func (pp *PerformanceProfiler) RecordLatency(latency time.Duration) {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	pp.latencyHistory = append(pp.latencyHistory, latency)
	if len(pp.latencyHistory) > 100 {
		pp.latencyHistory = pp.latencyHistory[1:]
	}
}

// GetPerformanceReport 获取性能报告
func (pp *PerformanceProfiler) GetPerformanceReport() map[string]interface{} {
	pp.mu.RLock()
	defer pp.mu.RUnlock()

	report := make(map[string]interface{})

	// 计算吞吐量统计
	if len(pp.throughputHistory) > 0 {
		var sum, max, min float64 = 0, 0, pp.throughputHistory[0]
		for _, v := range pp.throughputHistory {
			sum += v
			if v > max {
				max = v
			}
			if v < min {
				min = v
			}
		}
		avg := sum / float64(len(pp.throughputHistory))

		report["throughput"] = map[string]interface{}{
			"avg":     avg,
			"max":     max,
			"min":     min,
			"samples": len(pp.throughputHistory),
		}
	}

	// 计算延迟统计
	if len(pp.latencyHistory) > 0 {
		var sum, max, min time.Duration = 0, 0, pp.latencyHistory[0]
		for _, v := range pp.latencyHistory {
			sum += v
			if v > max {
				max = v
			}
			if v < min {
				min = v
			}
		}
		avg := sum / time.Duration(len(pp.latencyHistory))

		report["latency"] = map[string]interface{}{
			"avg_ms":  avg.Milliseconds(),
			"max_ms":  max.Milliseconds(),
			"min_ms":  min.Milliseconds(),
			"samples": len(pp.latencyHistory),
		}
	}

	return report
}
