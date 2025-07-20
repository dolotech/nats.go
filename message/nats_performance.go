package message

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// ==================== 性能优化和资源管理 ====================

// ResourceManager 资源管理器
type ResourceManager struct {
	// 池引用
	pools       map[string]*Pool
	subscribers map[string]*Subscriber

	// 资源限制
	maxGoroutines int64
	maxMemoryMB   int64

	// 统计
	totalGoroutines int64
	peakGoroutines  int64

	// 控制
	mu            sync.RWMutex
	ctx           context.Context
	cancel        context.CancelFunc
	cleanupTicker *time.Ticker
}

// NewResourceManager 创建资源管理器
func NewResourceManager() *ResourceManager {
	ctx, cancel := context.WithCancel(context.Background())

	rm := &ResourceManager{
		pools:         make(map[string]*Pool),
		subscribers:   make(map[string]*Subscriber),
		maxGoroutines: int64(runtime.GOMAXPROCS(0) * 1000), // 默认限制
		maxMemoryMB:   2048,                                // 2GB默认限制
		ctx:           ctx,
		cancel:        cancel,
		cleanupTicker: time.NewTicker(1 * time.Minute),
	}

	go rm.resourceMonitor()
	return rm
}

// RegisterPool 注册连接池
func (rm *ResourceManager) RegisterPool(name string, pool *Pool) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.pools[name] = pool
	zap.S().Infof("注册连接池: %s", name)
}

// RegisterSubscriber 注册订阅者
func (rm *ResourceManager) RegisterSubscriber(name string, subscriber *Subscriber) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.subscribers[name] = subscriber
	zap.S().Infof("注册订阅者: %s", name)
}

// GetSystemStats 获取系统统计信息
func (rm *ResourceManager) GetSystemStats() map[string]interface{} {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	currentGoroutines := int64(runtime.NumGoroutine())
	if currentGoroutines > atomic.LoadInt64(&rm.peakGoroutines) {
		atomic.StoreInt64(&rm.peakGoroutines, currentGoroutines)
	}

	stats := map[string]interface{}{
		"goroutines": map[string]interface{}{
			"current": currentGoroutines,
			"peak":    atomic.LoadInt64(&rm.peakGoroutines),
			"limit":   rm.maxGoroutines,
		},
		"memory": map[string]interface{}{
			"alloc_mb":      m.Alloc / 1024 / 1024,
			"sys_mb":        m.Sys / 1024 / 1024,
			"heap_alloc_mb": m.HeapAlloc / 1024 / 1024,
			"heap_sys_mb":   m.HeapSys / 1024 / 1024,
			"limit_mb":      rm.maxMemoryMB,
		},
		"gc": map[string]interface{}{
			"num_gc":          m.NumGC,
			"pause_ns":        m.PauseNs[(m.NumGC+255)%256],
			"gc_cpu_fraction": m.GCCPUFraction,
		},
		"pools":       len(rm.pools),
		"subscribers": len(rm.subscribers),
	}

	return stats
}

// CheckResourceLimits 检查资源限制
func (rm *ResourceManager) CheckResourceLimits() []string {
	var warnings []string

	// 检查goroutine数量
	currentGoroutines := int64(runtime.NumGoroutine())
	if currentGoroutines > rm.maxGoroutines {
		warnings = append(warnings,
			fmt.Sprintf("Goroutine数量超限: %d > %d", currentGoroutines, rm.maxGoroutines))
	}

	// 检查内存使用
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	allocMB := m.Alloc / 1024 / 1024
	if int64(allocMB) > rm.maxMemoryMB {
		warnings = append(warnings,
			fmt.Sprintf("内存使用超限: %d MB > %d MB", allocMB, rm.maxMemoryMB))
	}

	// 检查连接池健康度
	rm.mu.RLock()
	for name, pool := range rm.pools {
		metrics := pool.GetMetrics()
		if metrics.ConnectionLeaks > 10 {
			warnings = append(warnings,
				fmt.Sprintf("连接池 %s 泄露过多: %d", name, metrics.ConnectionLeaks))
		}

		// 检查连接利用率
		if metrics.TotalConnections > 0 {
			utilization := float64(metrics.BorrowedConnections) / float64(metrics.TotalConnections)
			if utilization > 0.9 {
				warnings = append(warnings,
					fmt.Sprintf("连接池 %s 利用率过高: %.2f%%", name, utilization*100))
			}
		}
	}

	// 检查订阅者状态
	for name, subscriber := range rm.subscribers {
		metrics := subscriber.GetMetrics()
		if metrics.MessagesDropped > 1000 {
			warnings = append(warnings,
				fmt.Sprintf("订阅者 %s 丢弃消息过多: %d", name, metrics.MessagesDropped))
		}

		if metrics.MessagesFailed > 100 {
			warnings = append(warnings,
				fmt.Sprintf("订阅者 %s 处理失败过多: %d", name, metrics.MessagesFailed))
		}
	}
	rm.mu.RUnlock()

	return warnings
}

// ForceGC 强制垃圾回收（谨慎使用）
func (rm *ResourceManager) ForceGC() {
	before := runtime.NumGoroutine()
	runtime.GC()
	runtime.GC() // 连续两次确保彻底清理
	after := runtime.NumGoroutine()

	zap.S().Infof("强制GC完成: goroutines %d -> %d", before, after)
}

// OptimizeForThroughput 针对高吞吐量优化
func (rm *ResourceManager) OptimizeForThroughput() {
	// 调整GC目标
	runtime.GC()

	// 建议增加连接池大小
	rm.mu.RLock()
	for name, pool := range rm.pools {
		metrics := pool.GetMetrics()
		utilization := float64(metrics.BorrowedConnections) / float64(metrics.TotalConnections)
		if utilization > 0.8 {
			zap.S().Warnf("建议增加连接池 %s 的大小，当前利用率: %.2f%%", name, utilization*100)
		}
	}
	rm.mu.RUnlock()

	zap.S().Info("已应用高吞吐量优化建议")
}

// OptimizeForLatency 针对低延迟优化
func (rm *ResourceManager) OptimizeForLatency() {
	// 预热连接池
	rm.mu.RLock()
	for name, pool := range rm.pools {
		if pool == nil {
			continue // 跳过空的连接池
		}
		go func(poolName string, p *Pool) {
			// 预创建一些连接
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			for i := 0; i < 3; i++ { // 减少预热数量避免过度消耗
				if conn, err := p.Get(ctx); err == nil {
					if conn != nil {
						p.Put(conn) // 立即归还，目的是预热
					}
				} else {
					zap.S().Debugf("连接池 %s 预热第 %d 次失败: %v", poolName, i+1, err)
					break // 失败就停止预热
				}
			}
			zap.S().Debugf("连接池 %s 预热完成", poolName)
		}(name, pool)
	}
	rm.mu.RUnlock()

	zap.S().Info("已应用低延迟优化")
}

// Cleanup 清理无用资源
func (rm *ResourceManager) Cleanup() {
	runtime.GC()

	// 清理连接池中的过期连接
	rm.mu.RLock()
	for name, pool := range rm.pools {
		go func(poolName string, p *Pool) {
			// 这里可以添加连接池的清理逻辑
			zap.S().Debugf("清理连接池 %s", poolName)
		}(name, pool)
	}
	rm.mu.RUnlock()
}

// resourceMonitor 资源监控循环
func (rm *ResourceManager) resourceMonitor() {
	defer rm.cleanupTicker.Stop()

	for {
		select {
		case <-rm.ctx.Done():
			return
		case <-rm.cleanupTicker.C:
			warnings := rm.CheckResourceLimits()
			for _, warning := range warnings {
				zap.S().Warn(warning)
			}

			// 定期清理
			rm.Cleanup()
		}
	}
}

// Close 关闭资源管理器
func (rm *ResourceManager) Close() {
	rm.cancel()

	// 关闭所有注册的资源
	rm.mu.Lock()
	defer rm.mu.Unlock()

	for name, pool := range rm.pools {
		pool.Close()
		zap.S().Infof("关闭连接池: %s", name)
	}

	for name, subscriber := range rm.subscribers {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		subscriber.Close(ctx)
		cancel()
		zap.S().Infof("关闭订阅者: %s", name)
	}

	zap.S().Info("资源管理器已关闭")
}

// ==================== 内存池优化 ====================

// BytePool 字节池，减少内存分配
type BytePool struct {
	small  sync.Pool // 小缓冲区 (< 1KB)
	medium sync.Pool // 中等缓冲区 (1KB - 64KB)
	large  sync.Pool // 大缓冲区 (> 64KB)
}

// NewBytePool 创建字节池
func NewBytePool() *BytePool {
	return &BytePool{
		small: sync.Pool{
			New: func() interface{} {
				buf := make([]byte, 1024)
				return &buf
			},
		},
		medium: sync.Pool{
			New: func() interface{} {
				buf := make([]byte, 64*1024)
				return &buf
			},
		},
		large: sync.Pool{
			New: func() interface{} {
				buf := make([]byte, 1024*1024)
				return &buf
			},
		},
	}
}

// Get 获取缓冲区
func (bp *BytePool) Get(size int) *[]byte {
	switch {
	case size <= 1024:
		return bp.small.Get().(*[]byte)
	case size <= 64*1024:
		return bp.medium.Get().(*[]byte)
	default:
		return bp.large.Get().(*[]byte)
	}
}

// Put 归还缓冲区
func (bp *BytePool) Put(buf *[]byte, size int) {
	if buf == nil {
		return
	}

	// 重置长度但保持容量
	*buf = (*buf)[:cap(*buf)]

	switch {
	case size <= 1024:
		bp.small.Put(buf)
	case size <= 64*1024:
		bp.medium.Put(buf)
	default:
		bp.large.Put(buf)
	}
}

// 全局字节池实例
var GlobalBytePool = NewBytePool()

// ==================== 批量操作优化 ====================

// BatchProcessor 批量处理器
type BatchProcessor struct {
	batchSize    int
	flushTimeout time.Duration
	processor    func([]interface{}) error

	buffer []interface{}
	mu     sync.Mutex
	timer  *time.Timer
	ctx    context.Context
	cancel context.CancelFunc
}

// NewBatchProcessor 创建批量处理器
func NewBatchProcessor(batchSize int, flushTimeout time.Duration, processor func([]interface{}) error) *BatchProcessor {
	ctx, cancel := context.WithCancel(context.Background())

	bp := &BatchProcessor{
		batchSize:    batchSize,
		flushTimeout: flushTimeout,
		processor:    processor,
		buffer:       make([]interface{}, 0, batchSize),
		ctx:          ctx,
		cancel:       cancel,
	}

	return bp
}

// Add 添加项目到批次
func (bp *BatchProcessor) Add(item interface{}) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	bp.buffer = append(bp.buffer, item)

	// 如果是第一个项目，启动定时器
	if len(bp.buffer) == 1 && bp.timer == nil {
		bp.timer = time.AfterFunc(bp.flushTimeout, bp.flush)
	}

	// 如果达到批次大小，立即处理
	if len(bp.buffer) >= bp.batchSize {
		bp.flushLocked()
	}

	return nil
}

// flush 刷新批次（定时器回调）
func (bp *BatchProcessor) flush() {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.flushLocked()
}

// flushLocked 刷新批次（需要持有锁）
func (bp *BatchProcessor) flushLocked() {
	if len(bp.buffer) == 0 {
		return
	}

	// 停止定时器
	if bp.timer != nil {
		bp.timer.Stop()
		bp.timer = nil
	}

	// 处理批次
	batch := make([]interface{}, len(bp.buffer))
	copy(batch, bp.buffer)
	bp.buffer = bp.buffer[:0] // 重置缓冲区

	// 异步处理，避免阻塞
	go func() {
		if err := bp.processor(batch); err != nil {
			zap.S().Errorf("批量处理失败: %v", err)
		}
	}()
}

// Close 关闭批量处理器
func (bp *BatchProcessor) Close() {
	bp.cancel()

	// 处理剩余项目
	bp.mu.Lock()
	if len(bp.buffer) > 0 {
		bp.flushLocked()
	}
	bp.mu.Unlock()
}
