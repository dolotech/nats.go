package message

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// BenchmarkConnectionPool 连接池性能基准测试
func BenchmarkConnectionPool(b *testing.B) {
	s := startTestNATSServer(b)
	defer s.Shutdown()

	pool := createTestPool(b, s.ClientURL(), 0)
	defer pool.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ctx := context.Background()
			conn, err := pool.Get(ctx)
			if err != nil {
				b.Errorf("获取连接失败: %v", err)
				continue
			}
			pool.Put(conn)
		}
	})
}

// BenchmarkStreamProcessing 流式处理性能基准测试
func BenchmarkStreamProcessing(b *testing.B) {
	s := startTestNATSServer(b)
	defer s.Shutdown()

	pool := createTestPool(b, s.ClientURL(), 0)
	defer pool.Close()

	subscriber, err := NewSubscriber([]string{s.ClientURL()})
	if err != nil {
		b.Fatalf("创建订阅者失败: %v", err)
	}
	defer subscriber.Close(context.Background())

	// 启动流式处理器
	var processedCount int64
	handler := func(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
		atomic.AddInt64(&processedCount, 1)
		sender.Send([]byte("response"))
		return sender.End()
	}

	handle, err := subscriber.StreamQueueSubscribeHandlerAsync(
		context.Background(), "bench.stream", "workers", handler)
	if err != nil {
		b.Fatalf("启动异步订阅失败: %v", err)
	}
	defer handle.Stop()

	time.Sleep(100 * time.Millisecond) // 等待订阅生效

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			pool.StreamRequest(ctx, "bench.stream", []byte("test"), func(msg *nats.Msg) bool {
				return false // 收到响应后立即结束
			})
			cancel()
		}
	})

	b.Logf("处理消息总数: %d", atomic.LoadInt64(&processedCount))
}

// TestResourceManager 资源管理器测试
func TestResourceManager(t *testing.T) {
	s := startTestNATSServer(t)
	defer s.Shutdown()

	rm := NewResourceManager()
	defer rm.Close()

	// 创建并注册资源
	pool := createTestPool(t, s.ClientURL(), 0)
	subscriber, err := NewSubscriber([]string{s.ClientURL()})
	if err != nil {
		t.Fatalf("创建订阅者失败: %v", err)
	}

	rm.RegisterPool("test-pool", pool)
	rm.RegisterSubscriber("test-subscriber", subscriber)

	// 测试资源统计
	stats := rm.GetSystemStats()
	t.Logf("系统统计: %+v", stats)

	// 测试资源检查
	warnings := rm.CheckResourceLimits()
	if len(warnings) > 0 {
		t.Logf("资源警告: %v", warnings)
	}

	// 测试优化
	rm.OptimizeForThroughput()
	rm.OptimizeForLatency()

	// 清理
	pool.Close()
	subscriber.Close(context.Background())
}

// TestMemoryUsage 内存使用测试
func TestMemoryUsage(t *testing.T) {
	s := startTestNATSServer(t)
	defer s.Shutdown()

	var beforeMem, afterMem runtime.MemStats
	runtime.ReadMemStats(&beforeMem)

	// 创建大量连接和订阅
	pools := make([]*Pool, 10)
	subscribers := make([]*Subscriber, 10)

	for i := 0; i < 10; i++ {
		pools[i] = createTestPool(t, s.ClientURL(), 0)

		sub, err := NewSubscriber([]string{s.ClientURL()})
		if err != nil {
			t.Fatalf("创建订阅者失败: %v", err)
		}
		subscribers[i] = sub
	}

	// 执行一些操作
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				pool := pools[j%len(pools)]
				ctx := context.Background()
				if conn, err := pool.Get(ctx); err == nil {
					pool.Put(conn)
				}
			}
		}()
	}
	wg.Wait()

	// 清理资源
	for i := 0; i < 10; i++ {
		pools[i].Close()
		subscribers[i].Close(context.Background())
	}

	runtime.GC()
	runtime.ReadMemStats(&afterMem)

	t.Logf("内存使用情况:")
	t.Logf("  前: %d KB", beforeMem.Alloc/1024)
	t.Logf("  后: %d KB", afterMem.Alloc/1024)
	t.Logf("  增长: %d KB", (afterMem.Alloc-beforeMem.Alloc)/1024)
}

// TestGoroutineLeaks 协程泄露测试
func TestGoroutineLeaks(t *testing.T) {
	s := startTestNATSServer(t)
	defer s.Shutdown()

	beforeGoroutines := runtime.NumGoroutine()

	// 创建并启动多个异步订阅
	subscriber, err := NewSubscriber([]string{s.ClientURL()})
	if err != nil {
		t.Fatalf("创建订阅者失败: %v", err)
	}

	var handles []*StreamSubscriptionHandle
	handler := func(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
		return sender.End()
	}

	for i := 0; i < 10; i++ {
		subject := fmt.Sprintf("test.goroutine.%d", i)
		handle, err := subscriber.StreamQueueSubscribeHandlerAsync(
			context.Background(), subject, "workers", handler)
		if err != nil {
			t.Errorf("启动异步订阅失败: %v", err)
			continue
		}
		handles = append(handles, handle)
	}

	time.Sleep(200 * time.Millisecond) // 等待goroutine启动

	// 停止所有订阅
	for _, handle := range handles {
		if err := handle.Stop(); err != nil {
			t.Errorf("停止订阅失败: %v", err)
		}
	}

	subscriber.Close(context.Background())

	// 等待协程清理
	time.Sleep(500 * time.Millisecond)
	runtime.GC()

	afterGoroutines := runtime.NumGoroutine()
	diff := afterGoroutines - beforeGoroutines

	t.Logf("协程数量:")
	t.Logf("  前: %d", beforeGoroutines)
	t.Logf("  后: %d", afterGoroutines)
	t.Logf("  差异: %d", diff)

	// 允许一些合理的协程数量差异（如监控协程等）
	if diff > 5 {
		t.Errorf("可能存在协程泄露: 增加了 %d 个协程", diff)
	}
}

// TestConcurrentStreamProcessing 并发流式处理测试
func TestConcurrentStreamProcessing(t *testing.T) {
	s := startTestNATSServer(t)
	defer s.Shutdown()

	pool := createTestPool(t, s.ClientURL(), 0)
	defer pool.Close()

	subscriber, err := NewSubscriber([]string{s.ClientURL()})
	if err != nil {
		t.Fatalf("创建订阅者失败: %v", err)
	}
	defer subscriber.Close(context.Background())

	var processedCount int64
	var totalResponseTime int64

	handler := func(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
		start := time.Now()
		defer func() {
			atomic.AddInt64(&totalResponseTime, time.Since(start).Nanoseconds())
		}()

		atomic.AddInt64(&processedCount, 1)

		// 模拟一些处理时间
		time.Sleep(1 * time.Millisecond)

		for i := 0; i < 5; i++ {
			if err := sender.Send([]byte("response")); err != nil {
				return err
			}
		}
		return sender.End()
	}

	handle, err := subscriber.StreamQueueSubscribeHandlerAsync(
		context.Background(), "concurrent.stream", "workers", handler)
	if err != nil {
		t.Fatalf("启动异步订阅失败: %v", err)
	}
	defer handle.Stop()

	time.Sleep(100 * time.Millisecond) // 等待订阅生效

	// 并发发送请求
	const numRequests = 100
	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			var responseCount int
			err := pool.StreamRequest(ctx, "concurrent.stream", []byte("test"), func(msg *nats.Msg) bool {
				responseCount++
				return responseCount < 5 // 期望收到5个响应
			})
			if err != nil {
				t.Errorf("流式请求失败: %v", err)
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	processed := atomic.LoadInt64(&processedCount)
	avgResponseTime := atomic.LoadInt64(&totalResponseTime) / processed

	t.Logf("并发流式处理结果:")
	t.Logf("  请求数: %d", numRequests)
	t.Logf("  处理数: %d", processed)
	t.Logf("  总耗时: %v", duration)
	t.Logf("  平均耗时: %v", duration/time.Duration(numRequests))
	t.Logf("  平均响应时间: %v", time.Duration(avgResponseTime))
	t.Logf("  吞吐量: %.2f req/s", float64(numRequests)/duration.Seconds())

	// 验证所有请求都被处理
	if processed != numRequests {
		t.Errorf("期望处理 %d 个请求，实际处理 %d 个", numRequests, processed)
	}
}

// 辅助函数（这些函数可能在其他测试文件中已经定义，如果冲突请删除）
func startTestNATSServer(t testing.TB) *server.Server {
	opts := &server.Options{
		Host: "127.0.0.1",
		Port: -1, // 随机端口
	}
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("创建NATS服务器失败: %v", err)
	}

	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatalf("NATS服务器启动超时")
	}

	return s
}

func createTestPool(t testing.TB, url string, maxLife time.Duration) *Pool {
	cfg := Config{
		Servers:       []string{url},
		IdlePerServer: 16,
		DialTimeout:   5 * time.Second,
		MaxLife:       maxLife,
		BackoffMin:    100 * time.Millisecond,
		BackoffMax:    5 * time.Second,
	}

	pool, err := New(cfg)
	if err != nil {
		t.Fatalf("创建连接池失败: %v", err)
	}
	return pool
}
