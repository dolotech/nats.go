package message

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	// 替换为你的包路径
)

/* ---------- 测试工具 ---------- */

func runNATSServer(t *testing.T) *server.Server {
	opts := test.DefaultTestOptions
	opts.Port = -1 // 随机端口
	s := test.RunServer(&opts)
	t.Cleanup(s.Shutdown)
	return s
}

func newPool(t *testing.T, url string, ttl time.Duration) *Pool {
	cfg := Config{
		Servers:       []string{url},
		IdlePerServer: 8,
		MaxLife:       ttl,
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	t.Cleanup(p.Close)
	return p
}

/* ---------- 单元测试 ---------- */

func TestPublishRequest(t *testing.T) {
	s := runNATSServer(t)
	pool := newPool(t, s.ClientURL(), 0)

	// 应答者
	nc, _ := nats.Connect(s.ClientURL())
	nc.Subscribe("echo", func(m *nats.Msg) { m.Respond(m.Data) })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	msg, err := pool.Request(ctx, "echo", []byte("hello"))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("bad reply: %s", msg.Data)
	}
}

func TestConnectionReuseAndTTL(t *testing.T) {
	s := runNATSServer(t)
	pool := newPool(t, s.ClientURL(), 500*time.Millisecond)

	ctx := context.Background()
	c1, _ := pool.Get(ctx)
	pool.Put(c1)

	// 等到 TTL 过期
	time.Sleep(700 * time.Millisecond)

	c2, _ := pool.Get(ctx)
	pool.Put(c2)

	if c1 == c2 {
		t.Fatalf("connection should have been renewed after TTL")
	}
}

func TestConcurrentPublish(t *testing.T) {
	s := runNATSServer(t)
	pool := newPool(t, s.ClientURL(), 0)

	const n = 10000
	var ok int32

	// 订阅累加器
	nc, _ := nats.Connect(s.ClientURL())
	nc.Subscribe("inc", func(_ *nats.Msg) { atomic.AddInt32(&ok, 1) })

	ctx := context.Background()
	wg := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer func() { wg <- struct{}{} }()
			pool.Publish(ctx, "inc", []byte(fmt.Sprintf("%d", i)))
		}(i)
	}
	// 等待
	for i := 0; i < n; i++ {
		<-wg
	}
	// 给 NATS flush
	nc.Flush()

	if ok != n {
		t.Fatalf("expected %d messages, got %d", n, ok)
	}
}

func TestSubscriberQueue(t *testing.T) {
	s := runNATSServer(t)

	subA, _ := NewSubscriber([]string{s.ClientURL()})
	subB, _ := NewSubscriber([]string{s.ClientURL()})
	defer subA.Close(context.Background())
	defer subB.Close(context.Background())

	var aCnt, bCnt int32

	subA.QueueSubscribe("jobs", "workers", func(*nats.Msg) { atomic.AddInt32(&aCnt, 1) })
	subB.QueueSubscribe("jobs", "workers", func(*nats.Msg) { atomic.AddInt32(&bCnt, 1) })

	nc, _ := nats.Connect(s.ClientURL())
	for i := 0; i < 100000; i++ {
		nc.Publish("jobs", nil)
	}
	nc.Flush()

	time.Sleep(500 * time.Millisecond)
	if aCnt+bCnt != 100 {
		t.Fatalf("queue delivery mismatch: %d + %d", aCnt, bCnt)
	}
}

func TestServerDownRecovery(t *testing.T) {
	s := runNATSServer(t)
	pool := newPool(t, s.ClientURL(), 0)

	// 停服
	s.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if _, err := pool.Get(ctx); err == nil {
		t.Fatalf("expected error when server down")
	}

	// 重启
	s = runNATSServer(t)
	defer s.Shutdown()

	ctx2, c2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer c2()
	if _, err := pool.Get(ctx2); err != nil {
		t.Fatalf("should reconnect after server restart: %v", err)
	}
}

// TestStreamRequestResponse 测试流式请求-响应功能
func TestStreamRequestResponse(t *testing.T) {
	s := runNATSServer(t)
	pool := newPool(t, s.ClientURL(), 0)

	t.Run("基本流式请求-响应", func(t *testing.T) {
		var receivedCount int32

		// 创建流式响应处理器
		subscriber, err := NewSubscriber([]string{s.ClientURL()})
		if err != nil {
			t.Fatalf("创建订阅者失败: %v", err)
		}
		defer subscriber.Close(context.Background())

		// 启动流式响应处理器
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// 流式响应处理器 - 模拟生成数据流
		responseHandler := func(requestMsg *nats.Msg, responseIndex int) ([]byte, bool, error) {
			if responseIndex >= 10 { // 发送10条响应后结束
				return nil, false, nil
			}

			responseData := fmt.Sprintf("流式响应_%d_请求数据:%s", responseIndex, string(requestMsg.Data))
			return []byte(responseData), true, nil
		}

		// 启动流式订阅处理器
		go func() {
			err := subscriber.StreamSubscribeHandler(ctx, "stream.data", responseHandler)
			if err != nil && err != context.Canceled {
				t.Errorf("流式订阅处理器错误: %v", err)
			}
		}()

		// 等待订阅器启动
		time.Sleep(100 * time.Millisecond)

		// 发起流式请求
		requestCtx, requestCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer requestCancel()

		requestData := []byte("测试请求数据")

		// 流式响应处理器
		responseProcessor := func(msg *nats.Msg) bool {
			data := string(msg.Data)

			// 检查是否是结束信号
			if data == "__STREAM_END__" {
				t.Logf("收到流式结束信号")
				return false
			}

			// 检查是否是错误信号
			if strings.HasPrefix(data, "__STREAM_ERROR__") {
				t.Errorf("收到流式错误信号: %s", data)
				return false
			}

			atomic.AddInt32(&receivedCount, 1)
			t.Logf("收到流式响应: %s", data)

			// 验证响应格式
			if !strings.Contains(data, "流式响应_") || !strings.Contains(data, "测试请求数据") {
				t.Errorf("响应格式不正确: %s", data)
			}

			return true
		}

		// 发起流式请求
		err = pool.StreamRequest(requestCtx, "stream.data", requestData, responseProcessor)
		if err != nil {
			t.Fatalf("流式请求失败: %v", err)
		}

		// 验证收到的响应数量
		received := atomic.LoadInt32(&receivedCount)
		t.Logf("总共收到响应数: %d", received)

		if received != 10 {
			t.Errorf("期望收到10个响应，实际收到%d个", received)
		}
	})

	t.Run("带超时的流式请求", func(t *testing.T) {
		var receivedCount int32

		// 创建慢响应处理器
		subscriber, err := NewSubscriber([]string{s.ClientURL()})
		if err != nil {
			t.Fatalf("创建订阅者失败: %v", err)
		}
		defer subscriber.Close(context.Background())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// 慢响应处理器 - 每个响应延迟500ms
		slowResponseHandler := func(requestMsg *nats.Msg, responseIndex int) ([]byte, bool, error) {
			time.Sleep(500 * time.Millisecond) // 模拟慢处理

			if responseIndex >= 20 { // 尝试发送很多响应
				return nil, false, nil
			}

			responseData := fmt.Sprintf("慢响应_%d", responseIndex)
			return []byte(responseData), true, nil
		}

		// 启动慢响应处理器
		go func() {
			err := subscriber.StreamSubscribeHandler(ctx, "slow.stream", slowResponseHandler)
			if err != nil && err != context.Canceled {
				t.Errorf("慢响应处理器错误: %v", err)
			}
		}()

		time.Sleep(100 * time.Millisecond)

		// 发起带超时的流式请求
		requestData := []byte("超时测试")

		responseProcessor := func(msg *nats.Msg) bool {
			data := string(msg.Data)
			if data == "__STREAM_END__" {
				return false
			}
			if strings.HasPrefix(data, "__STREAM_ERROR__") {
				return false
			}

			atomic.AddInt32(&receivedCount, 1)
			t.Logf("收到慢响应: %s", data)
			return true
		}

		// 使用较短的超时时间
		err = pool.StreamRequestWithTimeout(context.Background(), "slow.stream", requestData,
			200*time.Millisecond, 2*time.Second, responseProcessor)

		// 应该因为超时而结束
		if err == nil {
			t.Error("期望超时错误，但请求正常完成")
		}

		received := atomic.LoadInt32(&receivedCount)
		t.Logf("超时前收到响应数: %d", received)

		// 因为超时，收到的响应数应该少于预期
		if received >= 20 {
			t.Errorf("超时测试异常，收到了过多响应: %d", received)
		}
	})

	t.Run("批量流式请求-响应", func(t *testing.T) {
		var receivedCount int32
		var batchCount int32

		subscriber, err := NewSubscriber([]string{s.ClientURL()})
		if err != nil {
			t.Fatalf("创建订阅者失败: %v", err)
		}
		defer subscriber.Close(context.Background())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// 批量响应处理器 - 每批生成5条数据
		batchResponseHandler := func(requestMsg *nats.Msg, batchIndex int) ([][]byte, bool, error) {
			if batchIndex >= 3 { // 发送3批数据
				return nil, false, nil
			}

			var batchData [][]byte
			for i := 0; i < 5; i++ {
				data := fmt.Sprintf("批量响应_批次%d_项目%d", batchIndex, i)
				batchData = append(batchData, []byte(data))
			}

			return batchData, true, nil
		}

		// 启动批量响应处理器
		go func() {
			err := subscriber.BatchStreamSubscribeHandler(ctx, "batch.stream", batchResponseHandler)
			if err != nil && err != context.Canceled {
				t.Errorf("批量响应处理器错误: %v", err)
			}
		}()

		time.Sleep(100 * time.Millisecond)

		// 发起流式请求
		requestCtx, requestCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer requestCancel()

		requestData := []byte("批量测试请求")

		responseProcessor := func(msg *nats.Msg) bool {
			data := string(msg.Data)

			if data == "__STREAM_END__" {
				t.Logf("收到批量流式结束信号")
				return false
			}

			if strings.HasPrefix(data, "__STREAM_ERROR__") {
				t.Errorf("收到批量流式错误信号: %s", data)
				return false
			}

			atomic.AddInt32(&receivedCount, 1)

			// 检查是否是新批次的第一个项目
			if strings.Contains(data, "_项目0") {
				atomic.AddInt32(&batchCount, 1)
			}

			t.Logf("收到批量响应: %s", data)
			return true
		}

		err = pool.StreamRequest(requestCtx, "batch.stream", requestData, responseProcessor)
		if err != nil {
			t.Fatalf("批量流式请求失败: %v", err)
		}

		received := atomic.LoadInt32(&receivedCount)
		batches := atomic.LoadInt32(&batchCount)

		t.Logf("总共收到响应数: %d, 批次数: %d", received, batches)

		if received != 15 { // 3批 * 5项 = 15
			t.Errorf("期望收到15个响应，实际收到%d个", received)
		}
		if batches != 3 {
			t.Errorf("期望收到3个批次，实际收到%d个", batches)
		}
	})
}

// TestStreamRequestRetry 测试流式请求重试功能
func TestStreamRequestRetry(t *testing.T) {
	s := runNATSServer(t)
	pool := newPool(t, s.ClientURL(), 0)

	// 简化的重试测试 - 测试重试机制本身
	t.Run("基本重试功能", func(t *testing.T) {
		var attemptCount int32

		// 创建总是失败的响应处理器
		subscriber, err := NewSubscriber([]string{s.ClientURL()})
		if err != nil {
			t.Fatalf("创建订阅者失败: %v", err)
		}
		defer subscriber.Close(context.Background())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// 总是失败的响应处理器
		alwaysFailHandler := func(requestMsg *nats.Msg, responseIndex int) ([]byte, bool, error) {
			attempt := atomic.AddInt32(&attemptCount, 1)
			t.Logf("收到请求尝试 #%d", attempt)
			return nil, false, fmt.Errorf("模拟服务不可用_%d", attempt)
		}

		go func() {
			err := subscriber.StreamSubscribeHandler(ctx, "always.fail", alwaysFailHandler)
			if err != nil && err != context.Canceled {
				t.Errorf("失败处理器错误: %v", err)
			}
		}()

		time.Sleep(100 * time.Millisecond)

		var errorCount int32
		responseProcessor := func(msg *nats.Msg) bool {
			data := string(msg.Data)
			if strings.HasPrefix(data, "__STREAM_ERROR__") {
				atomic.AddInt32(&errorCount, 1)
				t.Logf("收到错误信号: %s", data)
				return false
			}
			return true
		}

		// 发起重试请求，应该失败3次然后放弃
		requestData := []byte("重试测试")
		err = pool.StreamRequestWithRetry(context.Background(), "always.fail", requestData,
			2, 100*time.Millisecond, responseProcessor) // 最多重试2次

		// 应该最终失败
		if err == nil {
			t.Error("期望重试最终失败，但请求成功了")
		}

		attempts := atomic.LoadInt32(&attemptCount)
		errors := atomic.LoadInt32(&errorCount)

		t.Logf("重试尝试次数: %d, 收到错误次数: %d", attempts, errors)

		// 验证重试次数 (1次初始 + 2次重试 = 3次)
		if attempts < 3 {
			t.Errorf("期望至少3次尝试，实际%d次", attempts)
		}
	})

	t.Run("重试后成功", func(t *testing.T) {
		var attemptCount int32
		var successCount int32

		subscriber, err := NewSubscriber([]string{s.ClientURL()})
		if err != nil {
			t.Fatalf("创建订阅者失败: %v", err)
		}
		defer subscriber.Close(context.Background())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// 前2次失败，第3次成功的处理器
		retryThenSuccessHandler := func(requestMsg *nats.Msg, responseIndex int) ([]byte, bool, error) {
			attempt := atomic.AddInt32(&attemptCount, 1)
			t.Logf("收到请求尝试 #%d", attempt)

			if attempt <= 2 {
				return nil, false, fmt.Errorf("模拟临时失败_%d", attempt)
			}

			// 第3次成功，发送2个响应
			if responseIndex >= 2 {
				return nil, false, nil
			}

			atomic.AddInt32(&successCount, 1)
			responseData := fmt.Sprintf("成功响应_%d", responseIndex)
			return []byte(responseData), true, nil
		}

		go func() {
			err := subscriber.StreamSubscribeHandler(ctx, "retry.success", retryThenSuccessHandler)
			if err != nil && err != context.Canceled {
				t.Errorf("重试成功处理器错误: %v", err)
			}
		}()

		time.Sleep(100 * time.Millisecond)

		var errorCount int32
		var responseCount int32
		responseProcessor := func(msg *nats.Msg) bool {
			data := string(msg.Data)

			if strings.HasPrefix(data, "__STREAM_ERROR__") {
				atomic.AddInt32(&errorCount, 1)
				t.Logf("收到错误信号: %s", data)
				return false
			}

			if data == "__STREAM_END__" {
				t.Logf("收到结束信号")
				return false
			}

			atomic.AddInt32(&responseCount, 1)
			t.Logf("收到成功响应: %s", data)
			return true
		}

		// 发起重试请求
		requestData := []byte("重试测试")
		err = pool.StreamRequestWithRetry(context.Background(), "retry.success", requestData,
			3, 200*time.Millisecond, responseProcessor)

		if err != nil {
			t.Logf("重试请求结果: %v", err)
		}

		attempts := atomic.LoadInt32(&attemptCount)
		errors := atomic.LoadInt32(&errorCount)
		responses := atomic.LoadInt32(&responseCount)
		successes := atomic.LoadInt32(&successCount)

		t.Logf("尝试次数: %d, 错误次数: %d, 响应次数: %d, 成功处理次数: %d", attempts, errors, responses, successes)

		// 验证重试逻辑
		if attempts < 3 {
			t.Errorf("期望至少3次尝试，实际%d次", attempts)
		}
		if errors < 2 {
			t.Errorf("期望至少2次错误，实际%d次", errors)
		}
	})
}
