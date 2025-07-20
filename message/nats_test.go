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

	received := atomic.LoadInt32(&ok)
	if received != n {
		t.Fatalf("expected %d messages, got %d", n, received)
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
	totalReceived := atomic.LoadInt32(&aCnt) + atomic.LoadInt32(&bCnt)
	if totalReceived != 100000 {
		t.Fatalf("queue delivery mismatch: %d + %d = %d, expected 100000",
			atomic.LoadInt32(&aCnt), atomic.LoadInt32(&bCnt), totalReceived)
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

	t.Run("基本流式请求-响应（回调模式）", func(t *testing.T) {
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

		// 回调式流式响应处理器 - 模拟生成数据流
		responseHandler := func(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
			// 在异步协程中处理
			go func() {
				defer sender.End()

				for i := 0; i < 10; i++ {
					select {
					case <-ctx.Done():
						sender.SendError(ctx.Err())
						return
					default:
						if sender.IsClosed() {
							return
						}

						responseData := fmt.Sprintf("流式响应_%d_请求数据:%s", i, string(requestMsg.Data))
						if err := sender.Send([]byte(responseData)); err != nil {
							sender.SendError(err)
							return
						}

						// 短暂延迟模拟实际处理时间
						time.Sleep(20 * time.Millisecond)
					}
				}
			}()

			return nil // 立即返回，异步处理
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

	t.Run("带超时的流式请求（回调模式）", func(t *testing.T) {
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
		slowResponseHandler := func(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
			go func() {
				defer sender.End()

				for i := 0; i < 20; i++ { // 尝试发送20个响应
					select {
					case <-ctx.Done():
						sender.SendError(ctx.Err())
						return
					default:
						if sender.IsClosed() {
							return
						}

						// 模拟慢处理
						time.Sleep(500 * time.Millisecond)

						responseData := fmt.Sprintf("慢响应_%d", i)
						if err := sender.Send([]byte(responseData)); err != nil {
							sender.SendError(err)
							return
						}
					}
				}
			}()

			return nil
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

	t.Run("第三方API模拟测试", func(t *testing.T) {
		var receivedCount int32

		subscriber, err := NewSubscriber([]string{s.ClientURL()})
		if err != nil {
			t.Fatalf("创建订阅者失败: %v", err)
		}
		defer subscriber.Close(context.Background())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// 模拟调用第三方API的处理器
		thirdPartyAPIHandler := func(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
			// 异步调用第三方API
			go func() {
				defer sender.End()

				// 模拟第三方API调用延迟
				select {
				case <-ctx.Done():
					sender.SendError(ctx.Err())
					return
				case <-time.After(100 * time.Millisecond):
					// 继续处理
				}

				// 模拟从第三方API获取到的数据流
				for i := 0; i < 5; i++ {
					select {
					case <-ctx.Done():
						sender.SendError(ctx.Err())
						return
					default:
						if sender.IsClosed() {
							return
						}

						// 模拟第三方API返回的数据
						apiData := fmt.Sprintf(`{
							"api_response": %d,
							"request_id": "%s",
							"timestamp": %d,
							"status": "success"
						}`, i, string(requestMsg.Data), time.Now().Unix())

						if err := sender.Send([]byte(apiData)); err != nil {
							sender.SendError(err)
							return
						}

						// 模拟API调用间隔
						time.Sleep(150 * time.Millisecond)
					}
				}
			}()

			return nil // 立即返回，不阻塞
		}

		// 启动第三方API处理器
		go func() {
			err := subscriber.StreamSubscribeHandler(ctx, "thirdparty.api", thirdPartyAPIHandler)
			if err != nil && err != context.Canceled {
				t.Errorf("第三方API处理器错误: %v", err)
			}
		}()

		time.Sleep(100 * time.Millisecond)

		// 发起流式请求
		requestCtx, requestCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer requestCancel()

		requestData := []byte("api_request_123")

		responseProcessor := func(msg *nats.Msg) bool {
			data := string(msg.Data)

			if data == "__STREAM_END__" {
				t.Logf("第三方API流式响应结束")
				return false
			}

			if strings.HasPrefix(data, "__STREAM_ERROR__") {
				t.Errorf("第三方API错误: %s", data)
				return false
			}

			atomic.AddInt32(&receivedCount, 1)
			t.Logf("收到第三方API响应: %s", data)

			// 验证响应包含请求ID
			if !strings.Contains(data, "api_request_123") {
				t.Errorf("响应中缺少请求ID: %s", data)
			}

			return true
		}

		err = pool.StreamRequest(requestCtx, "thirdparty.api", requestData, responseProcessor)
		if err != nil {
			t.Fatalf("第三方API流式请求失败: %v", err)
		}

		received := atomic.LoadInt32(&receivedCount)
		t.Logf("总共收到第三方API响应数: %d", received)

		if received != 5 {
			t.Errorf("期望收到5个第三方API响应，实际收到%d个", received)
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

		// 总是失败的响应处理器（回调模式）
		alwaysFailHandler := func(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
			attempt := atomic.AddInt32(&attemptCount, 1)
			t.Logf("收到请求尝试 #%d", attempt)
			return sender.SendError(fmt.Errorf("模拟服务不可用_%d", attempt))
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

		// 前2次失败，第3次成功的处理器（回调模式）
		retryThenSuccessHandler := func(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
			attempt := atomic.AddInt32(&attemptCount, 1)
			t.Logf("收到请求尝试 #%d", attempt)

			if attempt <= 2 {
				return sender.SendError(fmt.Errorf("模拟临时失败_%d", attempt))
			}

			// 第3次成功，异步发送2个响应
			go func() {
				defer sender.End()

				for i := 0; i < 2; i++ {
					select {
					case <-ctx.Done():
						sender.SendError(ctx.Err())
						return
					default:
						if sender.IsClosed() {
							return
						}

						atomic.AddInt32(&successCount, 1)
						responseData := fmt.Sprintf("成功响应_%d", i)
						if err := sender.Send([]byte(responseData)); err != nil {
							sender.SendError(err)
							return
						}

						time.Sleep(10 * time.Millisecond)
					}
				}
			}()

			return nil
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

// TestAsyncStreamSubscription 测试异步流式订阅API
func TestAsyncStreamSubscription(t *testing.T) {
	s := runNATSServer(t)
	defer s.Shutdown()

	// 创建连接池和订阅者
	pool := newPool(t, s.ClientURL(), 0)
	subscriber, err := NewSubscriber([]string{s.ClientURL()})
	if err != nil {
		t.Fatalf("创建订阅者失败: %v", err)
	}
	defer subscriber.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("基本异步队列订阅", func(t *testing.T) {
		var responseCount int32
		var completedStreams int32
		var handlerCalled int32

		// 创建流式处理器
		handler := func(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
			atomic.AddInt32(&handlerCalled, 1)
			requestData := string(requestMsg.Data)
			t.Logf("处理器被调用: request=%s, reply=%s", requestData, requestMsg.Reply)

			// 发送3个响应
			for i := 0; i < 3; i++ {
				response := fmt.Sprintf("async_response_%d_%s", i, requestData)
				if err := sender.Send([]byte(response)); err != nil {
					t.Errorf("发送响应失败: %v", err)
					return err
				}
				atomic.AddInt32(&responseCount, 1)
				t.Logf("发送响应: %s", response)
			}

			atomic.AddInt32(&completedStreams, 1)
			t.Logf("流处理完成: %s", requestData)
			return sender.End()
		}

		// ✅ 异步启动订阅（不阻塞测试线程）
		handle, err := subscriber.StreamQueueSubscribeHandlerAsync(ctx, "async.test", "workers", handler)
		if err != nil {
			t.Fatalf("启动异步订阅失败: %v", err)
		}

		// 验证订阅立即返回，没有阻塞
		t.Log("异步订阅已启动，测试线程未阻塞")

		// 等待订阅生效
		time.Sleep(200 * time.Millisecond)

		// 发送一个测试请求
		requestData := "test_data_0"

		// 使用超时上下文避免无限等待
		requestCtx, requestCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer requestCancel()

		var receivedResponses int32
		err = pool.StreamRequest(requestCtx, "async.test", []byte(requestData), func(msg *nats.Msg) bool {
			received := atomic.AddInt32(&receivedResponses, 1)
			t.Logf("客户端收到响应 %d: %s", received, string(msg.Data))

			// 收到3个响应后结束
			return received < 3
		})

		if err != nil {
			t.Errorf("发送流式请求失败: %v", err)
		}

		// 验证结果
		if called := atomic.LoadInt32(&handlerCalled); called != 1 {
			t.Errorf("期望处理器被调用 1 次，实际 %d 次", called)
		}

		if streams := atomic.LoadInt32(&completedStreams); streams != 1 {
			t.Errorf("期望完成 1 个流，实际完成 %d 个", streams)
		}

		if responses := atomic.LoadInt32(&receivedResponses); responses != 3 {
			t.Errorf("期望客户端收到 3 个响应，实际收到 %d 个", responses)
		}

		// ✅ 精确控制停止
		if err := handle.Stop(); err != nil {
			t.Errorf("停止订阅失败: %v", err)
		}

		// 验证停止后的状态
		if !handle.IsStopped() {
			t.Error("订阅应该已停止")
		}

		t.Log("异步订阅测试完成")
	})

	t.Run("多个并发异步订阅", func(t *testing.T) {
		// 简化测试：只测试两个订阅
		subjects := []string{"async.orders", "async.payments"}
		var handles []*StreamSubscriptionHandle
		var totalResponses int32

		// 创建统一的处理器
		handler := func(subject string) CallbackStreamHandler {
			return func(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
				response := fmt.Sprintf("%s_processed_%s", subject, string(requestMsg.Data))
				atomic.AddInt32(&totalResponses, 1)
				t.Logf("处理器 %s 发送响应: %s", subject, response)
				sender.Send([]byte(response))
				return sender.End()
			}
		}

		// 启动多个异步订阅
		for _, subject := range subjects {
			handle, err := subscriber.StreamQueueSubscribeHandlerAsync(ctx, subject, "workers", handler(subject))
			if err != nil {
				t.Fatalf("启动订阅失败 %s: %v", subject, err)
			}
			handles = append(handles, handle)
		}

		t.Logf("成功启动 %d 个异步订阅", len(handles))

		// 等待订阅生效
		time.Sleep(200 * time.Millisecond)

		// 向每个subject发送请求
		for _, subject := range subjects {
			reqCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err := pool.StreamRequest(reqCtx, subject, []byte("test"), func(msg *nats.Msg) bool {
				t.Logf("客户端收到 %s 的响应: %s", subject, string(msg.Data))
				return false // 收到一个响应后结束
			})
			cancel()
			if err != nil {
				t.Errorf("发送请求失败 %s: %v", subject, err)
			}
		}

		// 验证所有订阅都处理了请求
		if responses := atomic.LoadInt32(&totalResponses); responses != int32(len(subjects)) {
			t.Errorf("期望 %d 个响应，实际 %d 个", len(subjects), responses)
		}

		// 批量停止
		for i, handle := range handles {
			if err := handle.Stop(); err != nil {
				t.Errorf("停止订阅 %d 失败: %v", i, err)
			}
		}

		t.Log("多订阅测试完成")
	})

	t.Run("上下文取消测试", func(t *testing.T) {
		// 创建可取消的上下文
		ctxWithCancel, cancel := context.WithCancel(context.Background())

		var handlerCalled int32

		handler := func(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
			atomic.AddInt32(&handlerCalled, 1)
			t.Log("处理器开始处理请求")

			// 简化处理：只发送一个响应然后检查上下文
			sender.Send([]byte("processing"))

			// 检查上下文是否取消
			select {
			case <-ctx.Done():
				t.Log("处理器接收到取消信号")
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
				t.Log("处理器完成")
				return sender.End()
			}
		}

		// 启动异步订阅
		handle, err := subscriber.StreamQueueSubscribeHandlerAsync(ctxWithCancel, "async.cancel", "workers", handler)
		if err != nil {
			t.Fatalf("启动订阅失败: %v", err)
		}

		// 等待订阅生效
		time.Sleep(100 * time.Millisecond)

		// 立即取消上下文
		cancel()

		// 等待清理完成
		time.Sleep(300 * time.Millisecond)

		// 验证订阅已停止
		if !handle.IsStopped() {
			t.Error("订阅应该已自动停止")
		}

		t.Log("上下文取消测试完成")
	})
}

// TestSubscriptionHandleManagement 测试订阅句柄管理
func TestSubscriptionHandleManagement(t *testing.T) {
	s := runNATSServer(t)
	defer s.Shutdown()

	subscriber, err := NewSubscriber([]string{s.ClientURL()})
	if err != nil {
		t.Fatalf("创建订阅者失败: %v", err)
	}
	defer subscriber.Close(context.Background())

	ctx := context.Background()

	t.Run("句柄状态管理", func(t *testing.T) {
		handler := func(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
			return sender.End()
		}

		// 启动订阅
		handle, err := subscriber.StreamQueueSubscribeHandlerAsync(ctx, "handle.test", "workers", handler)
		if err != nil {
			t.Fatalf("启动订阅失败: %v", err)
		}

		// 验证初始状态
		if handle.IsStopped() {
			t.Error("新创建的句柄不应该是停止状态")
		}

		// 验证上下文
		if handle.Context() == nil {
			t.Error("句柄应该有有效的上下文")
		}

		// 停止订阅
		if err := handle.Stop(); err != nil {
			t.Errorf("停止订阅失败: %v", err)
		}

		// 验证停止状态
		if !handle.IsStopped() {
			t.Error("停止后句柄应该是停止状态")
		}

		// 重复停止应该是安全的
		if err := handle.Stop(); err != nil {
			t.Errorf("重复停止应该是安全的: %v", err)
		}
	})

	t.Run("无队列版本测试", func(t *testing.T) {
		var processedCount int32

		handler := func(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
			atomic.AddInt32(&processedCount, 1)
			return sender.End()
		}

		// 启动无队列版本的异步订阅
		handle, err := subscriber.StreamSubscribeHandlerAsync(ctx, "noqueue.test", handler)
		if err != nil {
			t.Fatalf("启动无队列订阅失败: %v", err)
		}

		// 创建连接池发送请求
		pool := newPool(t, s.ClientURL(), 0)

		// 发送请求
		err = pool.StreamRequest(context.Background(), "noqueue.test", []byte("test"), func(msg *nats.Msg) bool {
			return false
		})
		if err != nil {
			t.Errorf("发送请求失败: %v", err)
		}

		// 等待处理
		time.Sleep(500 * time.Millisecond)

		// 验证处理
		if count := atomic.LoadInt32(&processedCount); count != 1 {
			t.Errorf("期望处理 1 个请求，实际处理 %d 个", count)
		}

		// 停止订阅
		handle.Stop()
	})
}
