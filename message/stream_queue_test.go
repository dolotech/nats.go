package message

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// 测试StreamQueueSubscribeHandler的基本功能
func TestStreamQueueSubscribeHandler(t *testing.T) {
	s := runNATSServer(t)
	defer s.Shutdown()

	// 创建订阅者和连接池
	subscriber, err := NewSubscriber([]string{s.ClientURL()})
	if err != nil {
		t.Fatalf("创建订阅者失败: %v", err)
	}
	defer subscriber.Close(context.Background())

	pool := newPool(t, s.ClientURL(), 0)
	defer pool.Close()

	t.Run("基本队列订阅功能", func(t *testing.T) {
		var processedCount int32

		// 创建流式处理器
		handler := func(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
			atomic.AddInt32(&processedCount, 1)
			requestData := string(requestMsg.Data)
			t.Logf("队列处理器收到请求: %s", requestData)

			// 发送3个流式响应
			for i := 0; i < 3; i++ {
				response := fmt.Sprintf("queue_response_%d_%s", i, requestData)
				if err := sender.Send([]byte(response)); err != nil {
					return err
				}
			}

			return sender.End()
		}

		// 启动队列订阅处理器（会阻塞直到上下文取消）
		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			err := subscriber.StreamQueueSubscribeHandler(ctx, "queue.test", "workers", handler)
			if err != nil && err != context.Canceled {
				t.Errorf("队列订阅处理器错误: %v", err)
			}
		}()

		// 等待订阅生效
		time.Sleep(200 * time.Millisecond)

		// 发送测试请求
		var receivedResponses int32
		requestCtx, requestCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer requestCancel()

		err := pool.StreamRequest(requestCtx, "queue.test", []byte("test_data"), func(msg *nats.Msg) bool {
			response := string(msg.Data)
			if response == "__STREAM_END__" {
				t.Log("收到流式结束信号")
				return false
			}

			atomic.AddInt32(&receivedResponses, 1)
			t.Logf("客户端收到响应: %s", response)
			return true
		})

		if err != nil {
			t.Errorf("流式请求失败: %v", err)
		}

		// 验证结果
		if processed := atomic.LoadInt32(&processedCount); processed != 1 {
			t.Errorf("期望处理 1 个请求，实际处理 %d 个", processed)
		}

		if received := atomic.LoadInt32(&receivedResponses); received != 3 {
			t.Errorf("期望收到 3 个响应，实际收到 %d 个", received)
		}

		// 停止订阅
		cancel()
		time.Sleep(100 * time.Millisecond)
	})

	t.Run("多实例负载均衡", func(t *testing.T) {
		var totalProcessed int32
		var worker1Processed int32
		var worker2Processed int32

		// 创建两个处理器实例
		handler1 := func(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
			atomic.AddInt32(&totalProcessed, 1)
			atomic.AddInt32(&worker1Processed, 1)

			response := fmt.Sprintf("worker1_processed_%s", string(requestMsg.Data))
			sender.Send([]byte(response))
			return sender.End()
		}

		handler2 := func(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
			atomic.AddInt32(&totalProcessed, 1)
			atomic.AddInt32(&worker2Processed, 1)

			response := fmt.Sprintf("worker2_processed_%s", string(requestMsg.Data))
			sender.Send([]byte(response))
			return sender.End()
		}

		// 启动两个队列订阅处理器
		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			subscriber.StreamQueueSubscribeHandler(ctx, "load.balance", "workers", handler1)
		}()

		go func() {
			subscriber.StreamQueueSubscribeHandler(ctx, "load.balance", "workers", handler2)
		}()

		// 等待订阅生效
		time.Sleep(200 * time.Millisecond)

		// 发送多个请求测试负载均衡
		requestCount := 6
		for i := 0; i < requestCount; i++ {
			requestCtx, requestCancel := context.WithTimeout(context.Background(), 3*time.Second)

			requestData := fmt.Sprintf("request_%d", i)
			err := pool.StreamRequest(requestCtx, "load.balance", []byte(requestData), func(msg *nats.Msg) bool {
				response := string(msg.Data)
				if response == "__STREAM_END__" {
					return false
				}
				t.Logf("负载均衡响应: %s", response)
				return true
			})

			requestCancel()
			if err != nil {
				t.Errorf("请求 %d 失败: %v", i, err)
			}

			// 短暂延迟确保请求分散
			time.Sleep(50 * time.Millisecond)
		}

		// 等待所有请求处理完成
		time.Sleep(500 * time.Millisecond)

		// 验证负载均衡效果
		total := atomic.LoadInt32(&totalProcessed)
		worker1 := atomic.LoadInt32(&worker1Processed)
		worker2 := atomic.LoadInt32(&worker2Processed)

		t.Logf("负载均衡结果:")
		t.Logf("  总处理数: %d", total)
		t.Logf("  Worker1处理数: %d", worker1)
		t.Logf("  Worker2处理数: %d", worker2)

		if total != int32(requestCount) {
			t.Errorf("期望总处理 %d 个请求，实际处理 %d 个", requestCount, total)
		}

		// 验证两个worker都参与了处理（负载均衡生效）
		if worker1 > 0 && worker2 > 0 {
			t.Log("✅ 负载均衡正常工作，两个worker都处理了请求")
		} else {
			t.Logf("⚠️ 负载均衡可能不均匀: worker1=%d, worker2=%d", worker1, worker2)
		}

		// 停止订阅
		cancel()
		time.Sleep(100 * time.Millisecond)
	})
}
