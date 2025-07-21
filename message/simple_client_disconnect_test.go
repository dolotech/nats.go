package message

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// 测试简化版本的客户端离线检测
func TestSimpleClientDisconnectDetection(t *testing.T) {
	s := runNATSServer(t)
	defer s.Shutdown()

	// 创建订阅者和连接池
	subscriber, err := NewSubscriber([]string{s.ClientURL()})
	if err != nil {
		t.Fatalf("创建订阅者失败: %v", err)
	}
	defer subscriber.Close(context.Background())

	// 启动客户端离线事件监听器
	err = subscriber.StartClientOfflineListener()
	if err != nil {
		t.Fatalf("启动离线监听器失败: %v", err)
	}

	pool := newPool(t, s.ClientURL(), 0)
	defer pool.Close()

	t.Run("基本离线检测功能", func(t *testing.T) {
		var processedCount int32
		var offlineDetected int32

		// 创建流式处理器
		handler := func(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
			atomic.AddInt32(&processedCount, 1)
			connectID := requestMsg.Header.Get("Connect-ID")
			t.Logf("开始处理请求: connectID=%s", connectID)

			// 异步处理，模拟长时间响应
			go func() {
				defer sender.End()

				for i := 0; i < 20; i++ {
					response := fmt.Sprintf("响应_%d", i)
					if err := sender.Send([]byte(response)); err != nil {
						// 检测到客户端离线
						atomic.AddInt32(&offlineDetected, 1)
						t.Logf("✅ 检测到客户端离线: %v, 循环第%d次", err, i)
						sender.SendError(err)
						return
					}

					// 模拟处理延迟
					time.Sleep(100 * time.Millisecond)
				}
			}()

			return nil
		}

		// 启动流式处理器
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := subscriber.StreamQueueSubscribeHandler(ctx, "simple.test", "workers", handler)
			if err != nil && err != context.Canceled {
				t.Errorf("流式处理器错误: %v", err)
			}
		}()

		// 等待订阅生效
		time.Sleep(200 * time.Millisecond)

		// 发起带客户端ID的流式请求
		connectID := "test_client_123"
		var receivedCount int32

		requestCtx, requestCancel := context.WithCancel(context.Background())

		go func() {
			err := pool.StreamRequestWithClient(requestCtx, "simple.test", []byte("test"), connectID, func(msg *nats.Msg) bool {
				response := string(msg.Data)

				if response == "__STREAM_END__" {
					return false
				}

				if len(response) > 16 && response[:16] == "__STREAM_ERROR__" {
					t.Logf("收到错误信号: %s", response)
					return false
				}

				atomic.AddInt32(&receivedCount, 1)
				t.Logf("客户端收到响应: %s", response)
				return true
			})

			if err != nil {
				t.Logf("流式请求结束: %v", err)
			}
		}()

		// 等待开始处理
		time.Sleep(500 * time.Millisecond)

		// 验证客户端在线
		if !IsClientOnline(connectID) {
			t.Error("客户端应该在线")
		}

		// 模拟客户端离线 - 取消请求上下文
		t.Log("⚠️ 模拟客户端离线...")
		requestCancel()

		// 等待离线事件传播
		time.Sleep(500 * time.Millisecond)

		// 验证客户端已离线
		if IsClientOnline(connectID) {
			t.Error("客户端应该已离线")
		}

		// 等待处理完成
		time.Sleep(500 * time.Millisecond)

		// 验证结果
		processed := atomic.LoadInt32(&processedCount)
		offline := atomic.LoadInt32(&offlineDetected)
		received := atomic.LoadInt32(&receivedCount)

		t.Logf("测试结果:")
		t.Logf("  处理的请求数: %d", processed)
		t.Logf("  检测到离线数: %d", offline)
		t.Logf("  客户端收到响应数: %d", received)

		if processed != 1 {
			t.Errorf("期望处理 1 个请求，实际处理 %d 个", processed)
		}

		if offline != 1 {
			t.Errorf("期望检测到 1 次离线，实际检测到 %d 次", offline)
		}

		if received == 0 {
			t.Error("客户端应该收到至少一些响应")
		}
	})

	t.Run("手动标记客户端离线", func(t *testing.T) {
		connectID := "manual_test_456"

		// 手动标记客户端在线
		MarkClientOnline(connectID)
		if !IsClientOnline(connectID) {
			t.Error("客户端应该在线")
		}

		// 手动标记客户端离线
		MarkClientOffline(connectID)
		if IsClientOnline(connectID) {
			t.Error("客户端应该已离线")
		}

		t.Log("✅ 手动标记测试成功")
	})
}
