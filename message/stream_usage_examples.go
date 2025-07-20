package message

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// =====================================================
// 异步流式订阅API使用示例
// =====================================================

// 示例1：基本异步流式队列订阅
func ExampleAsyncStreamQueueSubscription() {
	// 创建订阅者
	subscriber, err := NewSubscriber([]string{"nats://localhost:4222"})
	if err != nil {
		panic(err)
	}
	defer subscriber.Close(context.Background())

	// 创建应用上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 定义流式处理器
	handler := func(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
		requestData := string(requestMsg.Data)
		zap.S().Infof("处理流式请求: %s", requestData)

		// 模拟流式响应
		for i := 0; i < 10; i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				response := fmt.Sprintf("流式响应_%d: %s", i, requestData)
				if err := sender.Send([]byte(response)); err != nil {
					return err
				}
				time.Sleep(100 * time.Millisecond)
			}
		}

		return sender.End() // 结束流式响应
	}

	// ✅ 异步启动订阅（不阻塞）
	handle, err := subscriber.StreamQueueSubscribeHandlerAsync(ctx, "stream.requests", "workers", handler)
	if err != nil {
		panic(err)
	}

	// 继续执行其他业务逻辑...
	fmt.Println("订阅已启动，继续处理其他任务...")

	// 模拟运行一段时间
	time.Sleep(30 * time.Second)

	// 优雅停止
	if err := handle.Stop(); err != nil {
		zap.S().Errorf("停止订阅失败: %v", err)
	}
}

// 示例2：多个并发流式订阅管理
func ExampleMultipleAsyncSubscriptions() {
	subscriber, err := NewSubscriber([]string{"nats://localhost:4222"})
	if err != nil {
		panic(err)
	}
	defer subscriber.Close(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 订阅管理器
	var handles []*StreamSubscriptionHandle
	var mu sync.Mutex

	// 启动多个流式订阅
	subjects := []string{"stream.orders", "stream.payments", "stream.notifications"}

	for _, subject := range subjects {
		handler := createHandlerForSubject(subject)

		// ✅ 异步启动，不阻塞
		handle, err := subscriber.StreamQueueSubscribeHandlerAsync(ctx, subject, "processors", handler)
		if err != nil {
			zap.S().Errorf("启动订阅失败 %s: %v", subject, err)
			continue
		}

		mu.Lock()
		handles = append(handles, handle)
		mu.Unlock()

		zap.S().Infof("成功启动流式订阅: %s", subject)
	}

	// 主程序继续运行...
	fmt.Printf("成功启动 %d 个流式订阅\n", len(handles))

	// 等待信号或超时
	time.Sleep(60 * time.Second)

	// 批量停止所有订阅
	mu.Lock()
	for _, handle := range handles {
		if err := handle.Stop(); err != nil {
			zap.S().Errorf("停止订阅失败: %v", err)
		}
	}
	mu.Unlock()

	fmt.Println("所有订阅已停止")
}

// 示例3：健康检查和重启机制
func ExampleSubscriptionHealthCheck() {
	subscriber, err := NewSubscriber([]string{"nats://localhost:4222"})
	if err != nil {
		panic(err)
	}
	defer subscriber.Close(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 自动重启的订阅管理器
	manager := &SubscriptionManager{
		subscriber: subscriber,
		handles:    make(map[string]*StreamSubscriptionHandle),
	}

	// 启动订阅（带自动重启）
	subject := "stream.critical"
	handler := func(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
		// 关键业务处理
		return processCriticalRequest(ctx, requestMsg, sender)
	}

	if err := manager.StartManagedSubscription(ctx, subject, "critical-workers", handler); err != nil {
		panic(err)
	}

	// 启动健康检查
	go manager.HealthCheck(ctx)

	// 主程序运行...
	time.Sleep(300 * time.Second)

	// 清理
	manager.StopAll()
}

// SubscriptionManager 订阅管理器
type SubscriptionManager struct {
	subscriber *Subscriber
	handles    map[string]*StreamSubscriptionHandle
	mu         sync.RWMutex
}

// StartManagedSubscription 启动带管理的订阅
func (m *SubscriptionManager) StartManagedSubscription(ctx context.Context, subject, queue string, handler CallbackStreamHandler) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查是否已存在
	if _, exists := m.handles[subject]; exists {
		return fmt.Errorf("订阅已存在: %s", subject)
	}

	handle, err := m.subscriber.StreamQueueSubscribeHandlerAsync(ctx, subject, queue, handler)
	if err != nil {
		return err
	}

	m.handles[subject] = handle
	return nil
}

// HealthCheck 健康检查
func (m *SubscriptionManager) HealthCheck(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.checkAndRestart(ctx)
		}
	}
}

// checkAndRestart 检查并重启异常订阅
func (m *SubscriptionManager) checkAndRestart(ctx context.Context) {
	m.mu.RLock()
	var stoppedSubscriptions []string

	for subject, handle := range m.handles {
		if handle.IsStopped() {
			stoppedSubscriptions = append(stoppedSubscriptions, subject)
		}
	}
	m.mu.RUnlock()

	// 重启停止的订阅
	for _, subject := range stoppedSubscriptions {
		zap.S().Warnf("检测到订阅停止，准备重启: %s", subject)
		// 这里可以添加重启逻辑
	}
}

// StopAll 停止所有订阅
func (m *SubscriptionManager) StopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for subject, handle := range m.handles {
		if err := handle.Stop(); err != nil {
			zap.S().Errorf("停止订阅失败 %s: %v", subject, err)
		}
	}

	m.handles = make(map[string]*StreamSubscriptionHandle)
}

// 辅助函数
func createHandlerForSubject(subject string) CallbackStreamHandler {
	return func(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
		// 根据subject实现不同的处理逻辑
		switch subject {
		case "stream.orders":
			return processOrderStream(ctx, requestMsg, sender)
		case "stream.payments":
			return processPaymentStream(ctx, requestMsg, sender)
		case "stream.notifications":
			return processNotificationStream(ctx, requestMsg, sender)
		default:
			return sender.SendError(fmt.Errorf("未知的流式处理类型: %s", subject))
		}
	}
}

func processOrderStream(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
	// 订单流处理逻辑
	for i := 0; i < 5; i++ {
		response := fmt.Sprintf("订单处理步骤_%d", i+1)
		if err := sender.Send([]byte(response)); err != nil {
			return err
		}
		time.Sleep(200 * time.Millisecond)
	}
	return sender.End()
}

func processPaymentStream(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
	// 支付流处理逻辑
	steps := []string{"验证支付信息", "调用支付网关", "处理回调", "更新订单状态", "发送通知"}

	for _, step := range steps {
		if err := sender.Send([]byte(step)); err != nil {
			return err
		}
		time.Sleep(500 * time.Millisecond)
	}
	return sender.End()
}

func processNotificationStream(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
	// 通知流处理逻辑
	channels := []string{"SMS", "Email", "Push", "WebSocket"}

	for _, channel := range channels {
		notification := fmt.Sprintf("通过%s发送通知", channel)
		if err := sender.Send([]byte(notification)); err != nil {
			return err
		}
		time.Sleep(300 * time.Millisecond)
	}
	return sender.End()
}

func processCriticalRequest(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
	// 关键业务处理
	zap.S().Infof("处理关键请求: %s", string(requestMsg.Data))

	// 模拟关键处理步骤
	steps := []string{"身份验证", "权限检查", "数据处理", "结果验证", "日志记录"}

	for i, step := range steps {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			progress := fmt.Sprintf("步骤%d/%d: %s", i+1, len(steps), step)
			if err := sender.Send([]byte(progress)); err != nil {
				return err
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	return sender.End()
}

// =====================================================
// 性能对比示例
// =====================================================

// 对比：阻塞式 vs 异步式
func ExamplePerformanceComparison() {
	subscriber, _ := NewSubscriber([]string{"nats://localhost:4222"})
	defer subscriber.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	handler := func(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
		return sender.Send([]byte("response"))
	}

	// ✅ 新方式：非阻塞，直接调用
	handle, err := subscriber.StreamQueueSubscribeHandlerAsync(ctx, "new.subject", "workers", handler)
	if err != nil {
		zap.S().Errorf("异步订阅失败: %v", err)
		return
	}

	// 主线程可以继续处理其他任务
	fmt.Println("两个订阅都已启动，主线程未阻塞")

	// 精确控制停止时机
	time.Sleep(5 * time.Second)
	handle.Stop()

	fmt.Println("异步订阅已精确停止")
}
