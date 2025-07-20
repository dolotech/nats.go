package message

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// ----------------------------------------------------------------------------
// 生产级别增强说明：
// 1. 使用信号量池限制并发，防止 goroutine 无上限膨胀。
// 2. 合理设置 PendingLimits 防止内存占用失控。
// 3. Close() 等待所有异步 handler 完成，确保优雅退出。
// 4. 订阅创建时立即设置 PendingLimits。
// ----------------------------------------------------------------------------

// 合理默认：64K 条消息或 64 MiB 数据即可触发快速反压。
// 注意：这些常量已被 SubscriberConfig 替代，保留用于向后兼容
const (
	defaultPendingMsgs  = 64 * 1024
	defaultPendingBytes = 64 * 1024 * 1024
)

type subscription struct {
	subject string
	queue   string // 空表示普通订阅
	handler nats.MsgHandler
}

type Subscriber struct {
	cancel context.CancelFunc
	ctx    context.Context
	Conn   *nats.Conn
	config SubscriberConfig // 订阅者配置
	closed atomic.Bool
	wg     sync.WaitGroup // 同步订阅 msgLoop 追踪
	tasks  sync.WaitGroup // 异步 handler 追踪
	pool   chan struct{}  // 信号量池，用作并发控制

	// 新增：统计和监控 - 使用原子计数器
	messagesReceived  int64
	messagesProcessed int64
	messagesFailed    int64
	messagesDropped   int64
	goroutineCount    int64

	// 新增：活跃订阅追踪
	activeSubscriptions sync.Map
	subscriptionCount   int64
}

// SubscriberConfig 订阅者配置
type SubscriberConfig struct {
	MaxWorkers    int           // 最大并发工作者数量，默认为 GOMAXPROCS * 32
	PendingMsgs   int           // 每个订阅的消息缓冲区大小，默认 64K
	PendingBytes  int64         // 每个订阅的字节缓冲区大小，默认 64MB
	ReconnectWait time.Duration // 重连等待时间，默认 2秒
	MaxReconnects int           // 最大重连次数，-1 表示无限重连
	PingInterval  time.Duration // Ping间隔，默认 10秒
	MaxPingsOut   int           // 最大未响应Ping数，默认 3
}

// DefaultSubscriberConfig 返回默认订阅者配置
func DefaultSubscriberConfig() SubscriberConfig {
	return SubscriberConfig{
		MaxWorkers:    runtime.GOMAXPROCS(0) * 32,
		PendingMsgs:   64 * 1024,
		PendingBytes:  64 * 1024 * 1024,
		ReconnectWait: 2 * time.Second,
		MaxReconnects: -1,
		PingInterval:  10 * time.Second,
		MaxPingsOut:   3,
	}
}

// NewSubscriber 新建 Subscriber，并通过信号量池限制异步处理并发度。
func NewSubscriber(servers []string, opts ...nats.Option) (*Subscriber, error) {
	return NewSubscriberWithConfig(servers, DefaultSubscriberConfig(), opts...)
}

// NewSubscriberWithConfig 使用自定义配置创建订阅者
func NewSubscriberWithConfig(servers []string, config SubscriberConfig, opts ...nats.Option) (*Subscriber, error) {
	if len(servers) == 0 {
		return nil, errors.New("subscriber: server 列表为空")
	}

	ctx, cancel := context.WithCancel(context.Background())
	// 打乱顺序 → 负载均衡
	s := make([]string, len(servers))
	copy(s, servers)
	rand.Shuffle(len(s), func(i, j int) { s[i], s[j] = s[j], s[i] })

	sub := &Subscriber{
		ctx:    ctx,
		cancel: cancel,
		pool:   make(chan struct{}, config.MaxWorkers),
		config: config,
	}

	// 连接事件日志
	opts = append(opts,
		nats.Name("nats-subscriber"), // 便于监控
		nats.ErrorHandler(func(c *nats.Conn, s *nats.Subscription, err error) {
			if err == nats.ErrSlowConsumer && s != nil {
				pending, _, _ := s.Pending()
				droped, _ := s.Dropped()
				zap.S().Warnf(
					"[已丢弃的消息数量] subject=%s pending=%d/%d dropped=%d",
					s.Subject, pending, config.PendingMsgs, droped)
				return
			}
			subj := "<nil>"
			if s != nil {
				subj = s.Subject
			}
			zap.S().Errorf("async err on %s: %v", subj, err)
		}),
		nats.PingInterval(config.PingInterval),
		nats.MaxPingsOutstanding(config.MaxPingsOut),
		nats.MaxReconnects(config.MaxReconnects),
		nats.ReconnectWait(config.ReconnectWait),
		nats.ClosedHandler(func(_ *nats.Conn) { zap.S().Debug("Subscriber连接已关闭") }),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) { zap.S().Debugf("Subscriber断开: %v", err) }),
		// 当底层 TCP 断开并重新握手成功时，doReconnect()调用 resendSubscriptions() ,会遍历这个 conn.subs map，针对每个仍然有效的订阅调用 ，发一条新的 SUB/SUB <subject> <sid> <queue> 指令给服务器。
		// 你显式调用 Unsubscribe() / Drain()	订阅会从 conn.subs 表里被删掉，重连时自然不会再发送 SUB。
		nats.ReconnectHandler(func(c *nats.Conn) {
			zap.S().Debugf("Subscriber已重连 → %s", maskURL(c.ConnectedUrl()))
		}),
		nats.ConnectHandler(func(c *nats.Conn) { zap.S().Debugf("Subscriber已连接 → %s", maskURL(c.ConnectedUrl())) }),
	)
	var err error
	sub.Conn, err = nats.Connect(strings.Join(s, ","), opts...)
	if err != nil {
		return nil, err
	}
	return sub, nil
}

// QueueSubscribeSync 队列订阅（单线程消息拉取, 手动回调）。
// 顺序可预测：同一 goroutine 顺序调用 NextMsg，即收到顺序。
func (s *Subscriber) QueueSubscribeSync(subj, queue string, h nats.MsgHandler) error {
	if s.closed.Load() {
		return errors.New("subscriber: closed")
	}
	sub, err := s.Conn.QueueSubscribeSync(subj, queue)
	if err != nil {
		return err
	}
	sub.SetPendingLimits(s.config.PendingMsgs, int(s.config.PendingBytes))
	s.wg.Add(1)
	go s.msgLoop(sub, h)
	return nil
}

// SubscribeSync 普通同步订阅。
func (s *Subscriber) SubscribeSync(subj string, h nats.MsgHandler) error {
	if s.closed.Load() {
		return errors.New("subscriber: closed")
	}
	sub, err := s.Conn.SubscribeSync(subj)
	if err != nil {
		return err
	}
	sub.SetPendingLimits(s.config.PendingMsgs, int(s.config.PendingBytes))
	s.wg.Add(1)
	go s.msgLoop(sub, h)
	return nil
}

// QueueSubscribe 队列订阅（服务器端负载均衡）。
func (s *Subscriber) QueueSubscribe(subj, queue string, h nats.MsgHandler) error {
	if s.closed.Load() {
		return errors.New("subscriber: closed")
	}
	zap.S().Debugf("队列订阅开始: %s [%s]", subj, queue)
	sub, err := s.Conn.QueueSubscribe(subj, queue, s.wrap(subj, h))
	if err != nil {
		return err
	}
	sub.SetPendingLimits(s.config.PendingMsgs, int(s.config.PendingBytes))
	return nil
}

// Subscribe 普通订阅。
func (s *Subscriber) Subscribe(subj string, h nats.MsgHandler) error {
	if s.closed.Load() {
		return errors.New("subscriber: closed")
	}
	zap.S().Debugf("普通订阅开始: %s", subj)
	sub, err := s.Conn.Subscribe(subj, s.wrap(subj, h))
	if err != nil {
		return err
	}
	sub.SetPendingLimits(s.config.PendingMsgs, int(s.config.PendingBytes))
	return nil
}

// QueueSubscribe 队列订阅（服务器端负载均衡）。
func (s *Subscriber) QueueSubscribeGo(subj, queue string, h nats.MsgHandler) error {
	if s.closed.Load() {
		return errors.New("subscriber: closed")
	}
	zap.S().Debugf("队列订阅开始: %s [%s]", subj, queue)
	sub, err := s.Conn.QueueSubscribe(subj, queue, s.wrapgo(subj, h))
	if err != nil {
		return err
	}
	sub.SetPendingLimits(s.config.PendingMsgs, int(s.config.PendingBytes))
	return nil
}

// Subscribe 普通订阅。
func (s *Subscriber) SubscribeGo(subj string, h nats.MsgHandler) error {
	if s.closed.Load() {
		return errors.New("subscriber: closed")
	}
	zap.S().Debugf("普通订阅开始: %s", subj)
	sub, err := s.Conn.Subscribe(subj, s.wrapgo(subj, h))
	if err != nil {
		return err
	}
	sub.SetPendingLimits(s.config.PendingMsgs, int(s.config.PendingBytes))
	return nil
}

// wrap 为异步回调提供 panic 防护
func (s *Subscriber) wrapgo(subj string, h nats.MsgHandler) nats.MsgHandler {
	return func(m *nats.Msg) {
		go func(msg *nats.Msg) {
			defer func() {
				if err := recover(); err != nil {
					zap.S().Errorf("Err:%v\nSubject:%s\nStack:\n%s",
						err, subj, debug.Stack())
				}
			}()
			h(msg)
		}(m)
	}
}

// 接受推送过来的消息，如果处理不完直接丢弃
func (s *Subscriber) SubscribeAndDrop(subj string, h nats.MsgHandler) error {
	if s.closed.Load() {
		return errors.New("subscriber: closed")
	}
	zap.S().Debugf("普通订阅开始: %s", subj)
	sub, err := s.Conn.Subscribe(subj, s.drop(subj, h))
	if err != nil {
		return err
	}
	sub.SetPendingLimits(s.config.PendingMsgs, int(s.config.PendingBytes))
	return nil
}

// 为异步回调提供 panic 防护 + 并发限流。
func (s *Subscriber) drop(subj string, h nats.MsgHandler) nats.MsgHandler {
	return func(m *nats.Msg) {
		atomic.AddInt64(&s.messagesReceived, 1)

		// ① 先探测一下令牌可不可拿
		select {
		case s.pool <- struct{}{}:
			// 立刻拿到令牌，正常流程
		default:
			// 探测失败 ⇒ 令牌池满，记录一次告警
			atomic.AddInt64(&s.messagesDropped, 1)
			zap.S().Warnf("令牌池满直接丢弃, subject=%s", subj)
			return
		}

		s.tasks.Add(1)
		atomic.AddInt64(&s.goroutineCount, 1)
		go func() {
			defer func() {
				<-s.pool       // 归还令牌
				s.tasks.Done() // 结束追踪
				atomic.AddInt64(&s.goroutineCount, -1)
				if err := recover(); err != nil {
					atomic.AddInt64(&s.messagesFailed, 1)
					stack := debug.Stack()
					zap.S().Errorf("Err:%v\nSubject:%s\nStack:\n%s", err, subj, stack)
				} else {
					atomic.AddInt64(&s.messagesProcessed, 1)
				}
			}()
			h(m)
		}()
	}
}

// Close 优雅关闭，等待所有 goroutine 完成，再关闭连接。
func (s *Subscriber) Close(ctx context.Context) error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		s.tasks.Wait() // 等待所有异步处理完成
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	s.cancel()
	return s.Conn.Drain()
}

// wrap 为异步回调提供 panic 防护
func (s *Subscriber) wrap(subj string, h nats.MsgHandler) nats.MsgHandler {
	return func(m *nats.Msg) {
		defer func() {
			if err := recover(); err != nil {
				stack := debug.Stack()
				zap.S().Errorf("Err:%v\nSubject:%s\nStack:\n%s", err, subj, stack)
			}
		}()
		h(m)
	}
}

// msgLoop 用于同步订阅的顺序拉取处理。
func (s *Subscriber) msgLoop(sub *nats.Subscription, h nats.MsgHandler) {
	defer s.wg.Done()
	backoff := time.Millisecond * 200 // 网络抖动退避
	for {
		msg, err := sub.NextMsgWithContext(s.ctx)
		if err != nil {
			switch {
			//  正常结束或显式关闭
			case errors.Is(err, context.Canceled):
				return // 业务主动关停
			case errors.Is(err, nats.ErrConnectionClosed) &&
				s.ctx.Err() != nil: // Conn 已关而 ctx 已取消
				return
			//  没拿到消息：超时轮询
			case errors.Is(err, context.DeadlineExceeded),
				errors.Is(err, nats.ErrTimeout):
				continue
			//  背压：慢消费者
			case errors.Is(err, nats.ErrSlowConsumer):
				p, _, _ := sub.Pending()
				zap.S().Warnf("[SlowConsumer] subject=%s pending=%d", sub.Subject, p)
				continue
			// 	临时掉线：处于 RECONNECTING 阶段，等待自动重连后重试
			case errors.Is(err, nats.ErrNoServers), errors.Is(err, nats.ErrConnectionClosed):
				zap.S().Warnf("no servers, backing off: %v", err)
				time.Sleep(backoff)
				continue
			// 其他致命错误：写日志后退出
			default:
				zap.S().Errorf("NextMsg err: %v (sub=%s)", err, sub.Subject)
				return
			}
		}
		func() {
			defer func() {
				if err := recover(); err != nil {
					stack := debug.Stack()
					zap.S().Errorf("Err:%v\nSubject:%s\nStack:\n%s", err, sub.Subject, stack)
				}
			}()
			h(msg)
		}()
	}
}

// ResponseSender 响应发送器接口
type ResponseSender interface {
	Send(data []byte) error    // 发送数据
	SendError(err error) error // 发送错误
	End() error                // 结束流式响应
	IsClosed() bool            // 检查是否已关闭
}

// CallbackStreamHandler 回调式流式处理器
type CallbackStreamHandler func(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error

// streamResponseSender ResponseSender 的实现
type streamResponseSender struct {
	conn      *nats.Conn
	inbox     string
	ctx       context.Context
	closed    atomic.Bool
	mu        sync.RWMutex
	sendCount int64
}

// newStreamResponseSender 创建新的响应发送器
func newStreamResponseSender(conn *nats.Conn, inbox string, ctx context.Context) *streamResponseSender {
	return &streamResponseSender{
		conn:  conn,
		inbox: inbox,
		ctx:   ctx,
	}
}

// Send 发送数据
func (s *streamResponseSender) Send(data []byte) error {
	if s.IsClosed() {
		return errors.New("sender已关闭")
	}

	// 检查上下文是否已取消
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
		// 检查连接状态
		if s.conn.IsClosed() {
			return errors.New("NATS连接已关闭")
		}

		// 检查连接健康状态
		if !s.conn.IsConnected() {
			return errors.New("NATS连接不可用")
		}

		// 发送数据
		if err := s.conn.Publish(s.inbox, data); err != nil {
			zap.S().Errorf("发送流式响应失败: %v", err)
			return err
		}

		// 强制刷新确保消息发送
		if err := s.conn.Flush(); err != nil {
			zap.S().Errorf("刷新NATS连接失败: %v", err)
			return fmt.Errorf("消息发送后刷新失败: %w", err)
		}

		atomic.AddInt64(&s.sendCount, 1)
		return nil
	}
}

// SendError 发送错误信号
func (s *streamResponseSender) SendError(err error) error {
	if s.IsClosed() {
		return errors.New("sender已关闭")
	}

	// 检查连接状态
	if s.conn.IsClosed() {
		s.close()
		return errors.New("NATS连接已关闭")
	}

	if !s.conn.IsConnected() {
		s.close()
		return errors.New("NATS连接不可用")
	}

	errorMsg := []byte("__STREAM_ERROR__:" + err.Error())
	if publishErr := s.conn.Publish(s.inbox, errorMsg); publishErr != nil {
		zap.S().Errorf("发送流式错误信号失败: %v", publishErr)
		s.close()
		return publishErr
	}

	// 强制刷新确保错误信号发送
	if flushErr := s.conn.Flush(); flushErr != nil {
		zap.S().Errorf("刷新NATS连接失败: %v", flushErr)
		s.close()
		return fmt.Errorf("错误信号发送后刷新失败: %w", flushErr)
	}

	s.close()
	return nil
}

// End 结束流式响应
func (s *streamResponseSender) End() error {
	if s.IsClosed() {
		return nil // 已经关闭，忽略
	}

	// 检查连接状态
	if s.conn.IsClosed() {
		s.close()
		return errors.New("NATS连接已关闭")
	}

	if !s.conn.IsConnected() {
		s.close()
		return errors.New("NATS连接不可用")
	}

	endMsg := []byte("__STREAM_END__")
	if err := s.conn.Publish(s.inbox, endMsg); err != nil {
		zap.S().Errorf("发送流式结束信号失败: %v", err)
		s.close()
		return err
	}

	// 强制刷新确保结束信号发送
	if flushErr := s.conn.Flush(); flushErr != nil {
		zap.S().Errorf("刷新NATS连接失败: %v", flushErr)
		s.close()
		return fmt.Errorf("结束信号发送后刷新失败: %w", flushErr)
	}

	s.close()
	return nil
}

// IsClosed 检查是否已关闭
func (s *streamResponseSender) IsClosed() bool {
	return s.closed.Load()
}

// close 内部关闭方法
func (s *streamResponseSender) close() {
	s.closed.Store(true)
}

// GetSendCount 获取发送次数（用于监控）
func (s *streamResponseSender) GetSendCount() int64 {
	return atomic.LoadInt64(&s.sendCount)
}

// StreamSubscribeHandler 流式订阅处理器 - 使用回调模式
// 基于请求-响应模型，只有当收到流式请求时才会发送对应的流式消息
func (s *Subscriber) StreamSubscribeHandler(ctx context.Context, subject string, handler CallbackStreamHandler) error {
	if s.closed.Load() {
		return errors.New("subscriber: closed")
	}

	zap.S().Infof("启动流式订阅处理器（回调模式）: %s", subject)

	// 订阅流式请求
	sub, err := s.Conn.Subscribe(subject, func(m *nats.Msg) {
		// 为每个请求启动独立的流式响应协程
		go s.handleCallbackStreamRequest(ctx, m, handler)
	})

	if err != nil {
		return fmt.Errorf("订阅流式请求失败: %v", err)
	}

	sub.SetPendingLimits(s.config.PendingMsgs, int(s.config.PendingBytes))

	// 等待上下文取消
	<-ctx.Done()

	// 清理订阅
	sub.Unsubscribe()
	zap.S().Infof("流式订阅处理器已停止: %s", subject)

	return ctx.Err()
}

// StreamQueueSubscribeHandler 流式队列订阅处理器 - 使用回调模式
// 基于请求-响应模型，只有当收到流式请求时才会发送对应的流式消息
// 使用队列组实现负载均衡，多个实例可以共享工作负载
func (s *Subscriber) StreamQueueSubscribeHandler(ctx context.Context, subject, queue string, handler CallbackStreamHandler) error {
	if s.closed.Load() {
		return errors.New("subscriber: closed")
	}

	zap.S().Infof("启动流式队列订阅处理器（回调模式）: %s [queue: %s]", subject, queue)

	// 订阅流式请求
	sub, err := s.Conn.QueueSubscribe(subject, queue, func(m *nats.Msg) {
		// 为每个请求启动独立的流式响应协程
		go s.handleCallbackStreamRequest(ctx, m, handler)
	})

	if err != nil {
		return fmt.Errorf("订阅流式请求失败: %v", err)
	}

	sub.SetPendingLimits(s.config.PendingMsgs, int(s.config.PendingBytes))

	// 等待上下文取消
	<-ctx.Done()

	// 清理订阅
	sub.Unsubscribe()
	zap.S().Infof("流式队列订阅处理器已停止: %s [queue: %s]", subject, queue)

	return ctx.Err()
}

// StreamSubscriptionHandle 流式订阅控制句柄
type StreamSubscriptionHandle struct {
	subscription *nats.Subscription
	ctx          context.Context
	cancel       context.CancelFunc
	subject      string
	queue        string
	mu           sync.Mutex
	stopped      bool

	// 新增：资源追踪
	subscriber     *Subscriber
	monitorStopped chan struct{}
}

// Stop 停止流式订阅
func (h *StreamSubscriptionHandle) Stop() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.stopped {
		return nil
	}

	h.stopped = true
	h.cancel() // 取消上下文

	// 等待监控goroutine停止
	if h.monitorStopped != nil {
		select {
		case <-h.monitorStopped:
			// 监控goroutine已停止
		case <-time.After(1 * time.Second):
			zap.S().Warnf("监控goroutine停止超时: %s", h.subject)
		}
	}

	if h.subscription != nil {
		if err := h.subscription.Unsubscribe(); err != nil {
			zap.S().Errorf("取消订阅失败: %v", err)
			return err
		}
	}

	// 从活跃订阅中移除
	if h.subscriber != nil {
		h.subscriber.activeSubscriptions.Delete(h.subject)
		atomic.AddInt64(&h.subscriber.subscriptionCount, -1)
	}

	if h.queue != "" {
		zap.S().Infof("流式队列订阅处理器已停止: %s [queue: %s]", h.subject, h.queue)
	} else {
		zap.S().Infof("流式订阅处理器已停止: %s", h.subject)
	}

	return nil
}

// IsStopped 检查是否已停止
func (h *StreamSubscriptionHandle) IsStopped() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.stopped
}

// Context 获取订阅上下文
func (h *StreamSubscriptionHandle) Context() context.Context {
	return h.ctx
}

// handleCallbackStreamRequest 处理单个回调式流式请求
func (s *Subscriber) handleCallbackStreamRequest(ctx context.Context, requestMsg *nats.Msg, handler CallbackStreamHandler) {
	if requestMsg.Reply == "" {
		zap.S().Warnf("流式请求缺少回复地址: subject=%s", requestMsg.Subject)
		return
	}

	inbox := requestMsg.Reply

	// 创建响应发送器，使用原始上下文避免过早取消
	sender := newStreamResponseSender(s.Conn, inbox, ctx)

	zap.S().Infof("开始处理回调式流式请求: subject=%s, inbox=%s", requestMsg.Subject, inbox)

	defer func() {
		if err := recover(); err != nil {
			zap.S().Errorf("回调式流式请求处理崩溃: %v\n%s", err, debug.Stack())
			sender.SendError(fmt.Errorf("服务器内部错误"))
		}

		sentCount := sender.GetSendCount()
		zap.S().Infof("回调式流式请求处理结束: subject=%s, inbox=%s, 发送数=%d",
			requestMsg.Subject, inbox, sentCount)
	}()

	// 调用用户提供的处理器
	if err := handler(ctx, requestMsg, sender); err != nil {
		zap.S().Errorf("回调式流式处理器返回错误: %v", err)
		sender.SendError(err)
		return
	}

	// 注意：不要在这里自动调用 sender.End()，因为处理器可能启动了异步协程
	// 异步协程应该负责调用 sender.End() 或 sender.SendError()
	// 如果处理器是同步的且没有调用 End()，那是处理器的责任
}

// GetMetrics 获取订阅者指标
func (s *Subscriber) GetMetrics() SubscriberMetrics {
	return SubscriberMetrics{
		TotalMessagesReceived: atomic.LoadInt64(&s.messagesReceived),
		MessagesProcessed:     atomic.LoadInt64(&s.messagesProcessed),
		MessagesFailed:        atomic.LoadInt64(&s.messagesFailed),
		MessagesDropped:       atomic.LoadInt64(&s.messagesDropped),
		ActiveSubscriptions:   atomic.LoadInt64(&s.subscriptionCount),
	}
}
