package message

import (
	"context"
	"errors"
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

// 合理默认：64K 条消息或 64 MiB 数据即可触发快速反压。
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
	closed atomic.Bool
	wg     sync.WaitGroup // 同步订阅 msgLoop 追踪
	tasks  sync.WaitGroup // 异步 handler 追踪
	pool   chan struct{}  // 信号量池，用作并发控制
}

// NewSubscriber 新建 Subscriber，并通过信号量池限制异步处理并发度。
func NewSubscriber(servers []string, opts ...nats.Option) (*Subscriber, error) {
	if len(servers) == 0 {
		return nil, errors.New("subscriber: server 列表为空")
	}
	ctx, cancel := context.WithCancel(context.Background())
	// 打乱顺序 → 负载均衡
	s := make([]string, len(servers))
	copy(s, servers)
	rand.Shuffle(len(s), func(i, j int) { s[i], s[j] = s[j], s[i] })

	maxWorkers := runtime.GOMAXPROCS(0) * 32 // 默认并发度，可根据业务规模调整
	sub := &Subscriber{
		ctx:    ctx,
		cancel: cancel,
		pool:   make(chan struct{}, maxWorkers),
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
					s.Subject, pending, defaultPendingMsgs, droped)
				return
			}
			subj := "<nil>"
			if s != nil {
				subj = s.Subject
			}
			zap.S().Errorf("async err on %s: %v", subj, err)
		}),
		nats.PingInterval(10*time.Second),
		nats.MaxPingsOutstanding(3),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
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
	sub.SetPendingLimits(defaultPendingMsgs, defaultPendingBytes)
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
	sub.SetPendingLimits(defaultPendingMsgs, defaultPendingBytes)
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
	sub.SetPendingLimits(defaultPendingMsgs, defaultPendingBytes)
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
	sub.SetPendingLimits(defaultPendingMsgs, defaultPendingBytes)
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
	sub.SetPendingLimits(defaultPendingMsgs, defaultPendingBytes)
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
	sub.SetPendingLimits(defaultPendingMsgs, defaultPendingBytes)
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
	sub.SetPendingLimits(defaultPendingMsgs, defaultPendingBytes)
	return nil
}

// 为异步回调提供 panic 防护 + 并发限流。
func (s *Subscriber) drop(subj string, h nats.MsgHandler) nats.MsgHandler {
	return func(m *nats.Msg) {
		// ① 先探测一下令牌可不可拿
		select {
		case s.pool <- struct{}{}:
			// 立刻拿到令牌，正常流程
		default:
			// 探测失败 ⇒ 令牌池满，记录一次告警
			zap.S().Warnf("令牌池满直接丢弃, subject=%s", subj)
			return
		}

		s.tasks.Add(1)
		go func() {
			defer func() {
				<-s.pool       // 归还令牌
				s.tasks.Done() // 结束追踪
				if err := recover(); err != nil {
					stack := debug.Stack()
					zap.S().Errorf("Err:%v\nSubject:%s\nStack:\n%s", err, subj, stack)
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
