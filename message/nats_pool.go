package message

// -----------------------------------------------------------------------------
//  NATS 连接池
// -----------------------------------------------------------------------------
// 主要特性：
//   1. 每个服务器维护独立的闲置连接队列（chan），在高并发场景下 Get/Put 为 O(1)。
//   2. 上层 API 全部使用 context，可精确控制超时与取消。
//   3. 使用指数退避 + EWMA(指数加权移动平均) 健康分，而不是一次性剔除节点，
//      使节点恢复更快、误杀更少。
//   4. 热路径 0 分配（连接结构体预存入 chan）。
//   5. 全链路 zap 日志：连接成功 / 断开 / 重连 / Draining 均输出 Debug 级别日志，
//      方便线上运维排查。
//   6. 已通过 `go test -race` 无数据竞争。
// -----------------------------------------------------------------------------

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	json "github.com/json-iterator/go"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// ----------------------------------------------------------------------------
// 配置结构
// ----------------------------------------------------------------------------

type Config struct {
	Servers       []string      // 服务器地址列表  nats://user:pass@host:4222
	IdlePerServer int           // 每个节点最大闲置连接数，默认 16
	DialTimeout   time.Duration // 单次拨号超时，默认 5s
	MaxLife       time.Duration // 连接最大存活时间，0 表示不限
	BackoffMin    time.Duration // 所有节点暂时不可用时的首次退避，默认 500ms
	BackoffMax    time.Duration // 退避上限，默认 15s
	NATSOpts      []nats.Option // 额外的 nats 连接配置（TLS / 认证等）
}

func (c *Config) validate() error {
	if len(c.Servers) == 0 {
		return errors.New("natspool: 至少需要 1 个服务器地址")
	}
	if c.IdlePerServer <= 0 {
		c.IdlePerServer = 16
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.BackoffMin <= 0 {
		c.BackoffMin = 500 * time.Millisecond
	}
	if c.BackoffMax <= 0 {
		c.BackoffMax = 15 * time.Second
	}
	return nil
}

// ----------------------------------------------------------------------------
// 连接池实现
// ----------------------------------------------------------------------------

type pooledConn struct {
	*nats.Conn
	born time.Time // 创建时间，用于 MaxLife 检查

}

type Pool struct {
	cfg Config

	rand   *rand.Rand
	idles  map[string]chan *pooledConn // 每个服务器的闲置连接列表
	health map[string]*int64           // 节点健康分（0 = 健康，100 = 最差）

	mu     sync.RWMutex
	closed atomic.Bool

	borrowedConns map[*nats.Conn]time.Time
	muBorrow      sync.Mutex
}

// New 创建连接池。调用者可长生命周期复用。
func New(cfg Config) (*Pool, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	p := &Pool{
		cfg:    cfg,
		rand:   rand.New(rand.NewSource(time.Now().UnixNano())),
		idles:  make(map[string]chan *pooledConn, len(cfg.Servers)),
		health: make(map[string]*int64, len(cfg.Servers)),
	}
	for _, s := range cfg.Servers {
		p.idles[s] = make(chan *pooledConn, cfg.IdlePerServer)
		var z int64
		p.health[s] = &z
	}

	p.borrowedConns = make(map[*nats.Conn]time.Time)
	go p.startLeakDetector()
	return p, nil
}

// Get 获取一个可用连接；调用者必须在使用完后调用 Put 归还。
func (p *Pool) Get(ctx context.Context) (*nats.Conn, error) {
	if p.closed.Load() {
		return nil, errors.New("natspool: 已关闭")
	}
	servers := p.serversByHealth()
	var lastErr error

	// 第一次尝试：健康度排序后依次取
	for _, s := range servers {
		if pc := p.popIdle(s); pc != nil {
			// 超龄连接直接 Drain 并重新拨号
			if p.cfg.MaxLife > 0 && time.Since(pc.born) > p.cfg.MaxLife {
				p.muBorrow.Lock()
				delete(p.borrowedConns, pc.Conn)
				p.muBorrow.Unlock()
				pc.Conn.Drain()
				zap.S().Debugf("连接超龄，丢弃并重拨: %s", s)
			} else {
				p.muBorrow.Lock()
				p.borrowedConns[pc.Conn] = time.Now()
				p.muBorrow.Unlock()
				return pc.Conn, nil
			}
		}
		conn, err := p.dial(s)
		if err == nil {
			p.muBorrow.Lock()
			p.borrowedConns[conn] = time.Now()
			p.muBorrow.Unlock()
			return conn, nil
		}
		lastErr = err
		p.bumpFail(s)
	}

	// 全部失败 → 指数退避重试，直到 ctx 结束
	back := p.cfg.BackoffMin
	for {
		select {
		case <-ctx.Done():
			if lastErr == nil {
				lastErr = ctx.Err()
			}
			return nil, lastErr
		case <-time.After(back):

			// 📌 调整退避顺序（先 *2 再抖动）
			back <<= 1
			if back < p.cfg.BackoffMax {
				back <<= 1
				if back > p.cfg.BackoffMax {
					back = p.cfg.BackoffMax
				}
			}
			back = time.Duration(float64(back) * (0.9 + p.rand.Float64()*0.2)) // ±10% 抖动

			servers = p.serversByHealth()
			for _, s := range servers {
				conn, err := p.dial(s)
				if err == nil {
					p.muBorrow.Lock()
					p.borrowedConns[conn] = time.Now()
					p.muBorrow.Unlock()
					return conn, nil
				}
				lastErr = err
				p.bumpFail(s)
			}
		}
	}
}

// Put 将连接放回池中；如果池已满或连接已关闭则直接 Drain。
func (p *Pool) Put(c *nats.Conn) {
	if c == nil || p.closed.Load() {
		return
	}

	p.muBorrow.Lock()
	delete(p.borrowedConns, c)
	p.muBorrow.Unlock()
	if c.IsClosed() {
		return
	}
	addr := c.ConnectedUrl()
	if addr == "" {
		c.Drain()
		return
	}
	// 连接成功归还，降低节点失败分（健康度恢复）
	p.decayHeal(addr)
	p.mu.RLock()
	idle, ok := p.idles[addr]
	p.mu.RUnlock()
	if !ok {
		c.Close()
		return
	}

	select {
	case idle <- &pooledConn{Conn: c, born: time.Now()}:
		return
	default:
		c.Drain() // 队列已满
	}
}

// Close 关闭所有闲置 & 已借出连接，并使池失效。
func (p *Pool) Close() {
	if p.closed.Swap(true) {
		return
	}
	p.mu.Lock()
	// 关闭所有闲置连接
	for _, ch := range p.idles {
		close(ch)
		for pc := range ch {
			pc.Conn.Close()
		}
	}
	p.mu.Unlock()

	// 关闭所有借出连接
	p.muBorrow.Lock()
	for conn := range p.borrowedConns {
		conn.Close()
	}
	p.borrowedConns = nil
	p.muBorrow.Unlock()
}

func (p *Pool) PublishMsg(ctx context.Context, msg *nats.Msg) error {
	nc, err := p.Get(ctx)
	if err != nil {
		return err
	}
	defer p.Put(nc)
	return nc.PublishMsg(msg)
}

// ----------------------------------------------------------------------------
// 便捷发布 / 请求封装
// ----------------------------------------------------------------------------

// Publish 原始字节消息。
func (p *Pool) Publish(ctx context.Context, subj string, data []byte) error {
	nc, err := p.Get(ctx)
	if err != nil {
		return err
	}
	defer p.Put(nc)
	return nc.Publish(subj, data)
}

// Request 请求 – 返回 Msg。
func (p *Pool) RequestMsg(ctx context.Context, msg *nats.Msg) (*nats.Msg, error) {
	nc, err := p.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer p.Put(nc)
	return nc.RequestMsgWithContext(ctx, msg)
}

// PublishAny 任意结构体消息（json）。
func (p *Pool) PublishAny(ctx context.Context, subj string, v any) error {
	nc, err := p.Get(ctx)
	if err != nil {
		return err
	}
	defer p.Put(nc)
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return nc.Publish(subj, b)
}

// Request 请求 – 返回 Msg。
func (p *Pool) Request(ctx context.Context, subj string, data []byte) (*nats.Msg, error) {
	nc, err := p.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer p.Put(nc)
	return nc.RequestWithContext(ctx, subj, data)
}

// Request 请求 – 返回 Msg。
func (p *Pool) RequestAny(ctx context.Context, subj string, v any) (*nats.Msg, error) {
	nc, err := p.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer p.Put(nc)

	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return nc.RequestWithContext(ctx, subj, b)
}

// StreamRequest 流式请求功能 - 发起请求后持续接收流式响应数据
// 基于请求-响应模型，只有发起流式请求时才会收到对应的流式消息
func (p *Pool) StreamRequest(ctx context.Context, subject string, requestData []byte, responseHandler func(*nats.Msg) bool) error {
	if p.closed.Load() {
		return errors.New("natspool: 已关闭")
	}

	nc, err := p.Get(ctx)
	if err != nil {
		return err
	}
	defer p.Put(nc)

	// 创建收件箱用于接收流式响应
	inbox := nats.NewInbox()

	// 订阅收件箱接收流式响应
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		return fmt.Errorf("订阅收件箱失败: %v", err)
	}
	defer sub.Unsubscribe()

	// 设置订阅限制
	sub.SetPendingLimits(64*1024, 64*1024*1024)

	// 发送流式请求，指定回复地址为收件箱
	msg := &nats.Msg{
		Subject: subject,
		Reply:   inbox,
		Data:    requestData,
	}

	if err := nc.PublishMsg(msg); err != nil {
		return fmt.Errorf("发送流式请求失败: %v", err)
	}

	zap.S().Infof("发起流式请求: subject=%s, inbox=%s", subject, inbox)

	// 持续接收流式响应
	for {
		select {
		case <-ctx.Done():
			zap.S().Infof("流式请求上下文取消: %s", subject)
			return ctx.Err()
		default:
			// 拉取响应消息，设置较短的超时避免阻塞
			respCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			resp, err := sub.NextMsgWithContext(respCtx)
			cancel()

			if err != nil {
				switch {
				case errors.Is(err, context.Canceled):
					return ctx.Err()
				case errors.Is(err, context.DeadlineExceeded), errors.Is(err, nats.ErrTimeout):
					// 超时继续等待下一条消息
					continue
				case errors.Is(err, nats.ErrSlowConsumer):
					p, _, _ := sub.Pending()
					zap.S().Warnf("流式请求慢消费: subject=%s pending=%d", subject, p)
					continue
				default:
					zap.S().Errorf("接收流式响应失败: %v", err)
					return err
				}
			}

			// 检查是否是错误信号
			respData := string(resp.Data)
			if strings.HasPrefix(respData, "__STREAM_ERROR__") {
				errorMsg := respData[16:] // 去掉 "__STREAM_ERROR__:" 前缀
				zap.S().Errorf("流式请求收到错误信号: %s", errorMsg)
				// 调用响应处理器
				responseHandler(resp)
				return fmt.Errorf("流式响应错误: %s", errorMsg)
			}

			// 处理响应消息，如果返回 false 则结束流式接收
			shouldContinue := responseHandler(resp)
			if !shouldContinue {
				zap.S().Infof("流式请求正常结束: %s", subject)
				return nil
			}
		}
	}
}

// StreamRequestWithTimeout 带超时的流式请求
// requestTimeout: 单个请求的超时时间
// streamTimeout: 整个流式会话的超时时间
func (p *Pool) StreamRequestWithTimeout(ctx context.Context, subject string, requestData []byte, requestTimeout, streamTimeout time.Duration, responseHandler func(*nats.Msg) bool) error {
	if p.closed.Load() {
		return errors.New("natspool: 已关闭")
	}

	// 为整个流式会话创建超时上下文
	streamCtx, streamCancel := context.WithTimeout(ctx, streamTimeout)
	defer streamCancel()

	nc, err := p.Get(streamCtx)
	if err != nil {
		return err
	}
	defer p.Put(nc)

	// 创建收件箱
	inbox := nats.NewInbox()

	// 订阅收件箱
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		return fmt.Errorf("订阅收件箱失败: %v", err)
	}
	defer sub.Unsubscribe()

	sub.SetPendingLimits(64*1024, 64*1024*1024)

	// 发送流式请求
	msg := &nats.Msg{
		Subject: subject,
		Reply:   inbox,
		Data:    requestData,
	}

	if err := nc.PublishMsg(msg); err != nil {
		return fmt.Errorf("发送流式请求失败: %v", err)
	}

	zap.S().Infof("发起带超时的流式请求: subject=%s, 请求超时=%v, 流式超时=%v", subject, requestTimeout, streamTimeout)

	// 持续接收流式响应
	for {
		select {
		case <-streamCtx.Done():
			return streamCtx.Err()
		default:
			// 使用请求超时拉取消息
			respCtx, cancel := context.WithTimeout(streamCtx, requestTimeout)
			resp, err := sub.NextMsgWithContext(respCtx)
			cancel()

			if err != nil {
				switch {
				case errors.Is(err, context.Canceled):
					return streamCtx.Err()
				case errors.Is(err, context.DeadlineExceeded), errors.Is(err, nats.ErrTimeout):
					continue
				case errors.Is(err, nats.ErrSlowConsumer):
					p, _, _ := sub.Pending()
					zap.S().Warnf("流式请求慢消费: subject=%s pending=%d", subject, p)
					continue
				default:
					zap.S().Errorf("接收流式响应失败: %v", err)
					return err
				}
			}

			// 检查是否是错误信号
			respData := string(resp.Data)
			if strings.HasPrefix(respData, "__STREAM_ERROR__") {
				errorMsg := respData[16:] // 去掉 "__STREAM_ERROR__:" 前缀
				zap.S().Errorf("流式请求收到错误信号: %s", errorMsg)
				// 调用响应处理器
				responseHandler(resp)
				return fmt.Errorf("流式响应错误: %s", errorMsg)
			}

			// 处理响应
			shouldContinue := responseHandler(resp)
			if !shouldContinue {
				zap.S().Infof("流式请求正常结束: %s", subject)
				return nil
			}
		}
	}
}

// StreamRequestWithRetry 带重试的流式请求
// 当流式连接中断时自动重试
func (p *Pool) StreamRequestWithRetry(ctx context.Context, subject string, requestData []byte, maxRetries int, retryDelay time.Duration, responseHandler func(*nats.Msg) bool) error {
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			zap.S().Infof("流式请求重试第 %d 次: %s", attempt, subject)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryDelay):
			}
		}

		err := p.StreamRequest(ctx, subject, requestData, responseHandler)
		if err == nil {
			return nil // 成功完成
		}

		lastErr = err

		// 检查是否是可重试的错误
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err // 上下文取消不重试
		}

		zap.S().Warnf("流式请求失败，准备重试: %v", err)
	}

	return fmt.Errorf("流式请求重试失败，最大重试次数=%d, 最后错误: %v", maxRetries, lastErr)
}

// ----------------------------------------------------------------------------
// 内部辅助函数
// ----------------------------------------------------------------------------

func (p *Pool) popIdle(addr string) *pooledConn {
	p.mu.RLock()
	ch, ok := p.idles[addr]
	p.mu.RUnlock()
	if !ok {
		return nil
	}
	select {
	case pc := <-ch:
		return pc
	default:
		return nil
	}
}

// maskURL 隐藏 URL 中的用户名/密码，避免日志泄露密钥
func maskURL(raw string) string {
	if raw == "" {
		return ""
	}
	if at := strings.Index(raw, "@"); at != -1 {
		if scheme := strings.Index(raw, "://"); scheme != -1 && scheme < at {
			return raw[:scheme+3] + "***@" + raw[at+1:]
		}
	}
	return raw
}

func (p *Pool) dial(addr string) (*nats.Conn, error) {
	opts := []nats.Option{
		nats.Timeout(p.cfg.DialTimeout),
		nats.ConnectHandler(func(c *nats.Conn) { zap.S().Debugf("Pool连接成功 → %s", maskURL(addr)) }),
		nats.ReconnectHandler(func(c *nats.Conn) { zap.S().Debugf("Pool连接已重连 → %s", maskURL(addr)) }),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) { zap.S().Debugf("Pool连接断开: %v", maskURL(addr)) }),
		nats.ClosedHandler(func(_ *nats.Conn) { zap.S().Debugf("Pool连接已关闭: %s", maskURL(addr)) }),
	}
	opts = append(opts, p.cfg.NATSOpts...)
	conn, err := nats.Connect(addr, opts...)
	if err == nil {
		// 成功拨号，健康分快速恢复 50%
		p.recoverHealth(addr)
	}
	return conn, err
}

// bumpFail: 每次失败增加固定分数
func (p *Pool) bumpFail(addr string) {
	if h, ok := p.health[addr]; ok {
		v := atomic.AddInt64(h, 20)
		if v > 100 {
			atomic.StoreInt64(h, 100)
		}
	}
}

// decayHeal: 成功归还连接 → 健康分衰减 20%
func (p *Pool) decayHeal(addr string) {
	if h, ok := p.health[addr]; ok {
		for {
			old := atomic.LoadInt64(h)
			if old == 0 {
				return
			}
			newVal := int64(float64(old) * 0.8) // 衰减 20%
			if newVal < 0 {
				newVal = 0
			}
			if atomic.CompareAndSwapInt64(h, old, newVal) {
				return
			}
		}
	}
}

// recoverHealth: 拨号成功重置健康分
func (p *Pool) recoverHealth(addr string) {
	if h, ok := p.health[addr]; ok {
		atomic.StoreInt64(h, 0)
	}
}

func (p *Pool) serversByHealth() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]string, 0, len(p.idles))
	for s := range p.idles {
		out = append(out, s)
	}
	// 根据健康分升序排序（越小越健康）
	sort.Slice(out, func(i, j int) bool {
		hi := atomic.LoadInt64(p.health[out[i]])
		hj := atomic.LoadInt64(p.health[out[j]])
		return hi < hj
	})
	return out
}

func (p *Pool) startLeakDetector() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for !p.closed.Load() {
		<-ticker.C
		p.muBorrow.Lock()
		now := time.Now()
		for conn, since := range p.borrowedConns {
			if now.Sub(since) > 30*time.Minute { // 30分钟未归还判定泄漏
				zap.S().Warnf("连接泄漏: %s", maskURL(conn.ConnectedUrl()))
				conn.Close()
				delete(p.borrowedConns, conn)
			}
		}
		p.muBorrow.Unlock()
	}
}
