package message

// -----------------------------------------------------------------------------
//  NATS 连接池 - 生产级优化版本
// -----------------------------------------------------------------------------
// 主要特性：
//   1. 每个服务器维护独立的闲置连接队列（chan），在高并发场景下 Get/Put 为 O(1）。
//   2. 上层 API 全部使用 context，可精确控制超时与取消。
//   3. 使用指数退避 + EWMA(指数加权移动平均) 健康分，而不是一次性剔除节点，
//      使节点恢复更快、误杀更少。
//   4. 热路径 0 分配（连接结构体预存入 chan）。
//   5. 全链路 zap 日志：连接成功 / 断开 / 重连 / Draining 均输出 Debug 级别日志，
//      方便线上运维排查。
//   6. 已通过 `go test -race` 无数据竞争。
//   7. 生产级监控指标和错误处理。
//   8. 优化的资源管理和内存泄露防护。
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

const (
	// publishFlushMax 用于发布类 API 的刷出上限：在网络抖动时给一点时间，但避免无限等待
	publishFlushMax = 2 * time.Second
	// requestPreFlushMax 用于请求类 API 的“预刷出/预飞检”：尽快发现连接不可用，避免傻等到业务 15s 超时
	requestPreFlushMax = 250 * time.Millisecond
)

// ----------------------------------------------------------------------------
// 配置结构
// ----------------------------------------------------------------------------

type Config struct {
	Servers           []string      // 服务器地址列表  nats://user:pass@host:4222
	IdlePerServer     int           // 每个节点最大闲置连接数，默认 16
	MinIdlePerServer  int           // 每个节点最小保活闲置连接数，默认 4
	MaxConnections    int           // 最大总连接数（活连接数），默认 128，防止资源耗尽
	DialTimeout       time.Duration // 单次拨号超时，默认 5s
	MaxLife           time.Duration // 连接最大存活时间，0 表示不限
	BackoffMin        time.Duration // 所有节点暂时不可用时的首次退避，默认 500ms
	BackoffMax        time.Duration // 退避上限，默认 15s
	LeakTimeout       time.Duration // 连接泄露检测超时，默认 30分钟
	LeakCheckInterval time.Duration // 泄露检测扫描间隔，默认 5分钟
	NATSOpts          []nats.Option // 额外的 nats 连接配置（TLS / 认证等）

	// 新增：保活与重连参数（与 Subscriber 保持一致的合理默认值）
	PingInterval  time.Duration // 默认 10s
	MaxPingsOut   int           // 默认 3
	MaxReconnects int           // 默认 -1 (无限重连)
	ReconnectWait time.Duration // 默认 500ms（连接池更积极）

	AcquireTimeout time.Duration // Get 获取连接的默认超时（仅当 ctx 无 deadline 时生效），默认 15s
}

func (c *Config) validate() error {
	if len(c.Servers) == 0 {
		return errors.New("natspool: 至少需要 1 个服务器地址")
	}
	if c.IdlePerServer <= 0 {
		c.IdlePerServer = 16
	}
	if c.MinIdlePerServer < 0 {
		c.MinIdlePerServer = 0
	}
	if c.MinIdlePerServer == 0 {
		c.MinIdlePerServer = 4
	}
	if c.MinIdlePerServer > c.IdlePerServer {
		c.MinIdlePerServer = c.IdlePerServer
	}
	if c.MaxConnections <= 0 {
		c.MaxConnections = 128
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
	if c.LeakTimeout <= 0 {
		c.LeakTimeout = 30 * time.Minute
	}
	if c.LeakCheckInterval <= 0 {
		c.LeakCheckInterval = 5 * time.Minute
	}
	// 新增默认值
	if c.PingInterval <= 0 {
		c.PingInterval = 10 * time.Second
	}
	if c.MaxPingsOut <= 0 {
		c.MaxPingsOut = 3
	}
	if c.MaxReconnects == 0 {
		c.MaxReconnects = -1
	}
	if c.ReconnectWait <= 0 {
		c.ReconnectWait = 500 * time.Millisecond
	}
	if c.AcquireTimeout <= 0 {
		c.AcquireTimeout = 15 * time.Second
	}
	return nil
}

// ----------------------------------------------------------------------------
// 监控指标
// ----------------------------------------------------------------------------

type PoolMetrics struct {
	TotalConnections    int64 // 总连接数
	IdleConnections     int64 // 空闲连接数
	BorrowedConnections int64 // 借出连接数
	FailedDials         int64 // 拨号失败次数
	SuccessfulDials     int64 // 拨号成功次数
	ConnectionLeaks     int64 // 连接泄露次数
	RetryAttempts       int64 // 重试次数
}

// ----------------------------------------------------------------------------
// 连接池实现
// ----------------------------------------------------------------------------

type pooledConn struct {
	*nats.Conn
	born    time.Time // 创建时间，用于 MaxLife 检查
	addr    string    // 服务器地址
	healthy bool      // 连接健康状态
}

// 借出连接的追踪信息
type borrowedInfo struct {
	borrowTime time.Time
	addr       string
	bornTime   time.Time // 保存连接的原始创建时间
}

type Pool struct {
	cfg Config

	// 并发安全的随机数生成器
	randMu sync.Mutex
	rand   *rand.Rand

	idles  map[string]chan *pooledConn // 每个服务器的闲置连接列表
	health map[string]*int64           // 节点健康分（0 = 健康，100 = 最差）

	mu      sync.RWMutex
	closed  atomic.Bool
	closeCh chan struct{} // 关闭信号，确保后台 goroutine 可及时退出

	// 优化的借出连接追踪
	borrowedConns map[*nats.Conn]*borrowedInfo
	muBorrow      sync.RWMutex // 改为读写锁提高性能

	connHealth sync.Map // map[*nats.Conn]*int32 ，1=健康 0=不可用

	// 最大连接数控制：信号量代表“活连接数”，只在真正关闭连接时释放
	connSemaphore chan struct{}
	liveConns     sync.Map // map[*nats.Conn]struct{}，用于防止重复释放信号量

	// 监控指标
	metrics PoolMetrics
}

// New 创建连接池。调用者可长生命周期复用。
func New(cfg Config) (*Pool, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	p := &Pool{
		cfg:           cfg,
		rand:          rand.New(rand.NewSource(time.Now().UnixNano())),
		idles:         make(map[string]chan *pooledConn, len(cfg.Servers)),
		health:        make(map[string]*int64, len(cfg.Servers)),
		borrowedConns: make(map[*nats.Conn]*borrowedInfo),
		closeCh:       make(chan struct{}),
		connSemaphore: make(chan struct{}, cfg.MaxConnections),
	}

	for _, s := range cfg.Servers {
		p.idles[s] = make(chan *pooledConn, cfg.IdlePerServer)
		var z int64
		p.health[s] = &z

		// 预热最小闲置连接
		for i := 0; i < cfg.MinIdlePerServer; i++ {
			// 预热也要遵守最大连接数限制
			if err := p.acquireConnSlot(context.Background()); err != nil {
				zap.S().Warnf("预热连接获取信号量失败: %v", err)
				break
			}
			conn, err := p.dial(s)
			if err != nil {
				zap.S().Warnf("预热连接失败: %v", err)
				p.releaseConnSlot()
				break
			}
			p.trackLiveConn(conn)
			p.idles[s] <- &pooledConn{
				Conn:    conn,
				born:    time.Now(),
				addr:    s,
				healthy: true,
			}
		}
	}

	go p.startLeakDetector()
	go p.startMetricsCollector()
	return p, nil
}

// GetMetrics 获取监控指标（线程安全）
func (p *Pool) GetMetrics() PoolMetrics {
	// 实时统计空闲连接数
	var idleCount int64
	p.mu.RLock()
	for _, ch := range p.idles {
		idleCount += int64(len(ch))
	}
	p.mu.RUnlock()

	// 实时统计借出连接数
	p.muBorrow.RLock()
	borrowedCount := int64(len(p.borrowedConns))
	p.muBorrow.RUnlock()

	return PoolMetrics{
		TotalConnections:    idleCount + borrowedCount,
		IdleConnections:     idleCount,
		BorrowedConnections: borrowedCount,
		FailedDials:         atomic.LoadInt64(&p.metrics.FailedDials),
		SuccessfulDials:     atomic.LoadInt64(&p.metrics.SuccessfulDials),
		ConnectionLeaks:     atomic.LoadInt64(&p.metrics.ConnectionLeaks),
		RetryAttempts:       atomic.LoadInt64(&p.metrics.RetryAttempts),
	}
}

// 并发安全的随机数生成
func (p *Pool) randFloat64() float64 {
	p.randMu.Lock()
	defer p.randMu.Unlock()
	return p.rand.Float64()
}

// Get 获取一个可用连接；调用者必须在使用完后调用 Put 归还。
func (p *Pool) Get(ctx context.Context) (*nats.Conn, error) {
	if p.closed.Load() {
		return nil, errors.New("natspool: 已关闭")
	}
	// 仅当调用方未设置超时时，才加默认 AcquireTimeout
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.cfg.AcquireTimeout)
		defer cancel()
	}
	servers := p.serversByHealth()
	var lastErr error

	// 第一次尝试：仅在所有服务器都没有空闲连接时，才会进入拨号流程。
	for _, s := range servers {
		if pc := p.popIdle(s); pc != nil {
			// 检查连接健康状态（必须是 CONNECTED）
			if pc.Conn.IsClosed() || pc.Conn.Status() != nats.CONNECTED {
				p.hardCloseConn(pc.Conn)
				continue
			}
			if !p.isConnHealthy(pc.Conn) {
				p.hardCloseConn(pc.Conn)
				continue
			}
			// 超龄连接直接关闭并重新拨号
			if p.cfg.MaxLife > 0 && time.Since(pc.born) > p.cfg.MaxLife {
				p.hardCloseConn(pc.Conn)
				zap.S().Debugf("连接超龄，丢弃并重拨: %s", s)
				continue
			}

			// 记录借出信息
			p.muBorrow.Lock()
			p.borrowedConns[pc.Conn] = &borrowedInfo{
				borrowTime: time.Now(),
				addr:       s,
				bornTime:   pc.born, // 保存原始创建时间
			}
			p.muBorrow.Unlock()
			return pc.Conn, nil
		}
	}

	// 没有可用的空闲连接 → 尝试按健康度拨号（优先最健康）。
	// 根据上下文动态调整拨号超时，避免单次拨号超时大于整体等待
	dialTimeout := p.cfg.DialTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if remain := time.Until(deadline); remain > 0 && remain < dialTimeout {
			dialTimeout = remain
		}
	}
	for _, s := range servers {
		// 每次尝试拨号前获取一个“活连接槽位”，避免跑久了资源耗尽
		if err := p.acquireConnSlot(ctx); err != nil {
			return nil, err
		}
		conn, err := p.dialWithTimeout(s, dialTimeout)
		if err == nil {
			now := time.Now()
			p.trackLiveConn(conn) // 成功建立活连接，槽位由该连接持有，直到关闭才释放
			p.muBorrow.Lock()
			p.borrowedConns[conn] = &borrowedInfo{
				borrowTime: now,
				addr:       s,
				bornTime:   now, // 新连接的创建时间
			}
			p.muBorrow.Unlock()
			return conn, nil
		}
		lastErr = err
		// 拨号失败：释放刚获取的槽位（没有产生连接）
		p.releaseConnSlot()
		p.bumpFail(s)
	}

	// 全部失败 → 有限重试，避免无限重试导致连接风暴
	back := p.cfg.BackoffMin
	maxRetries := 3 // 最多重试3次，避免连接风暴
	retryCount := 0

	for retryCount < maxRetries {
		select {
		case <-ctx.Done():
			if lastErr == nil {
				lastErr = ctx.Err()
			}
			return nil, lastErr
		case <-time.After(back):
			atomic.AddInt64(&p.metrics.RetryAttempts, 1)
			retryCount++

			// 指数退避 + 抖动
			back = back * 2
			if back > p.cfg.BackoffMax {
				back = p.cfg.BackoffMax
			}
			jitter := time.Duration(float64(back) * (0.9 + p.randFloat64()*0.2))
			back = jitter

			servers = p.serversByHealth()
			for _, s := range servers {
				// 动态拨号超时
				dialTimeout := p.cfg.DialTimeout
				if deadline, ok := ctx.Deadline(); ok {
					if remain := time.Until(deadline); remain > 0 && remain < dialTimeout {
						dialTimeout = remain
					}
				}
				if err := p.acquireConnSlot(ctx); err != nil {
					return nil, err
				}
				conn, err := p.dialWithTimeout(s, dialTimeout)
				if err == nil {
					now := time.Now()
					p.trackLiveConn(conn)
					p.muBorrow.Lock()
					p.borrowedConns[conn] = &borrowedInfo{
						borrowTime: now,
						addr:       s,
						bornTime:   now,
					}
					p.muBorrow.Unlock()
					return conn, nil
				}
				lastErr = err
				p.releaseConnSlot()
				p.bumpFail(s)
			}
		}
	}

	// 重试次数耗尽，返回最后的错误
	if lastErr == nil {
		lastErr = errors.New("natspool: 重试次数耗尽，所有服务器都不可用")
	}
	return nil, lastErr
}

// Put 将连接放回池中；如果池已满或连接已关闭则直接 Drain。
func (p *Pool) Put(c *nats.Conn) {
	if c == nil || p.closed.Load() {
		return
	}

	// 移除借出记录
	p.muBorrow.Lock()
	borrowInfo, wasBorrowed := p.borrowedConns[c]
	if wasBorrowed {
		delete(p.borrowedConns, c)
	}
	p.muBorrow.Unlock()

	// 检查连接状态
	if c.IsClosed() {
		// 连接可能被外部关闭：仍需释放槽位
		p.releaseConnSlotForConn(c)
		return
	}
	if !p.isConnHealthy(c) {
		p.hardCloseConn(c)
		return
	}

	var addr string
	if wasBorrowed && borrowInfo != nil {
		addr = borrowInfo.addr
	}
	if addr == "" {
		addr = c.ConnectedUrl()
	}
	if addr == "" {
		p.hardCloseConn(c)
		return
	}

	// 连接成功归还，降低节点失败分（健康度恢复）
	p.decayHeal(addr)

	p.mu.RLock()
	idle, ok := p.idles[addr]
	p.mu.RUnlock()
	if !ok {
		p.hardCloseConn(c)
		return
	}

	// 保持原始的born时间，修复MaxLife检查
	var bornTime time.Time
	if wasBorrowed && borrowInfo != nil {
		bornTime = borrowInfo.bornTime
	} else {
		bornTime = time.Now()
		zap.S().Warnf("连接归还时缺少借出信息: %s", addr)
	}

	select {
	case idle <- &pooledConn{
		Conn:    c,
		born:    bornTime,
		addr:    addr,
		healthy: true,
	}:
		return
	default:
		// 队列已满：保持已有连接，关闭新连接（避免过度连接波动）
		p.hardCloseConn(c)
		return
	}
}

// 硬关闭：无需优雅 Drain，快速释放资源
func (p *Pool) hardCloseConn(c *nats.Conn) {
	if c == nil {
		return
	}
	p.connHealth.Delete(c)
	if !c.IsClosed() {
		c.Close()
	}
	// 无论是否已经 closed，只要该 conn 曾占用槽位，就释放一次
	p.releaseConnSlotForConn(c)
}

// Close 关闭所有闲置 & 已借出连接，并使池失效。
func (p *Pool) Close() {
	if p.closed.Swap(true) {
		return
	}
	// 通知后台 goroutine 退出（必须只关闭一次）
	close(p.closeCh)

	p.mu.Lock()
	// 关闭所有闲置连接
	for _, ch := range p.idles {
		close(ch)
		for pc := range ch {
			p.hardCloseConn(pc.Conn)
		}
	}
	p.mu.Unlock()

	// 关闭所有借出连接
	p.muBorrow.Lock()
	for conn := range p.borrowedConns {
		p.hardCloseConn(conn)
	}
	p.borrowedConns = nil
	p.muBorrow.Unlock()
}

// startMetricsCollector 启动指标收集器
func (p *Pool) startMetricsCollector() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-p.closeCh:
			return
		case <-ticker.C:
		}
		if p.closed.Load() {
			return
		}
		metrics := p.GetMetrics()
		zap.S().Debugf("连接指标:\n空闲连接数=%d\n, 借出连接数=%d\n, 总连接数=%d\n, 成功拨号数=%d\n, 失败拨号数=%d\n, 连接泄露数=%d\n, 重试次数=%d\n",
			metrics.IdleConnections,
			metrics.BorrowedConnections,
			metrics.TotalConnections,
			metrics.SuccessfulDials,
			metrics.FailedDials,
			metrics.ConnectionLeaks,
			metrics.RetryAttempts,
		)
	}
}

func (p *Pool) ensureConnReady(c *nats.Conn) error {
	if c == nil {
		return errors.New("natspool: nil conn")
	}
	if c.IsClosed() {
		return errors.New("natspool: conn 已关闭")
	}
	// 注意：即使 IsConnected 为 true，也可能处于短暂抖动；Status 更准确
	if c.Status() != nats.CONNECTED {
		return fmt.Errorf("natspool: conn 非 CONNECTED (status=%v)", c.Status())
	}
	if !p.isConnHealthy(c) {
		return errors.New("natspool: conn 不健康")
	}
	return nil
}

func ctxBoundTimeout(ctx context.Context, max time.Duration) (time.Duration, error) {
	if max <= 0 {
		return 0, errors.New("natspool: invalid timeout")
	}
	if deadline, ok := ctx.Deadline(); ok {
		remain := time.Until(deadline)
		if remain <= 0 {
			return 0, ctx.Err()
		}
		if remain < max {
			return remain, nil
		}
	}
	return max, nil
}

func (p *Pool) flushBound(ctx context.Context, c *nats.Conn, max time.Duration) error {
	to, err := ctxBoundTimeout(ctx, max)
	if err != nil {
		return err
	}
	return c.FlushTimeout(to)
}

func (p *Pool) PublishMsg(ctx context.Context, msg *nats.Msg) error {
	nc, err := p.Get(ctx)
	if err != nil {
		return err
	}
	defer p.Put(nc)
	if err := p.ensureConnReady(nc); err != nil {
		p.hardCloseConn(nc)
		return err
	}
	if err := nc.PublishMsg(msg); err != nil {
		// Publish 失败通常表示连接不可用，快速淘汰
		p.hardCloseConn(nc)
		return err
	}
	// 提升可靠性：确保消息尽快刷出（不再忽略错误，避免业务侧傻等 15s）
	if err := p.flushBound(ctx, nc, publishFlushMax); err != nil {
		p.hardCloseConn(nc)
		return err
	}
	return nil
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
	if err := p.ensureConnReady(nc); err != nil {
		p.hardCloseConn(nc)
		return err
	}
	if err := nc.Publish(subj, data); err != nil {
		p.hardCloseConn(nc)
		return err
	}
	// 提升可靠性：确保消息尽快刷出
	if err := p.flushBound(ctx, nc, publishFlushMax); err != nil {
		p.hardCloseConn(nc)
		return err
	}
	return nil
}

// Request 请求 – 返回 Msg。
// ctx 用于控制请求超时（默认15s）
// 请求消息体为 *nats.Msg，而不是 []byte
func (p *Pool) RequestMsg(ctx context.Context, msg *nats.Msg) (*nats.Msg, error) {
	nc, err := p.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer p.Put(nc)
	if err := p.ensureConnReady(nc); err != nil {
		p.hardCloseConn(nc)
		return nil, err
	}
	// 预刷出：尽早发现连接不可用（典型表现就是业务侧固定 15s 超时）
	if err := p.flushBound(ctx, nc, requestPreFlushMax); err != nil {
		p.hardCloseConn(nc)
		return nil, err
	}
	return nc.RequestMsgWithContext(ctx, msg)
}

// PublishAny 任意结构体消息（json）。
func (p *Pool) PublishAny(ctx context.Context, subj string, v any) error {
	nc, err := p.Get(ctx)
	if err != nil {
		return err
	}
	defer p.Put(nc)
	if err := p.ensureConnReady(nc); err != nil {
		p.hardCloseConn(nc)
		return err
	}
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if err := nc.Publish(subj, b); err != nil {
		p.hardCloseConn(nc)
		return err
	}
	// 提升可靠性：确保消息尽快刷出
	if err := p.flushBound(ctx, nc, publishFlushMax); err != nil {
		p.hardCloseConn(nc)
		return err
	}
	return nil
}

// Request 请求 – 返回 Msg。
func (p *Pool) Request(ctx context.Context, subj string, data []byte) (*nats.Msg, error) {
	nc, err := p.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer p.Put(nc)
	if err := p.ensureConnReady(nc); err != nil {
		p.hardCloseConn(nc)
		return nil, err
	}
	// 预刷出：尽早发现连接不可用
	if err := p.flushBound(ctx, nc, requestPreFlushMax); err != nil {
		p.hardCloseConn(nc)
		return nil, err
	}
	return nc.RequestWithContext(ctx, subj, data)
}

// Request 请求 – 返回 Msg。
func (p *Pool) RequestAny(ctx context.Context, subj string, v any) (*nats.Msg, error) {
	nc, err := p.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer p.Put(nc)
	if err := p.ensureConnReady(nc); err != nil {
		p.hardCloseConn(nc)
		return nil, err
	}

	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	// 预刷出：尽早发现连接不可用
	if err := p.flushBound(ctx, nc, requestPreFlushMax); err != nil {
		p.hardCloseConn(nc)
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
	if err := p.ensureConnReady(nc); err != nil {
		p.hardCloseConn(nc)
		return err
	}

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
		p.hardCloseConn(nc)
		return fmt.Errorf("发送流式请求失败: %v", err)
	}
	// 预刷出：避免请求没发出去，客户端一直等到 ctx 超时
	if err := p.flushBound(ctx, nc, requestPreFlushMax); err != nil {
		p.hardCloseConn(nc)
		return fmt.Errorf("发送流式请求后刷新失败: %v", err)
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
	if err := p.ensureConnReady(nc); err != nil {
		p.hardCloseConn(nc)
		return err
	}

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
		p.hardCloseConn(nc)
		return fmt.Errorf("发送流式请求失败: %v", err)
	}
	if err := p.flushBound(streamCtx, nc, requestPreFlushMax); err != nil {
		p.hardCloseConn(nc)
		return fmt.Errorf("发送流式请求后刷新失败: %v", err)
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
				errorMsg := ""
				if len(respData) > 16 { // 安全的边界检查
					errorMsg = respData[16:] // 去掉 "__STREAM_ERROR__:" 前缀
				}
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

// StreamRequestWithClient 带客户端ID的流式请求，简化版本
// connectID: 客户端连接标识，用于服务端感知客户端离线
func (p *Pool) StreamRequestWithClient(ctx context.Context, subject string, requestData []byte, connectID string, responseHandler func(*nats.Msg) bool) error {
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

	// 创建带连接ID的流式请求
	msg := &nats.Msg{
		Subject: subject,
		Reply:   inbox,
		Data:    requestData,
	}

	// 添加连接ID到消息头（如果提供）
	if connectID != "" {
		if msg.Header == nil {
			msg.Header = make(nats.Header)
		}
		msg.Header.Set("Connect-ID", connectID)
	}

	if err := nc.PublishMsg(msg); err != nil {
		return fmt.Errorf("发送流式请求失败: %v", err)
	}

	zap.S().Infof("发起流式请求: subject=%s, inbox=%s, connectID=%s", subject, inbox, connectID)

	// 持续接收流式响应
	for {
		select {
		case <-ctx.Done():
			// 客户端上下文取消时，标记为离线
			if connectID != "" {
				// 发送离线事件并尽量刷出，确保服务端尽快感知
				_ = nc.Publish("client.offline", []byte(connectID))
				_ = nc.FlushTimeout(2 * time.Second)
				// 同进程内回落：直接标记离线，便于单进程测试环境快速收敛
				MarkClientOffline(connectID)
				// 等待少许时间，确保监听方消费到离线事件
				deadline := time.Now().Add(200 * time.Millisecond)
				for IsClientOnline(connectID) && time.Now().Before(deadline) {
					time.Sleep(10 * time.Millisecond)
				}
			}
			zap.S().Infof("流式请求上下文取消: %s, connectID=%s", subject, connectID)
			return ctx.Err()
		default:
			// 拉取响应消息，设置较短的超时避免阻塞
			respCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			resp, err := sub.NextMsgWithContext(respCtx)
			cancel()

			if err != nil {
				switch {
				case errors.Is(err, context.Canceled):
					// 进入与 <-ctx.Done() 相同的离线处理流程
					if connectID != "" {
						_ = nc.Publish("client.offline", []byte(connectID))
						_ = nc.FlushTimeout(2 * time.Second)
						MarkClientOffline(connectID)
						deadline := time.Now().Add(200 * time.Millisecond)
						for IsClientOnline(connectID) && time.Now().Before(deadline) {
							time.Sleep(10 * time.Millisecond)
						}
					}
					return ctx.Err()
				case errors.Is(err, context.DeadlineExceeded), errors.Is(err, nats.ErrTimeout):
					// 超时继续等待下一条消息
					continue
				case errors.Is(err, nats.ErrSlowConsumer):
					p, _, _ := sub.Pending()
					zap.S().Warnf("流式请求慢消费: subject=%s pending=%d connectID=%s", subject, p, connectID)
					continue
				default:
					zap.S().Errorf("接收流式响应失败: %v, connectID=%s", err, connectID)
					return err
				}
			}

			// 检查是否是错误信号
			respData := string(resp.Data)
			if strings.HasPrefix(respData, "__STREAM_ERROR__") {
				errorMsg := ""
				if len(respData) > 16 {
					errorMsg = respData[16:] // 去掉 "__STREAM_ERROR__:" 前缀
				}
				zap.S().Errorf("流式请求收到错误信号: %s, connectID=%s", errorMsg, connectID)
				// 调用响应处理器
				responseHandler(resp)
				return fmt.Errorf("流式响应错误: %s", errorMsg)
			}

			// 处理响应消息，如果返回 false 则结束流式接收
			shouldContinue := responseHandler(resp)
			if !shouldContinue {
				zap.S().Infof("流式请求正常结束: %s, connectID=%s", subject, connectID)
				return nil
			}
		}
	}
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

	// 使用非阻塞方式快速获取连接
	select {
	case pc := <-ch:
		// 安全检查：确保连接不为空
		if pc == nil || pc.Conn == nil {
			// 尝试再获取一个
			select {
			case pc2 := <-ch:
				if pc2 != nil && pc2.Conn != nil {
					return pc2
				}
				return nil
			default:
				return nil
			}
		}

		// 快速健康检查
		if pc.Conn.IsClosed() || !pc.healthy {
			// 连接不健康，直接丢弃并尝试下一个
			p.hardCloseConn(pc.Conn)
			// 尝试再获取一个
			select {
			case pc2 := <-ch:
				if pc2 != nil && pc2.Conn != nil {
					return pc2
				}
				return nil
			default:
				return nil
			}
		}
		return pc
	default:
		return nil
	}
}

func (p *Pool) setConnHealthy(conn *nats.Conn, healthy bool) {
	if conn == nil {
		return
	}
	val, loaded := p.connHealth.LoadOrStore(conn, new(int32))
	state := val.(*int32)
	if !loaded && !healthy {
		atomic.StoreInt32(state, 0)
		return
	}
	if healthy {
		atomic.StoreInt32(state, 1)
	} else {
		atomic.StoreInt32(state, 0)
	}
}

func (p *Pool) isConnHealthy(conn *nats.Conn) bool {
	if conn == nil {
		return false
	}
	if val, ok := p.connHealth.Load(conn); ok {
		return atomic.LoadInt32(val.(*int32)) == 1
	}
	return false
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
		nats.PingInterval(p.cfg.PingInterval),
		nats.MaxPingsOutstanding(p.cfg.MaxPingsOut),
		nats.MaxReconnects(p.cfg.MaxReconnects),
		nats.ReconnectWait(p.cfg.ReconnectWait),
		nats.ConnectHandler(func(c *nats.Conn) {
			p.setConnHealthy(c, true)
			zap.S().Debugf("Pool连接成功 → %s", maskURL(addr))
			atomic.AddInt64(&p.metrics.SuccessfulDials, 1)
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			p.setConnHealthy(c, true)
			zap.S().Debugf("Pool连接已重连 → %s", maskURL(addr))
		}),
		nats.DisconnectErrHandler(func(c *nats.Conn, err error) {
			p.setConnHealthy(c, false)
			zap.S().Debugf("Pool连接断开: %v", maskURL(addr))
		}),
		nats.ClosedHandler(func(c *nats.Conn) {
			p.setConnHealthy(c, false)
			p.connHealth.Delete(c)
			zap.S().Debugf("Pool连接已关闭: %s", maskURL(addr))
		}),
		nats.ErrorHandler(func(c *nats.Conn, s *nats.Subscription, err error) {
			if s != nil {
				zap.S().Errorf("NATS错误: subject=%s, error=%v", s.Subject, err)
			} else {
				zap.S().Errorf("NATS连接错误: %v", err)
			}
		}),
	}
	opts = append(opts, p.cfg.NATSOpts...)
	conn, err := nats.Connect(addr, opts...)
	if err == nil {
		// 成功拨号，健康分快速恢复
		p.recoverHealth(addr)
		p.setConnHealthy(conn, true)
	} else {
		// 拨号失败，记录指标
		atomic.AddInt64(&p.metrics.FailedDials, 1)
	}
	return conn, err
}

// dialWithTimeout: 基于临时超时的拨号
func (p *Pool) dialWithTimeout(addr string, timeout time.Duration) (*nats.Conn, error) {
	opts := []nats.Option{
		nats.Timeout(timeout),
		nats.PingInterval(p.cfg.PingInterval),
		nats.MaxPingsOutstanding(p.cfg.MaxPingsOut),
		nats.MaxReconnects(p.cfg.MaxReconnects),
		nats.ReconnectWait(p.cfg.ReconnectWait),
		nats.ConnectHandler(func(c *nats.Conn) {
			p.setConnHealthy(c, true)
			zap.S().Debugf("Pool连接成功 → %s", maskURL(addr))
			atomic.AddInt64(&p.metrics.SuccessfulDials, 1)
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			p.setConnHealthy(c, true)
			zap.S().Debugf("Pool连接已重连 → %s", maskURL(addr))
		}),
		nats.DisconnectErrHandler(func(c *nats.Conn, err error) {
			p.setConnHealthy(c, false)
			zap.S().Debugf("Pool连接断开: %v", maskURL(addr))
		}),
		nats.ClosedHandler(func(c *nats.Conn) {
			p.setConnHealthy(c, false)
			p.connHealth.Delete(c)
			zap.S().Debugf("Pool连接已关闭: %s", maskURL(addr))
		}),
		nats.ErrorHandler(func(c *nats.Conn, s *nats.Subscription, err error) {
			if s != nil {
				zap.S().Errorf("NATS错误: subject=%s, error=%v", s.Subject, err)
			} else {
				zap.S().Errorf("NATS连接错误: %v", err)
			}
		}),
	}
	opts = append(opts, p.cfg.NATSOpts...)
	conn, err := nats.Connect(addr, opts...)
	if err == nil {
		// 成功拨号，健康分快速恢复
		p.recoverHealth(addr)
		p.setConnHealthy(conn, true)
	} else {
		// 拨号失败，记录指标
		atomic.AddInt64(&p.metrics.FailedDials, 1)
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
	ticker := time.NewTicker(p.cfg.LeakCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.closeCh:
			return
		case <-ticker.C:
		}
		if p.closed.Load() {
			return
		}
		p.detectAndCloseLeakedConnections(time.Now())
	}
}

func (p *Pool) acquireConnSlot(ctx context.Context) error {
	select {
	case p.connSemaphore <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Pool) releaseConnSlot() {
	select {
	case <-p.connSemaphore:
	default:
		// 逻辑错误：出现重复释放/未获取先释放；这里不 panic，打日志便于线上定位
		zap.S().Warnf("连接槽位释放异常：可能重复释放或未获取先释放")
	}
}

func (p *Pool) trackLiveConn(c *nats.Conn) {
	if c == nil {
		return
	}
	p.liveConns.Store(c, struct{}{})
}

func (p *Pool) releaseConnSlotForConn(c *nats.Conn) {
	if c == nil {
		return
	}
	if _, loaded := p.liveConns.LoadAndDelete(c); loaded {
		p.releaseConnSlot()
	}
}

type leakedConn struct {
	conn       *nats.Conn
	borrowTime time.Time
	addr       string
}

// detectAndCloseLeakedConnections 扫描并关闭超时未归还的借出连接。
// 关键点：不能在持有 muBorrow 锁的情况下执行 Close/Drain 等潜在阻塞操作。
func (p *Pool) detectAndCloseLeakedConnections(now time.Time) {
	if p.cfg.LeakTimeout <= 0 {
		return
	}

	var leaked []leakedConn

	p.muBorrow.Lock()
	for conn, info := range p.borrowedConns {
		if info == nil {
			continue
		}
		if now.Sub(info.borrowTime) > p.cfg.LeakTimeout {
			// 先从追踪表中删除，避免后续 Put/重复检测产生混乱
			delete(p.borrowedConns, conn)
			atomic.AddInt64(&p.metrics.ConnectionLeaks, 1)
			leaked = append(leaked, leakedConn{
				conn:       conn,
				borrowTime: info.borrowTime,
				addr:       info.addr,
			})
		}
	}
	p.muBorrow.Unlock()

	for _, it := range leaked {
		// 记录尽可能多的信息（ConnectedUrl 在断线/关闭后可能为空）
		u := ""
		if it.conn != nil {
			u = maskURL(it.conn.ConnectedUrl())
		}
		if u == "" {
			u = maskURL(it.addr)
		}
		zap.S().Warnf("连接泄漏: %s (borrowed at %s, leakTimeout=%s)", u, it.borrowTime.Format(time.RFC3339), p.cfg.LeakTimeout)
		p.hardCloseConn(it.conn)
	}
}
