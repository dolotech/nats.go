package message

import (
	"context"
	"fmt"
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
