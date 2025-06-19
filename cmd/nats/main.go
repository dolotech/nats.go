package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

// getenv returns the value of the env var key or def if not set.
func getenv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func mustSplitHostPort(addr string) (string, int) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		log.Fatalf("invalid address %q: %v", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Fatalf("invalid port in %q: %v", addr, err)
	}
	return host, port
}

var opts = &server.Options{
	//------------------------------------------------------------------
	// 基本监听
	//------------------------------------------------------------------
	Host:     "0.0.0.0",
	Port:     4222,
	HTTPHost: "0.0.0.0", // 监控端点
	HTTPPort: 8222,

	//------------------------------------------------------------------
	// 关闭持久化与高层特性
	//------------------------------------------------------------------
	JetStream:       false, // 纯内存 Core NATS
	NoSystemAccount: true,  // 不产生 $SYS* 统计流量

	//------------------------------------------------------------------
	// 大吞吐专用的 “Limits”
	//------------------------------------------------------------------
	MaxConn:       1_000_000,         // 理论可支撑百万客户端
	MaxPayload:    2 * 1024 * 1024,   // 单条 8 MiB，官方建议的安全上限:contentReference[oaicite:0]{index=0}
	MaxPending:    128 * 1024 * 1024, // 每连接缓冲 128 MiB，免得突发流量丢包
	WriteDeadline: 2 * time.Second,   // 2 s 内写不完判定慢消费者:contentReference[oaicite:1]{index=1}

	//------------------------------------------------------------------
	// 快速探活、早期检测死连接
	//------------------------------------------------------------------
	PingInterval:          60 * time.Second, // 服务端每 60s 主动 PING
	MaxPingsOut:           3,                // 3 次未回应即踢掉
	DisableShortFirstPing: true,             // 避免一连上就 PING 造成抖动
	LameDuckDuration:      30 * time.Second, // 优雅下线时间窗
	LameDuckGracePeriod:   5 * time.Second,

	//------------------------------------------------------------------
	// 可选微调（按业务场景再决定）-------------------------------
	NoSublistCache:  false, // 若主题非常高基数才设 true:contentReference[oaicite:2]{index=2}
	NoHeaderSupport: false, // 若从不发 header，可设 true 省一次 alloc
	// Authorization: "YOUR_TOKEN", // 如需轻量安全，可放一个 Bearer Token
	//------------------------------------------------------------------
	Logtime: true, Debug: false, Trace: false,
}

func main() {
	// ----------- Runtime flags & env ------------------
	listen := flag.String("listen", getenv("NATS_LISTEN", "0.0.0.0:4222"), "client listen <host:port>")
	httpAddr := flag.String("http", getenv("NATS_HTTP", "0.0.0.0:8222"), "HTTP monitoring <host:port>")
	cluster := flag.String("cluster", getenv("NATS_CLUSTER", ""), "cluster listen <host:port>")
	logLevel := flag.String("log", getenv("NATS_LOG", "info"), "log level: debug|info|error")
	maxConns := flag.Int("max_conns", func() int {
		if v := getenv("NATS_MAX_CONNS", ""); v != "" {
			if i, err := strconv.Atoi(v); err == nil {
				return i
			}
		}
		return 65536
	}(), "maximum client connections")
	writeDeadlineSecs := flag.Int("write_deadline", func() int {
		if v := getenv("NATS_WRITE_DEADLINE", ""); v != "" {
			if i, err := strconv.Atoi(v); err == nil {
				return i
			}
		}
		return 2
	}(), "write deadline (seconds)")
	authToken := flag.String("auth", getenv("NATS_AUTH", ""), "authorization token for clients (equivalent to -auth in legacy CLI)")

	flag.Parse()

	// ------------- Build server options ---------------

	opts.MaxConn = *maxConns
	opts.WriteDeadline = time.Duration(*writeDeadlineSecs) * time.Second
	opts.Authorization = strings.TrimSpace(*authToken)
	opts.DisableShortFirstPing = true
	opts.Logtime = true
	opts.Debug = false
	opts.Trace = false
	opts.NoLog = false
	opts.NoSublistCache = false
	opts.NoHeaderSupport = false
	opts.JetStream = false
	opts.NoSystemAccount = true
	opts.JetStream = false

	// client listen address
	h, p := mustSplitHostPort(*listen)
	opts.Host, opts.Port = h, p

	// HTTP monitoring
	if *httpAddr != "" {
		h, p = mustSplitHostPort(*httpAddr)
		opts.HTTPHost, opts.HTTPPort = h, p
	}

	// cluster listen address (optional)
	if *cluster != "" {
		opts.Cluster.ListenStr = *cluster
	}

	// logging level
	switch strings.ToLower(*logLevel) {
	case "debug":
		opts.Debug = true
		opts.Trace = true
	case "error":
		opts.NoLog = false
		opts.Logtime = true
		// leave debug/trace off
	default: // info
		// default already prints info level
	}

	// ----------- Launch server ------------------------
	natsServer, err := server.NewServer(opts)
	if err != nil {
		log.Fatalf("error creating server: %v", err)
	}

	go natsServer.Start()

	if !natsServer.ReadyForConnections(10 * time.Second) {
		log.Fatalf("server failed to start within timeout")
	}

	fmt.Printf("NATS server is ready at nats://%s\n", *listen)
	if *authToken != "" {
		fmt.Println("Auth mode: token")
	}
	if *httpAddr != "" {
		fmt.Printf("Monitoring endpoints at http://%s\n", *httpAddr)
	}

	// ---------- Graceful shutdown ---------------------
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	log.Printf("shutdown signal received, closing NATS (%s)...", natsServer.ID())
	natsServer.Shutdown()
	natsServer.WaitForShutdown()
	log.Println("NATS server stopped.")
}
