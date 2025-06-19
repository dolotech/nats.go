# NATS‑Mini —— 高性能内存版 NATS 服务器

> 这是一个极简 Go 包装器，内嵌 **github.com/nats‑io/nats‑server/v2**，启动即得到关闭 JetStream 的纯 Core NATS 节点，默认参数针对**高吞吐、低延迟**做了优化。适用于服务网格 Side‑car、本地开发或任何只想要 “消息而不要磁盘” 的场景。

---

## 快速上手

```bash
# 直接运行（需 Go ≥1.22）
go run ./cmd/nats-mini

# 或编译后再运行
go build -o nats-mini ./cmd/nats-mini
./nats-mini -listen 0.0.0.0:4222 -http :8222
```

启动后，程序在通过自检后会输出客户端连接地址及监控端点。

---

## 构建要求

| 依赖          | 版本              |
| ----------- | --------------- |
| Go          | ≥ 1.22          |
| nats-server | 自动由 `go mod` 拉取 |

```bash
git clone https://github.com/your-org/nats-mini.git
cd nats-mini
go build -o nats-mini .
```

---

## 配置方式一览

既支持 **命令行参数**，也支持 **环境变量**（在 Docker/K8s 中更方便）。若两者并存，以 *命令行参数* 优先。

| 参数                | 环境变量                  | 默认值            | 说明                              |
| ----------------- | --------------------- | -------------- | ------------------------------- |
| `-listen`         | `NATS_LISTEN`         | `0.0.0.0:4222` | 客户端监听地址                         |
| `-http`           | `NATS_HTTP`           | `0.0.0.0:8222` | HTTP 监控端点                       |
| `-cluster`        | `NATS_CLUSTER`        | *(空)*          | 集群监听地址（启用后可互联）                  |
| `-auth`           | `NATS_AUTH`           | *(空)*          | 简易令牌认证（客户端作为密码发送）               |
| `-max_conns`      | `NATS_MAX_CONNS`      | `65536`        | 最大并发客户端连接数                      |
| `-write_deadline` | `NATS_WRITE_DEADLINE` | `2`            | 慢消费者写超时（秒）                      |
| `-log`            | `NATS_LOG`            | `info`         | 日志级别：`debug` / `info` / `error` |

### 源码中可调的高级限额

| 键              | 当前值       | 含义                                        |
| -------------- | --------- | ----------------------------------------- |
| `MaxPayload`   | **2 MiB** | 单条消息最大字节数（超过即被拒）。可改到 64 MiB，但官方建议 ≤8 MiB。 |
| `MaxPending`   | 128 MiB   | 每连接待发送缓冲。                                 |
| `PingInterval` | 60 s      | 服务器空闲时向客户端发送 PING 的间隔。                    |
| `MaxPingsOut`  | 3         | 连续几次 PING 未得到 PONG 即断开。                   |

> **提示** 如果想运行时修改这些高阶参数，可写 `nats-server.conf`（HOCON/JSON/TOML 均可）再用 `-c` 启动；内嵌服务器会合并配置。

---

## 监控

所有 HTTP 监控接口均由 `-http` 指定的地址提供。

| 路径        | 说明            |
| --------- | ------------- |
| `/varz`   | 服务器总体指标、内存占用  |
| `/connz`  | 当前连接详情、待发送字节  |
| `/subsz`  | 订阅列表及计数       |
| `/routez` | 集群路由信息（启用集群时） |

Prometheus 可直接抓取上述接口。

---

## 集群示例

```bash
./nats-mini -cluster 0.0.0.0:6222 -routes nats://n1:6222,nats://n2:6222
```

包装器仅暴露 *监听* 端口（`-cluster`），若需完整路由网，请在配置文件中补 `routes`。

---

## 容器镜像示例（多架构）

```dockerfile
FROM --platform=$BUILDPLATFORM golang:1.22-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -ldflags "-s -w" -o /out/nats-mini .

FROM gcr.io/distroless/static
COPY --from=build /out/nats-mini /nats-mini
ENV NATS_LOG=error
EXPOSE 4222 8222
ENTRYPOINT ["/nats-mini"]
```

---

## 操作系统 / Kubernetes 调优速查

```bash
# 提高文件描述符上限
aulimit -n 1048576

# 允许更大的连接积压
sysctl -w net.core.somaxconn=8192

# 为 25Gbps 网卡调大 TCP 缓冲
sysctl -w net.ipv4.tcp_rmem='4096 87380 33554432' \
           net.ipv4.tcp_wmem='4096 65536 33554432'
```

Helm `values.yaml` 片段：

```yaml
nats:
  limits:
    maxConnections: 1_000_000
    maxPayload: "2mb"
  resources:
    limits:
      memory: 2Gi
      cpu: "4"
```

---

## 优雅停止

进程捕获 **SIGINT/SIGTERM**：

1. 调用 `Server.Shutdown()`，拒绝新连接；
2. 等待存量连接自然断开；
3. 退出码 `0`。

---

## 常见问题排查

| 症状                               | 可能原因                | 解决方案                                       |
| -------------------------------- | ------------------- | ------------------------------------------ |
| `nats: maximum payload exceeded` | 消息尺寸 > `MaxPayload` | 拆分数据或提高 `max_payload` 限制                   |
| `Slow Consumer Detected`         | 客户端读取不及时            | 调大 `-write_deadline`、优化消费者或提高 `MaxPending` |

---

## 许可证

Apache‑2.0（与上游 *nats-server* 保持一致）。
