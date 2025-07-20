# NATS 流式请求-响应功能

## 概述

本实现基于 NATS 消息系统，提供了完整的流式请求-响应功能。采用**请求-响应模型**，即只有在发起流式请求时，才会收到对应的流式消息响应。

## 核心特性

### 1. 流式请求功能 (Connection Pool)

- **基本流式请求**: 发起请求后持续接收流式响应数据
- **带超时的流式请求**: 支持单次请求超时和整体流式会话超时
- **带重试的流式请求**: 当流式连接中断时自动重试

### 2. 流式订阅处理 (Subscriber)

- **流式响应处理器**: 接收请求后持续发送流式响应数据
- **批量流式响应**: 支持批量生成和发送流式响应数据
- **流式订阅写入**: 将流式响应数据持续写入到文件

### 3. 数据写入器

- **StreamWriter 接口**: 通用的流式写入器接口
- **HourlyFileWriter**: 按小时分割的文件写入器
- **自动文件切换**: 根据时间自动创建新的日志文件

## API 接口

### 连接池 (Pool) 方法

```go
// 基本流式请求
func (p *Pool) StreamRequest(ctx context.Context, subject string, requestData []byte, responseHandler func(*nats.Msg) bool) error

// 带超时的流式请求
func (p *Pool) StreamRequestWithTimeout(ctx context.Context, subject string, requestData []byte, requestTimeout, streamTimeout time.Duration, responseHandler func(*nats.Msg) bool) error

// 带重试的流式请求
func (p *Pool) StreamRequestWithRetry(ctx context.Context, subject string, requestData []byte, maxRetries int, retryDelay time.Duration, responseHandler func(*nats.Msg) bool) error
```

### 订阅者 (Subscriber) 方法

```go
// 流式响应处理器
func (s *Subscriber) StreamSubscribeHandler(ctx context.Context, subject string, responseHandler StreamResponseHandler) error

// 批量流式响应处理器
func (s *Subscriber) BatchStreamSubscribeHandler(ctx context.Context, subject string, batchResponseHandler func(*nats.Msg, int) ([][]byte, bool, error)) error

// 流式订阅写入文件
func (s *Subscriber) StreamSubscribeWithWriter(ctx context.Context, subject string, writer StreamWriter, formatter func(*nats.Msg) []byte) error
```

### 数据写入器

```go
// 创建按小时分割的文件写入器
func NewHourlyFileWriter(baseDir, filename string) *HourlyFileWriter

// StreamWriter 接口
type StreamWriter interface {
    io.Writer
    Flush() error
    Close() error
}
```

## 使用示例

### 1. 基本流式请求-响应

```go
// 创建连接池
pool, err := New(Config{
    Servers: []string{"nats://localhost:4222"},
    IdlePerServer: 8,
})
defer pool.Close()

// 创建流式响应处理器
subscriber, err := NewSubscriber([]string{"nats://localhost:4222"})
defer subscriber.Close(context.Background())

// 定义流式响应处理器
responseHandler := func(requestMsg *nats.Msg, responseIndex int) ([]byte, bool, error) {
    if responseIndex >= 10 { // 发送10条响应后结束
        return nil, false, nil
    }
    
    responseData := fmt.Sprintf("响应_%d", responseIndex)
    return []byte(responseData), true, nil
}

// 启动流式响应服务
ctx, cancel := context.WithCancel(context.Background())
go subscriber.StreamSubscribeHandler(ctx, "data.stream", responseHandler)

// 发起流式请求
requestData := []byte("请求数据")
responseProcessor := func(msg *nats.Msg) bool {
    data := string(msg.Data)
    
    if data == "__STREAM_END__" {
        fmt.Println("流式数据结束")
        return false
    }
    
    if strings.HasPrefix(data, "__STREAM_ERROR__") {
        fmt.Printf("流式错误: %s\n", data[16:])
        return false
    }
    
    fmt.Printf("收到数据: %s\n", data)
    return true
}

err = pool.StreamRequest(context.Background(), "data.stream", requestData, responseProcessor)
```

### 2. 流式数据写入文件

```go
// 创建文件写入器
writer := NewHourlyFileWriter("/var/log/nats", "stream_data")

// 创建订阅者
subscriber, err := NewSubscriber([]string{"nats://localhost:4222"})
defer subscriber.Close(context.Background())

// 消息格式化器
formatter := func(msg *nats.Msg) []byte {
    timestamp := time.Now().Format("2006-01-02 15:04:05.000")
    line := fmt.Sprintf("[%s] %s: %s\n", timestamp, msg.Subject, string(msg.Data))
    return []byte(line)
}

// 启动流式写入
ctx, cancel := context.WithCancel(context.Background())
err = subscriber.StreamSubscribeWithWriter(ctx, "logs.*", writer, formatter)
```

### 3. 带重试的流式请求

```go
// 响应处理器
responseProcessor := func(msg *nats.Msg) bool {
    data := string(msg.Data)
    
    if strings.HasPrefix(data, "__STREAM_ERROR__") {
        return false // 遇到错误结束当前请求，触发重试
    }
    
    if data == "__STREAM_END__" {
        return false
    }
    
    fmt.Printf("收到数据: %s\n", data)
    return true
}

// 发起带重试的流式请求
err = pool.StreamRequestWithRetry(
    context.Background(),
    "unstable.service",
    []byte("请求数据"),
    3,                    // 最大重试3次
    500*time.Millisecond, // 重试间隔500ms
    responseProcessor,
)
```

## 错误处理

### 流式信号

系统使用特殊的信号来标识流式状态：

- `__STREAM_END__`: 流式数据正常结束
- `__STREAM_ERROR__:<错误信息>`: 流式数据出现错误

### 重试机制

当收到错误信号时，`StreamRequestWithRetry` 会自动重试：

1. 检查错误类型，判断是否可重试
2. 等待指定的重试间隔
3. 重新发起流式请求
4. 达到最大重试次数后返回最后的错误

## 性能特性

### 连接池优化

- 每个服务器维护独立的连接池
- 支持连接生命周期管理
- 自动故障转移和健康检查

### 订阅处理优化

- 异步消息处理，避免阻塞
- 并发控制，防止 goroutine 泄漏
- 批量处理，提高吞吐量

### 文件写入优化

- 按时间自动分割文件
- 批量写入和定时刷新
- 支持并发写入

## 测试用例

完整的测试套件包括：

- **基本流式请求-响应测试**: 验证基本功能
- **超时控制测试**: 验证超时机制
- **批量处理测试**: 验证批量功能  
- **文件写入测试**: 验证数据持久化
- **重试机制测试**: 验证故障恢复

运行测试：

```bash
# 运行所有流式功能测试
go test -v ./message -run "TestStreamRequestResponse|TestStreamRequestRetry"

# 运行特定测试
go test -v ./message -run TestStreamRequestResponse
```

## 应用场景

### 1. 实时数据流

- 股票价格流
- 传感器数据流
- 日志流分析

### 2. 数据处理管道

- 批量数据处理
- 流式 ETL
- 实时计算

### 3. 监控和告警

- 系统监控数据流
- 实时告警
- 性能指标收集

## 注意事项

1. **上下文管理**: 使用 context 控制流式会话的生命周期
2. **资源清理**: 确保正确关闭连接池和订阅者
3. **错误处理**: 适当处理流式错误和重试逻辑
4. **内存管理**: 注意批量处理时的内存使用
5. **并发控制**: 合理设置并发度，避免系统过载

## 扩展性

该实现具有良好的扩展性：

- 可自定义响应处理器
- 可实现自定义写入器
- 支持自定义错误处理逻辑
- 可配置重试策略

通过这些特性，可以满足各种复杂的流式数据处理需求。 