# NATS 流式请求-响应使用指南

## 概述

基于 NATS Core 实现的轻量级流式请求-响应功能，适用于实时数据传输场景。

## 为什么不用 NATS JetStream？

- **JetStream**: 适合需要持久化、复杂流处理的场景
- **我们的实现**: 适合轻量级、实时的流式通信，无需持久化

## 基本用法

### 1. 简单的流式请求-响应

```go
// 服务端：流式响应处理器
responseHandler := func(requestMsg *nats.Msg, responseIndex int) ([]byte, bool, error) {
    if responseIndex >= 10 { // 发送10条数据后结束
        return nil, false, nil
    }
    
    data := fmt.Sprintf("数据_%d", responseIndex)
    return []byte(data), true, nil
}

// 启动服务
subscriber.StreamSubscribeHandler(ctx, "data.stream", responseHandler)

// 客户端：发起流式请求
responseProcessor := func(msg *nats.Msg) bool {
    data := string(msg.Data)
    
    if data == "__STREAM_END__" {
        return false // 结束
    }
    
    if strings.HasPrefix(data, "__STREAM_ERROR__") {
        return false // 错误
    }
    
    fmt.Printf("收到: %s\n", data)
    return true // 继续接收
}

pool.StreamRequest(ctx, "data.stream", []byte("请求"), responseProcessor)
```

### 2. 带重试的流式请求

```go
// 自动重试，适合不稳定的服务
err := pool.StreamRequestWithRetry(
    ctx,
    "unstable.service",
    []byte("请求数据"),
    3,                    // 最大重试3次
    500*time.Millisecond, // 重试间隔
    responseProcessor,
)
```

### 3. 批量流式响应

```go
// 每次返回一批数据，提高效率
batchHandler := func(requestMsg *nats.Msg, batchIndex int) ([][]byte, bool, error) {
    if batchIndex >= 5 {
        return nil, false, nil // 结束
    }
    
    var batch [][]byte
    for i := 0; i < 10; i++ {
        data := fmt.Sprintf("批次%d_项目%d", batchIndex, i)
        batch = append(batch, []byte(data))
    }
    
    return batch, true, nil // 继续发送下一批
}

subscriber.BatchStreamSubscribeHandler(ctx, "batch.stream", batchHandler)
```

## 错误处理

- `__STREAM_END__`: 流式数据正常结束
- `__STREAM_ERROR__:<错误信息>`: 流式数据出现错误

## 适用场景

- 股票价格流
- 传感器数据流
- 实时监控数据
- 微服务间流式通信

## 注意事项

1. 使用 `context` 控制生命周期
2. 及时关闭连接池和订阅者
3. 合理处理错误和重试
4. 避免内存泄漏 