# StreamQueueSubscribeHandler 使用指南

## 📝 概述

`StreamQueueSubscribeHandler` 是基于队列组的同步流式订阅处理器，支持多实例负载均衡。它与 `StreamSubscribeHandler` 类似，但增加了队列组功能，实现了消息在多个订阅者之间的自动分发。

## 🚀 主要特性

- ✅ **队列组负载均衡**：多个实例自动分担工作负载
- ✅ **同步阻塞模式**：简化服务器端代码结构
- ✅ **流式响应**：支持连续发送多条响应数据
- ✅ **错误处理**：完善的错误检测和报告机制
- ✅ **上下文控制**：支持优雅退出和超时控制

## 📊 与其他方法的对比

| 特性 | StreamSubscribeHandler | StreamQueueSubscribeHandler | StreamQueueSubscribeHandlerAsync |
|------|----------------------|----------------------------|----------------------------------|
| 队列组支持 | ❌ | ✅ | ✅ |
| 负载均衡 | ❌ | ✅ | ✅ |
| 阻塞模式 | ✅ | ✅ | ❌ |
| 适用场景 | 单实例服务 | 多实例服务 | 高并发微服务 |

## 💡 基本用法

### 服务器端实现

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"
    
    "github.com/dolotech/nats.go/message"
)

func main() {
    // 创建订阅者
    subscriber, err := message.NewSubscriber([]string{"nats://localhost:4222"})
    if err != nil {
        log.Fatalf("创建订阅者失败: %v", err)
    }
    defer subscriber.Close(context.Background())

    // 创建流式处理器
    handler := func(ctx context.Context, requestMsg *nats.Msg, sender message.ResponseSender) error {
        requestData := string(requestMsg.Data)
        log.Printf("处理队列请求: %s", requestData)
        
        // 模拟流式数据处理
        for i := 0; i < 5; i++ {
            response := fmt.Sprintf("处理结果_%d: %s", i, requestData)
            if err := sender.Send([]byte(response)); err != nil {
                return err
            }
            
            // 模拟处理延迟
            time.Sleep(200 * time.Millisecond)
        }
        
        return sender.End()
    }

    // 启动队列订阅处理器（同步阻塞）
    ctx := context.Background()
    log.Println("启动流式队列订阅处理器...")
    
    err = subscriber.StreamQueueSubscribeHandler(ctx, "data.process", "workers", handler)
    if err != nil {
        log.Fatalf("队列订阅处理器错误: %v", err)
    }
}
```

### 客户端使用

```go
package main

import (
    "context"
    "log"
    "time"
    
    "github.com/dolotech/nats.go/message"
    "github.com/nats-io/nats.go"
)

func main() {
    // 创建连接池
    pool, err := message.NewNATSPool([]string{"nats://localhost:4222"}, 5)
    if err != nil {
        log.Fatalf("创建连接池失败: %v", err)
    }
    defer pool.Close()

    // 发起流式请求
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    requestData := []byte("测试数据")
    
    err = pool.StreamRequest(ctx, "data.process", requestData, func(msg *nats.Msg) bool {
        response := string(msg.Data)
        
        if response == "__STREAM_END__" {
            log.Println("✅ 流式响应结束")
            return false
        }
        
        log.Printf("📥 收到响应: %s", response)
        return true
    })
    
    if err != nil {
        log.Fatalf("流式请求失败: %v", err)
    }
}
```

## 🏭 多实例负载均衡示例

### 启动多个工作实例

```go
// worker1.go
func main() {
    subscriber, _ := message.NewSubscriber([]string{"nats://localhost:4222"})
    defer subscriber.Close(context.Background())

    handler := func(ctx context.Context, requestMsg *nats.Msg, sender message.ResponseSender) error {
        log.Printf("🔵 Worker1 处理: %s", string(requestMsg.Data))
        
        sender.Send([]byte("Worker1_处理完成"))
        return sender.End()
    }

    // 使用相同的队列组名称"workers"
    subscriber.StreamQueueSubscribeHandler(context.Background(), "orders.process", "workers", handler)
}

// worker2.go  
func main() {
    subscriber, _ := message.NewSubscriber([]string{"nats://localhost:4222"})
    defer subscriber.Close(context.Background())

    handler := func(ctx context.Context, requestMsg *nats.Msg, sender message.ResponseSender) error {
        log.Printf("🟠 Worker2 处理: %s", string(requestMsg.Data))
        
        sender.Send([]byte("Worker2_处理完成"))
        return sender.End()
    }

    // 使用相同的队列组名称"workers"
    subscriber.StreamQueueSubscribeHandler(context.Background(), "orders.process", "workers", handler)
}
```

### 负载均衡效果

当客户端发送多个请求时，NATS会自动在Worker1和Worker2之间分发：

```
请求1 → Worker1 处理
请求2 → Worker2 处理  
请求3 → Worker1 处理
请求4 → Worker2 处理
...
```

## 🛠️ 高级用法

### 1. 错误处理

```go
handler := func(ctx context.Context, requestMsg *nats.Msg, sender message.ResponseSender) error {
    requestData := string(requestMsg.Data)
    
    // 验证请求数据
    if len(requestData) == 0 {
        return sender.SendError(fmt.Errorf("请求数据为空"))
    }
    
    // 处理业务逻辑
    result, err := processBusinessLogic(requestData)
    if err != nil {
        return sender.SendError(fmt.Errorf("业务处理失败: %w", err))
    }
    
    // 发送结果
    sender.Send(result)
    return sender.End()
}
```

### 2. 超时控制

```go
func main() {
    subscriber, _ := message.NewSubscriber([]string{"nats://localhost:4222"})
    defer subscriber.Close(context.Background())

    // 设置5分钟超时
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
    defer cancel()

    err := subscriber.StreamQueueSubscribeHandler(ctx, "long.task", "workers", handler)
    if err == context.DeadlineExceeded {
        log.Println("服务超时，正在关闭...")
    }
}
```

### 3. 优雅退出

```go
func main() {
    subscriber, _ := message.NewSubscriber([]string{"nats://localhost:4222"})
    defer subscriber.Close(context.Background())

    // 监听退出信号
    ctx, cancel := context.WithCancel(context.Background())
    
    go func() {
        // 监听SIGINT信号
        c := make(chan os.Signal, 1)
        signal.Notify(c, os.Interrupt)
        <-c
        
        log.Println("收到退出信号，正在优雅关闭...")
        cancel()
    }()

    subscriber.StreamQueueSubscribeHandler(ctx, "service.api", "workers", handler)
    log.Println("服务已停止")
}
```

## 📈 性能特点

### 测试结果

我们的测试显示：

```
=== RUN   TestStreamQueueSubscribeHandler/多实例负载均衡
    负载均衡结果:
      总处理数: 6
      Worker1处理数: 4  
      Worker2处理数: 2
    ✅ 负载均衡正常工作，两个worker都处理了请求
--- PASS: TestStreamQueueSubscribeHandler/多实例负载均衡 (1.12s)
```

### 性能优势

- ✅ **自动负载均衡**：无需手动分发，NATS自动处理
- ✅ **故障容错**：单个worker故障不影响整体服务
- ✅ **水平扩展**：可随时增减worker实例
- ✅ **高可用性**：多实例确保服务连续性

## 🔧 最佳实践

### 1. 队列组命名
- 使用有意义的队列组名称，如 "order-processors", "payment-workers"
- 同一服务的所有实例使用相同的队列组名称

### 2. 错误处理
- 始终检查并正确处理业务逻辑错误
- 使用 `sender.SendError()` 发送错误信息给客户端

### 3. 资源管理
- 合理设置超时时间
- 确保在服务退出时正确清理资源

### 4. 监控建议
- 监控处理的消息数量
- 跟踪错误率和响应时间
- 观察负载均衡效果

## 🎯 适用场景

- ✅ **订单处理系统**：多个worker处理订单队列
- ✅ **数据处理服务**：批量数据处理任务分发
- ✅ **API网关**：后端服务负载均衡
- ✅ **消息队列系统**：消息处理worker集群
- ✅ **微服务架构**：服务间流式通信

---

> 💡 **提示**：如果您需要更细粒度的控制或异步处理，请考虑使用 `StreamQueueSubscribeHandlerAsync` 方法。 