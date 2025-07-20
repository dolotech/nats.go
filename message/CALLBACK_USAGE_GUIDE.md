# NATS 回调模式流式处理指南

## 概述

本指南展示如何使用 NATS 的回调模式流式处理器，这是一个更优雅、更灵活的第三方API流式响应处理方案。

## 核心概念

### ResponseSender 接口

```go
type ResponseSender interface {
    Send(data []byte) error      // 发送数据
    SendError(err error) error   // 发送错误
    End() error                  // 结束流式响应
    IsClosed() bool              // 检查是否已关闭
}
```

### CallbackStreamHandler 处理器

```go
type CallbackStreamHandler func(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error
```

## 基本使用示例

### 1. 简单流式响应

```go
func simpleStreamHandler(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
    // 立即返回，异步处理
    go func() {
        defer sender.End() // 确保最终关闭
        
        for i := 0; i < 10; i++ {
            // 检查上下文和连接状态
            select {
            case <-ctx.Done():
                sender.SendError(ctx.Err())
                return
            default:
                if sender.IsClosed() {
                    return // 客户端已断开
                }
                
                data := fmt.Sprintf("响应数据_%d", i)
                if err := sender.Send([]byte(data)); err != nil {
                    sender.SendError(err)
                    return
                }
                
                time.Sleep(100 * time.Millisecond)
            }
        }
    }()
    
    return nil // 立即返回，不阻塞
}
```

### 2. 第三方API调用示例

```go
func thirdPartyAPIHandler(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
    go func() {
        defer sender.End()
        
        // 调用第三方API
        apiClient := &http.Client{Timeout: 30 * time.Second}
        
        // 解析请求参数
        var request APIRequest
        if err := json.Unmarshal(requestMsg.Data, &request); err != nil {
            sender.SendError(fmt.Errorf("解析请求失败: %v", err))
            return
        }
        
        // 发起第三方API请求
        resp, err := apiClient.Get(fmt.Sprintf("https://api.example.com/stream?param=%s", request.Param))
        if err != nil {
            sender.SendError(fmt.Errorf("API调用失败: %v", err))
            return
        }
        defer resp.Body.Close()
        
        // 流式读取响应
        scanner := bufio.NewScanner(resp.Body)
        for scanner.Scan() {
            select {
            case <-ctx.Done():
                sender.SendError(ctx.Err())
                return
            default:
                if sender.IsClosed() {
                    return
                }
                
                line := scanner.Bytes()
                if len(line) > 0 {
                    if err := sender.Send(line); err != nil {
                        sender.SendError(err)
                        return
                    }
                }
            }
        }
        
        if err := scanner.Err(); err != nil {
            sender.SendError(fmt.Errorf("读取API响应失败: %v", err))
        }
    }()
    
    return nil
}
```

### 3. 股票价格实时流

```go
type StockStreamHandler struct {
    symbol   string
    interval time.Duration
}

func NewStockStreamHandler(symbol string, interval time.Duration) CallbackStreamHandler {
    handler := &StockStreamHandler{
        symbol:   symbol,
        interval: interval,
    }
    
    return handler.handleRequest
}

func (h *StockStreamHandler) handleRequest(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
    go func() {
        defer sender.End()
        
        ticker := time.NewTicker(h.interval)
        defer ticker.Stop()
        
        count := 0
        for {
            select {
            case <-ctx.Done():
                sender.SendError(ctx.Err())
                return
                
            case <-ticker.C:
                if sender.IsClosed() {
                    return
                }
                
                // 调用股票API获取实时价格
                price, err := h.fetchStockPrice()
                if err != nil {
                    sender.SendError(err)
                    return
                }
                
                priceData := map[string]interface{}{
                    "symbol":    h.symbol,
                    "price":     price,
                    "timestamp": time.Now().Unix(),
                    "sequence":  count,
                }
                
                jsonData, _ := json.Marshal(priceData)
                if err := sender.Send(jsonData); err != nil {
                    sender.SendError(err)
                    return
                }
                
                count++
                
                // 可以设置最大发送次数
                if count >= 100 {
                    return
                }
            }
        }
    }()
    
    return nil
}

func (h *StockStreamHandler) fetchStockPrice() (float64, error) {
    // 实际实现中调用真实的股票API
    // 这里模拟返回价格
    return 100.0 + rand.Float64()*10.0, nil
}
```

## 服务端使用

```go
func main() {
    // 创建订阅者
    subscriber, err := NewSubscriber([]string{"nats://localhost:4222"})
    if err != nil {
        log.Fatalf("创建订阅者失败: %v", err)
    }
    defer subscriber.Close(context.Background())

    ctx := context.Background()

    // 注册不同的流式处理器
    go subscriber.StreamSubscribeHandler(ctx, "simple.stream", simpleStreamHandler)
    go subscriber.StreamSubscribeHandler(ctx, "api.stream", thirdPartyAPIHandler)
    go subscriber.StreamSubscribeHandler(ctx, "stock.AAPL", NewStockStreamHandler("AAPL", time.Second))

    log.Println("流式处理器已启动...")
    
    // 保持服务运行
    select {}
}
```

## 客户端使用

```go
func main() {
    // 创建连接池
    pool, err := NewNATSPool([]string{"nats://localhost:4222"}, 5)
    if err != nil {
        log.Fatalf("创建连接池失败: %v", err)
    }
    defer pool.Close()

    // 流式响应处理器
    responseProcessor := func(msg *nats.Msg) bool {
        data := string(msg.Data)
        
        // 处理结束信号
        if data == "__STREAM_END__" {
            log.Println("流式响应结束")
            return false
        }
        
        // 处理错误信号
        if strings.HasPrefix(data, "__STREAM_ERROR__") {
            log.Printf("收到错误: %s", data)
            return false
        }
        
        // 处理实际数据
        log.Printf("收到数据: %s", data)
        return true
    }

    // 发起流式请求
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    requestData := []byte(`{"param": "test"}`)
    
    err = pool.StreamRequest(ctx, "api.stream", requestData, responseProcessor)
    if err != nil {
        log.Fatalf("流式请求失败: %v", err)
    }
}
```

## 错误处理最佳实践

### 1. 超时处理

```go
func timeoutAwareHandler(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
    // 为每个请求设置超时
    requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
    defer cancel()
    
    go func() {
        defer sender.End()
        
        for i := 0; i < 100; i++ {
            select {
            case <-requestCtx.Done():
                if requestCtx.Err() == context.DeadlineExceeded {
                    sender.SendError(fmt.Errorf("处理超时"))
                } else {
                    sender.SendError(requestCtx.Err())
                }
                return
            default:
                // 处理数据...
            }
        }
    }()
    
    return nil
}
```

### 2. 重试机制

```go
func retryableAPIHandler(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
    go func() {
        defer sender.End()
        
        maxRetries := 3
        for attempt := 0; attempt < maxRetries; attempt++ {
            select {
            case <-ctx.Done():
                sender.SendError(ctx.Err())
                return
            default:
                // 尝试调用API
                if err := callExternalAPI(ctx, sender); err != nil {
                    if attempt == maxRetries-1 {
                        sender.SendError(fmt.Errorf("重试%d次后仍失败: %v", maxRetries, err))
                        return
                    }
                    
                    // 等待后重试
                    time.Sleep(time.Duration(attempt+1) * time.Second)
                    continue
                }
                
                // 成功
                return
            }
        }
    }()
    
    return nil
}
```

### 3. 资源清理

```go
func resourceAwareHandler(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
    go func() {
        defer sender.End()
        
        // 创建需要清理的资源
        conn, err := sql.Open("mysql", "dsn")
        if err != nil {
            sender.SendError(err)
            return
        }
        defer conn.Close() // 确保资源清理
        
        file, err := os.Open("data.txt")
        if err != nil {
            sender.SendError(err)
            return
        }
        defer file.Close()
        
        // 使用资源处理数据...
    }()
    
    return nil
}
```

## 监控和日志

```go
func monitoredHandler(ctx context.Context, requestMsg *nats.Msg, sender ResponseSender) error {
    startTime := time.Now()
    
    go func() {
        defer func() {
            duration := time.Since(startTime)
            zap.S().Infow("流式处理完成",
                "subject", requestMsg.Subject,
                "duration", duration,
                "sent_count", sender.(*streamResponseSender).GetSendCount(),
            )
            sender.End()
        }()
        
        // 处理逻辑...
    }()
    
    return nil
}
```

## 性能优化建议

1. **合理使用缓冲**: 对于高频数据，考虑使用缓冲机制批量发送
2. **控制并发**: 使用信号量或限流器控制并发处理数
3. **资源池化**: 复用数据库连接、HTTP客户端等资源
4. **监控指标**: 监控处理时间、错误率、吞吐量等关键指标
5. **优雅降级**: 在系统负载高时，提供降级方案

## 总结

回调模式的优势：

- ✅ **异步非阻塞**: 处理器立即返回，异步推送数据
- ✅ **灵活控制**: 可检查连接状态，避免无效发送
- ✅ **错误处理**: 清晰的错误处理机制
- ✅ **适合第三方API**: 完美支持异步的第三方API调用
- ✅ **资源效率**: 避免阻塞，提高系统吞吐量
- ✅ **可监控性**: 提供丰富的状态检查和监控接口 