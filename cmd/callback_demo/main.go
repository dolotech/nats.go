package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/dolotech/nats.go/message"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

func main() {
	// 配置日志
	cfg := zap.NewDevelopmentConfig()
	cfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	logger, _ := cfg.Build()
	zap.ReplaceGlobals(logger)

	fmt.Println("NATS 回调模式流式处理演示")
	fmt.Println("==========================")

	// 创建连接池
	poolCfg := message.Config{
		Servers:       []string{"nats://localhost:4222"},
		IdlePerServer: 5,
		MaxLife:       time.Hour,
	}
	pool, err := message.New(poolCfg)
	if err != nil {
		log.Fatalf("创建连接池失败: %v", err)
	}
	defer pool.Close()

	// 创建订阅者
	subscriber, err := message.NewSubscriber([]string{"nats://localhost:4222"})
	if err != nil {
		log.Fatalf("创建订阅者失败: %v", err)
	}
	defer subscriber.Close(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动第三方API模拟处理器
	go startThirdPartyAPIHandler(ctx, subscriber)

	// 启动股票价格流处理器
	go startStockPriceHandler(ctx, subscriber)

	// 等待服务启动
	time.Sleep(1 * time.Second)

	// 演示第三方API调用
	fmt.Println("\n1. 演示第三方API异步流式响应")
	demonstrateThirdPartyAPI(pool)

	time.Sleep(2 * time.Second)

	// 演示股票价格实时流
	fmt.Println("\n2. 演示股票价格实时数据流")
	demonstrateStockPrice(pool)

	fmt.Println("\n演示完成！")
}

// 第三方API处理器 - 模拟调用外部API
func startThirdPartyAPIHandler(ctx context.Context, subscriber *message.Subscriber) {
	handler := func(ctx context.Context, requestMsg *nats.Msg, sender message.ResponseSender) error {
		fmt.Printf("🔗 收到第三方API请求: %s\n", string(requestMsg.Data))

		// 异步处理第三方API调用
		go func() {
			defer sender.End()

			// 模拟API调用延迟
			time.Sleep(200 * time.Millisecond)

			// 模拟从第三方API获取的流式数据
			for i := 0; i < 5; i++ {
				select {
				case <-ctx.Done():
					sender.SendError(ctx.Err())
					return
				default:
					if sender.IsClosed() {
						return
					}

					// 模拟API返回的JSON数据
					apiData := fmt.Sprintf(`{
						"api_id": "external_api_v1",
						"data_index": %d,
						"value": %.2f,
						"timestamp": %d,
						"status": "success"
					}`, i, 100.0+rand.Float64()*50.0, time.Now().Unix())

					if err := sender.Send([]byte(apiData)); err != nil {
						sender.SendError(err)
						return
					}

					fmt.Printf("📤 API响应发送 #%d\n", i+1)

					// 模拟API响应间隔
					time.Sleep(300 * time.Millisecond)
				}
			}

			fmt.Println("✅ 第三方API响应完成")
		}()

		return nil // 立即返回，异步处理
	}

	err := subscriber.StreamSubscribeHandler(ctx, "demo.thirdparty.api", handler)
	if err != nil && err != context.Canceled {
		log.Printf("第三方API处理器错误: %v", err)
	}
}

// 股票价格处理器 - 模拟实时股票数据流
func startStockPriceHandler(ctx context.Context, subscriber *message.Subscriber) {
	handler := func(ctx context.Context, requestMsg *nats.Msg, sender message.ResponseSender) error {
		fmt.Printf("📈 收到股票价格订阅请求: %s\n", string(requestMsg.Data))

		// 异步生成实时股票价格流
		go func() {
			defer sender.End()

			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()

			basePrice := 150.0
			count := 0
			maxCount := 8 // 发送8次数据

			for {
				select {
				case <-ctx.Done():
					sender.SendError(ctx.Err())
					return

				case <-ticker.C:
					if sender.IsClosed() || count >= maxCount {
						return
					}

					// 模拟价格波动
					change := (rand.Float64() - 0.5) * 5.0 // ±2.5
					currentPrice := basePrice + change

					stockData := fmt.Sprintf(`{
						"symbol": "DEMO",
						"price": %.2f,
						"change": %.2f,
						"volume": %d,
						"timestamp": %d
					}`, currentPrice, change, rand.Intn(10000)+1000, time.Now().Unix())

					if err := sender.Send([]byte(stockData)); err != nil {
						sender.SendError(err)
						return
					}

					count++
					fmt.Printf("📊 股票价格更新 #%d: $%.2f\n", count, currentPrice)
				}
			}
		}()

		return nil
	}

	err := subscriber.StreamSubscribeHandler(ctx, "demo.stock.price", handler)
	if err != nil && err != context.Canceled {
		log.Printf("股票价格处理器错误: %v", err)
	}
}

// 演示第三方API调用
func demonstrateThirdPartyAPI(pool *message.Pool) {
	var receivedCount int32

	responseProcessor := func(msg *nats.Msg) bool {
		data := string(msg.Data)

		if data == "__STREAM_END__" {
			fmt.Println("🏁 第三方API响应结束")
			return false
		}

		if len(data) > 16 && data[:16] == "__STREAM_ERROR__" {
			fmt.Printf("❌ API错误: %s\n", data[17:])
			return false
		}

		atomic.AddInt32(&receivedCount, 1)
		fmt.Printf("📥 收到API数据 #%d: %s\n", receivedCount, data)
		return true
	}

	requestData := []byte(`{"endpoint": "/api/v1/data", "params": {"type": "stream"}}`)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := pool.StreamRequest(ctx, "demo.thirdparty.api", requestData, responseProcessor)
	if err != nil {
		fmt.Printf("❌ API请求失败: %v\n", err)
	} else {
		fmt.Printf("✅ API请求完成，共收到 %d 条数据\n", receivedCount)
	}
}

// 演示股票价格订阅
func demonstrateStockPrice(pool *message.Pool) {
	var priceUpdates int32

	responseProcessor := func(msg *nats.Msg) bool {
		data := string(msg.Data)

		if data == "__STREAM_END__" {
			fmt.Println("🏁 股票价格流结束")
			return false
		}

		if len(data) > 16 && data[:16] == "__STREAM_ERROR__" {
			fmt.Printf("❌ 价格流错误: %s\n", data[17:])
			return false
		}

		atomic.AddInt32(&priceUpdates, 1)
		fmt.Printf("💰 价格更新 #%d: %s\n", priceUpdates, data)
		return true
	}

	requestData := []byte(`{"symbol": "DEMO", "interval": "500ms"}`)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	err := pool.StreamRequest(ctx, "demo.stock.price", requestData, responseProcessor)
	if err != nil {
		fmt.Printf("❌ 股票订阅失败: %v\n", err)
	} else {
		fmt.Printf("✅ 股票订阅完成，共收到 %d 次价格更新\n", priceUpdates)
	}
}
