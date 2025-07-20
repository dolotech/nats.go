package message

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

// StreamRequestExample 演示基本流式请求功能
func StreamRequestExample() {
	// 创建连接池
	cfg := Config{
		Servers:       []string{"nats://localhost:4222"},
		IdlePerServer: 8,
		MaxLife:       time.Hour,
	}
	pool, err := New(cfg)
	if err != nil {
		log.Fatalf("创建连接池失败: %v", err)
	}
	defer pool.Close()

	// 创建流式响应处理器
	subscriber, err := NewSubscriber([]string{"nats://localhost:4222"})
	if err != nil {
		log.Fatalf("创建订阅者失败: %v", err)
	}
	defer subscriber.Close(context.Background())

	// 启动流式响应处理器
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 定义流式响应处理器 - 模拟实时数据生成
	responseHandler := func(requestMsg *nats.Msg, responseIndex int) ([]byte, bool, error) {
		if responseIndex >= 10 { // 发送10条数据后结束
			return nil, false, nil
		}

		// 模拟生成实时股票价格数据
		price := 100.0 + float64(responseIndex)*0.5
		responseData := fmt.Sprintf("{\"symbol\":\"AAPL\",\"price\":%.2f,\"timestamp\":%d}",
			price, time.Now().Unix())

		return []byte(responseData), true, nil
	}

	// 启动股票价格流式服务
	go func() {
		err := subscriber.StreamSubscribeHandler(ctx, "stock.price.stream", responseHandler)
		if err != nil && err != context.Canceled {
			log.Printf("流式响应处理器错误: %v", err)
		}
	}()

	// 等待服务启动
	time.Sleep(100 * time.Millisecond)

	// 发起流式请求获取股票价格流
	requestCtx, requestCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer requestCancel()

	requestData := []byte(`{"symbol":"AAPL","subscribe":true}`)

	// 定义响应处理器
	responseProcessor := func(msg *nats.Msg) bool {
		data := string(msg.Data)

		// 检查结束信号
		if data == "__STREAM_END__" {
			fmt.Println("股票价格流已结束")
			return false
		}

		// 检查错误信号
		if len(data) > 15 && data[:15] == "__STREAM_ERROR__" {
			fmt.Printf("股票价格流错误: %s\n", data[16:])
			return false
		}

		// 处理股票价格数据
		fmt.Printf("收到股票价格: %s\n", data)
		return true
	}

	fmt.Println("开始请求股票价格流...")
	err = pool.StreamRequest(requestCtx, "stock.price.stream", requestData, responseProcessor)
	if err != nil {
		fmt.Printf("流式请求结束: %v\n", err)
	}
}

// BatchStreamExample 演示批量流式请求功能
func BatchStreamExample() {
	// 创建连接池
	cfg := Config{
		Servers:       []string{"nats://localhost:4222"},
		IdlePerServer: 8,
	}
	pool, err := New(cfg)
	if err != nil {
		log.Fatalf("创建连接池失败: %v", err)
	}
	defer pool.Close()

	// 创建批量流式响应处理器
	subscriber, err := NewSubscriber([]string{"nats://localhost:4222"})
	if err != nil {
		log.Fatalf("创建订阅者失败: %v", err)
	}
	defer subscriber.Close(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 批量响应处理器 - 模拟批量数据分析结果
	batchResponseHandler := func(requestMsg *nats.Msg, batchIndex int) ([][]byte, bool, error) {
		if batchIndex >= 5 { // 发送5批数据
			return nil, false, nil
		}

		var batchData [][]byte
		// 每批生成3个分析结果
		for i := 0; i < 3; i++ {
			result := fmt.Sprintf(`{"batch":%d,"item":%d,"analysis":"trend_up","confidence":%.2f}`,
				batchIndex, i, 0.8+float64(i)*0.05)
			batchData = append(batchData, []byte(result))
		}

		return batchData, true, nil
	}

	// 启动批量分析服务
	go func() {
		err := subscriber.BatchStreamSubscribeHandler(ctx, "market.analysis.batch", batchResponseHandler)
		if err != nil && err != context.Canceled {
			log.Printf("批量响应处理器错误: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// 发起批量流式请求
	requestCtx, requestCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer requestCancel()

	requestData := []byte(`{"market":"NASDAQ","analysis_type":"trend","batch_size":3}`)

	batchCount := 0
	totalItems := 0

	responseProcessor := func(msg *nats.Msg) bool {
		data := string(msg.Data)

		if data == "__STREAM_END__" {
			fmt.Printf("批量分析完成，总批次: %d, 总项目: %d\n", batchCount, totalItems)
			return false
		}

		if len(data) > 15 && data[:15] == "__STREAM_ERROR__" {
			fmt.Printf("批量分析错误: %s\n", data[16:])
			return false
		}

		totalItems++

		// 检测新批次
		if len(data) > 24 && data[:8] == `{"batch":` && data[17:24] == `,"item":0` {
			batchCount++
			fmt.Printf("开始处理第 %d 批次\n", batchCount)
		}

		fmt.Printf("收到分析结果: %s\n", data)
		return true
	}

	fmt.Println("开始请求批量市场分析...")
	err = pool.StreamRequest(requestCtx, "market.analysis.batch", requestData, responseProcessor)
	if err != nil {
		fmt.Printf("批量流式请求结束: %v\n", err)
	}
}

// RetryStreamExample 演示带重试的流式请求功能
func RetryStreamExample() {
	// 创建连接池
	cfg := Config{
		Servers:       []string{"nats://localhost:4222"},
		IdlePerServer: 8,
	}
	pool, err := New(cfg)
	if err != nil {
		log.Fatalf("创建连接池失败: %v", err)
	}
	defer pool.Close()

	// 创建不稳定的响应处理器
	subscriber, err := NewSubscriber([]string{"nats://localhost:4222"})
	if err != nil {
		log.Fatalf("创建订阅者失败: %v", err)
	}
	defer subscriber.Close(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 模拟不稳定的服务 - 前几次请求失败
	requestCount := 0
	unstableResponseHandler := func(requestMsg *nats.Msg, responseIndex int) ([]byte, bool, error) {
		requestCount++

		// 前2次请求模拟服务器错误
		if requestCount <= 2 {
			return nil, false, fmt.Errorf("服务暂时不可用，请求次数: %d", requestCount)
		}

		// 第3次请求开始正常工作
		if responseIndex >= 5 {
			return nil, false, nil
		}

		responseData := fmt.Sprintf(`{"status":"success","data":"重试成功数据_%d","attempt":%d}`,
			responseIndex, requestCount)
		return []byte(responseData), true, nil
	}

	// 启动不稳定服务
	go func() {
		err := subscriber.StreamSubscribeHandler(ctx, "unstable.service", unstableResponseHandler)
		if err != nil && err != context.Canceled {
			log.Printf("不稳定服务错误: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// 响应处理器
	receivedCount := 0
	responseProcessor := func(msg *nats.Msg) bool {
		data := string(msg.Data)

		if len(data) > 15 && data[:15] == "__STREAM_ERROR__" {
			fmt.Printf("收到服务错误: %s\n", data[16:])
			return false
		}

		if data == "__STREAM_END__" {
			fmt.Printf("服务响应完成，总共收到 %d 条数据\n", receivedCount)
			return false
		}

		receivedCount++
		fmt.Printf("收到数据 [%d]: %s\n", receivedCount, data)
		return true
	}

	// 发起带重试的流式请求
	requestData := []byte(`{"service":"data_query","retry":true}`)

	fmt.Println("开始请求不稳定服务（带重试）...")
	err = pool.StreamRequestWithRetry(context.Background(), "unstable.service", requestData,
		3, 500*time.Millisecond, responseProcessor)

	if err != nil {
		fmt.Printf("重试流式请求最终结果: %v\n", err)
	}
}

// RealTimeDataStreamExample 演示实时数据流场景
func RealTimeDataStreamExample() {
	fmt.Println("=== 实时数据流演示 ===")

	// 创建连接池
	cfg := Config{
		Servers:       []string{"nats://localhost:4222"},
		IdlePerServer: 8,
	}
	pool, err := New(cfg)
	if err != nil {
		log.Fatalf("创建连接池失败: %v", err)
	}
	defer pool.Close()

	// 创建数据流订阅者
	subscriber, err := NewSubscriber([]string{"nats://localhost:4222"})
	if err != nil {
		log.Fatalf("创建订阅者失败: %v", err)
	}
	defer subscriber.Close(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 实时数据流处理器
	dataStreamHandler := func(requestMsg *nats.Msg, responseIndex int) ([]byte, bool, error) {
		if responseIndex >= 8 { // 发送8条数据
			return nil, false, nil
		}

		// 模拟传感器数据
		sensors := []string{"temperature", "humidity", "pressure", "light"}
		sensor := sensors[responseIndex%4]

		// 生成模拟数据
		var value float64
		switch sensor {
		case "temperature":
			value = 20.0 + float64(responseIndex)*0.5
		case "humidity":
			value = 50.0 + float64(responseIndex)*1.2
		case "pressure":
			value = 1013.0 + float64(responseIndex)*0.3
		case "light":
			value = 300.0 + float64(responseIndex)*10.0
		}

		dataResult := fmt.Sprintf(`{
			"timestamp":"%s",
			"sensor":"%s",
			"value":%.2f,
			"status":"normal",
			"sequence":%d
		}`, time.Now().Format(time.RFC3339), sensor, value, responseIndex)

		return []byte(dataResult), true, nil
	}

	// 启动实时数据流服务
	go func() {
		err := subscriber.StreamSubscribeHandler(ctx, "sensor.data.stream", dataStreamHandler)
		if err != nil && err != context.Canceled {
			log.Printf("数据流服务错误: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// 发起实时数据流请求
	requestCtx, requestCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer requestCancel()

	requestData := []byte(`{"sensors":["all"],"interval":"1s","format":"json"}`)

	dataCount := 0
	responseProcessor := func(msg *nats.Msg) bool {
		data := string(msg.Data)

		if data == "__STREAM_END__" {
			fmt.Printf("数据流结束，总共收到 %d 条数据\n", dataCount)
			return false
		}

		if len(data) > 15 && data[:15] == "__STREAM_ERROR__" {
			fmt.Printf("数据流错误: %s\n", data[16:])
			return false
		}

		dataCount++
		fmt.Printf("数据 [%d]: %s\n", dataCount, data)

		return true
	}

	fmt.Println("开始实时数据流...")
	err = pool.StreamRequest(requestCtx, "sensor.data.stream", requestData, responseProcessor)
	if err != nil {
		fmt.Printf("数据流请求结束: %v\n", err)
	}
}

// RunAllStreamExamples 运行所有流式功能示例
func RunAllStreamExamples() {
	fmt.Println("=== NATS 简化流式请求-响应功能演示 ===")
	fmt.Println()

	fmt.Println("1. 基本流式请求演示（股票价格流）")
	StreamRequestExample()
	fmt.Println()

	fmt.Println("2. 批量流式请求演示（市场分析）")
	BatchStreamExample()
	fmt.Println()

	fmt.Println("3. 带重试的流式请求演示（不稳定服务）")
	RetryStreamExample()
	fmt.Println()

	fmt.Println("4. 实时数据流综合演示")
	RealTimeDataStreamExample()
	fmt.Println()

	fmt.Println("=== 所有流式功能演示完成 ===")
}
