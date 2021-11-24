package main

import (
	"time"

	"kafka-go-example/conf"
	"kafka-go-example/consumer/standalone"
	"kafka-go-example/producer/sync"
)

func main() {
	topic := conf.Topic
	// 先启动消费者,保证能消费到后续发送的消息
    // 因为 sarama.OffsetOldest 配置，消费者总是从消费位移0开始
    // 消费成功以后，kafka不会清理消息，消费者位移决定读取位置
	go standalone.SinglePartition(topic)
	time.Sleep(time.Second)

    // 向该 topic 发送 100 get消息
	sync.Producer(topic, 100)

	// sleep 等待消费结束
	time.Sleep(time.Second * 10)
}
