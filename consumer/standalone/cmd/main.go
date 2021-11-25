package main

import (
	"kafka-go-example/conf"
	"kafka-go-example/consumer/standalone"
)

// 测试 独立消费者 先启动消费者再启动生产者
func main() {
	topic := conf.Topic
	standalone.Partitions(topic)
}
