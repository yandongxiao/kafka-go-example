package main

import (
	"kafka-go-example/conf"
	"kafka-go-example/producer/sync"
)

func main() {
	topic := conf.Topic2

	// 向该 topic 发送 100 get消息
	sync.Producer(topic, 100)
}
