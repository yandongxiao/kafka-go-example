package main

import (
	"time"

	"kafka-go-example/conf"
	"kafka-go-example/producer/async"
)

func main() {
	topic := conf.Topic2

	// 异步生产消息
	async.Producer(topic, 100)

	time.Sleep(time.Second * 1000)
}
