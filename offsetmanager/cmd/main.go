package main

import (
	"kafka-go-example/conf"
	"kafka-go-example/offsetmanager"
)

func main() {
	offsetmanager.OffsetManager(conf.Topic)
}
