package compression

import (
	"log"
	"strings"

	"github.com/Shopify/sarama"

	"kafka-go-example/conf"
)

// Producer 端压缩、Broker 端保持、Consumer 端解压缩。
//
// 压缩
// 在 Kafka 中，压缩可能发生在两个地方：Producer 端和 Broker 端。
// 除了在 Producer 端进行压缩外有两种例外情况就可能让 Broker 重新压缩消息：
//  1. Broker 端指定了和 Producer 端不同的压缩算法。Broker 端也有一个参数叫 compression.type ，默认情况下为 Producer，表示 Broker 端会“尊重” Producer 端使用的压缩算法。
//     如果设置了不同的 compression.type 值，可能会发生预料之外的压缩 / 解压缩操作，通常表现为 Broker 端 CPU 使用率飙升。
//  2. Broker 端发生了消息格式转换。在一个生产环境中，Kafka 集群中同时保存多种版本的消息格式非常常见。为了兼容老版本的格式，Broker 端会对新版本消息执行向老版本格式的转换。
//     这个过程中会涉及消息的解压缩和重新压缩。一般情况下这种消息格式转换对性能是有很大影响的，除了这里的压缩之外，它还让 Kafka 丧失了引以为豪的 Zero Copy 特性。
//     1. Kafka 的消息层次分为两层：消息集合（message set）以及消息（message）。
//     2. 一个消息集合中包含若干条日志项（record item）
//     3. Kafka 底层的消息日志由一系列消息集合日志项组成
//     4. 目前 Kafka 共有两大类消息格式，社区分别称之为 V1 版本和 V2 版本。V2 版本是 Kafka 0.11.0.0 中正式引入的。
//          1. 在 V1 版本中，每条消息都需要执行 CRC 校验，比较浪费 CPU ，在 V2 版本中，消息的 CRC 校验工作就被移到了消息集合这一层。
//
// 谁会进行解压缩？
//  1. consumer
//  3. Broker 端也会进行解压缩。每个压缩过的消息集合在 Broker 端写入时都要发生解压缩操作，目的就是为了对消息执行各种验证。而且这种校验是非常重要的，也不能直接去掉。
//
// 谁来负责压缩？
//  1. 由于压缩是发生在 Producer 的，所以肯定是在最终通过 BrokerProudcer 到达 Kafka 之前就需要进行压缩，有了大致方向于是开始翻源码。
var defaultMsg = strings.Repeat("Golang", 1000)

func Producer(topic string, limit int) {
	config := sarama.NewConfig()
	// 指定压缩算法和压缩等级，默认没有压缩
	// config.Producer.Compression = sarama.CompressionGZIP
	// config.Producer.CompressionLevel = gzip.BestCompression
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true // 这个默认值就是 true 可以不用手动 赋值

	producer, err := sarama.NewSyncProducer([]string{conf.HOST}, config)
	if err != nil {
		log.Fatal("NewSyncProducer err:", err)
	}
	defer producer.Close()
	for i := 0; i < limit; i++ {
		msg := &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(defaultMsg)}
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Println("SendMessage err: ", err)
			return
		}
		log.Printf("[Producer] partitionid: %d; offset:%d\n", partition, offset)
	}
}
