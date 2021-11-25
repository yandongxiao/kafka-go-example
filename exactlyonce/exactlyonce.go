package exactlyonce

import (
	"log"
	"strconv"
	"time"

	"github.com/Shopify/sarama"

	"kafka-go-example/conf"
)

// Go 客户端 sarama 暂时并没有实现 事务功能。
// 事务型 Producer 也不惧进程的重启。Producer 重启回来后，Kafka 依然保证它们发送消息的精确一次处理。

func Producer(topic string, limit int) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	// 开启幂等性，需要一些列的设置：https://www.lixueduan.com/post/kafka/10-exactly-once-impl/
	config.Producer.Idempotent = true                // 开启幂等性
	config.Producer.RequiredAcks = sarama.WaitForAll // 开启幂等性后 acks 必须设置为 -1 即所有 isr 列表中的 broker 都ack后才ok
	config.Net.MaxOpenRequests = 1                   // 开启幂等性后 并发请求数也只能为1
	// 上述的几个额外配置完全可以由 sarama 内置,或者直接提供一个方法即可，全部需要调用者手动配置感觉体验不是很好
	producer, err := sarama.NewSyncProducer([]string{conf.HOST}, config)
	if err != nil {
		log.Fatal("NewSyncProducer err:", err)
	}
	defer producer.Close()
	for i := 0; i < limit; i++ {
		str := strconv.Itoa(int(time.Now().UnixNano()))
		msg := &sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder(str), Value: sarama.StringEncoder(str)}
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Println("SendMessage err: ", err)
			return
		}
		log.Printf("[Producer] partitionid: %d; offset:%d, value: %s\n", partition, offset, str)
	}
}
