package standalone

import (
	"log"
	"sync"

	"github.com/Shopify/sarama"

	"kafka-go-example/conf"
)

/*
	本例展示最简单的 独立消费者 的使用（除独立消费者外 kafka 还有消费者组）
	本例中只包含了消息的消费逻辑，没有ACK相关逻辑,即所有消息都可以重复消费。

	注:kafka 中使用的是 offset 机制，每条消息都有一个 offset(类似于消息ID)，每个消费者会维护自己的消费 offset，
	kafka 中通过消费者 offset 和消息 offset 来区分哪些消息已消费，哪些没有。没有使用其他MQ的ACK机制。
	其他MQ中消息ACK后就会被删除，kafka 则不会，kafka 消息过期后才会删除，且过期时间可以自定义，
	即就算消费者A把所有消息都消费了，也可以重置自己的 offset 然后再从头开始消费。
	所以甚至可以将 kafka 用于存储消息。

名词: standalone consumer、topic、partition、offset
*/

// SinglePartition 单分区消费
// Topics() ([]string, error)
// Partitions(topic string) ([]int32, error)
// ConsumePartition(topic string, partition int32, offset int64) (PartitionConsumer, error)
// HighWaterMarks() map[string]map[int32]int64
// Close() error
func SinglePartition(topic string) {
	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer([]string{conf.HOST}, config)
	if err != nil {
		log.Fatal("NewConsumer err: ", err)
	}
	defer consumer.Close()

	// 参数1 指定消费哪个 topic
	// 参数2 分区 这里默认消费 0 号分区 kafka 中有分区的概念，类似于ES和MongoDB中的sharding，MySQL中的分表这种
	// 参数3 offset 从哪儿开始消费起走，正常情况下每次消费完都会将这次的offset提交到kafka，然后下次可以接着消费，
	// 这里demo就从最新的开始消费，即该 consumer 启动之前产生的消息都无法被消费
	// 如果改为 sarama.OffsetOldest 则会从最旧的消息开始消费，即每次重启 consumer 都会把该 topic 下的所有消息消费一次
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatal("ConsumePartition err: ", err)
	}
	defer partitionConsumer.Close()

	// 会一直阻塞在这里
	for message := range partitionConsumer.Messages() {
		log.Printf("[Consumer] partitionid: %d; offset:%d, value: %s\n", message.Partition, message.Offset, string(message.Value))
	}
}

// Partitions 多分区消费
// 开启多个协程，每个协程负责一个分区
func Partitions(topic string) {
	config := sarama.NewConfig()
	// sarama.NewConsumer 没有区分同步 consumer 还是异步 consumer
	consumer, err := sarama.NewConsumer([]string{conf.HOST}, config)
	if err != nil {
		log.Fatal("NewConsumer err: ", err)
	}
	defer consumer.Close() // 注意关闭

	// consumer.Topics() 方法：获取所有的topic名称
	topics, _ := consumer.Topics()
	for i := range topics {
		log.Printf("tolic: %s", topics[i])
	}
	log.Printf("total number of topics: %d", len(topics))

	// consumer.Topics() 方法：获取所有的topic的， 所有partition 的高水位
	// 什么是高水位？在 kafka 中个就是水位，用消息位移来表征。它是已提交消息和未提交消息的分界线
	// 什么是末端位移（Log End Offset）? 就是Broker中最后一个消息的位移，该消息可能未提交
	waterMarks := consumer.HighWaterMarks() // 为什么没有返回error, 说明没有网络通信
	for t, ps := range waterMarks {
		for p, hw := range ps {
			// TODO: 此处调用结果是空，未被执行
			log.Printf("tolic: %v, partition: %v, hw: %v ", t, p, hw)
		}
	}
	log.Printf("===========")

	// consumer.Partitions() 方法：获取topic的所有分区，按分区ID排序返回
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Fatal("Partitions err: ", err)
	}

	var wg sync.WaitGroup
	wg.Add(len(partitions))
	// 然后每个分区开一个 goroutine 来消费
	for _, partitionId := range partitions {
		go consumeByPartition(consumer, partitionId, &wg)
	}

	wg.Wait()
}

// partition consumer api
// AsyncClose initiates a shutdown of the PartitionConsumer. This method will return immediately, after which you
// should continue to service the 'Messages' and 'Errors' channels until they are empty. It is required to call this
// function, or Close before a consumer object passes out of scope, as it will otherwise leak memory. You must call
// this before calling Close on the underlying client.
// AsyncClose()
// Close() error
// Messages() <-chan *ConsumerMessage
// Errors() <-chan *ConsumerError
// HighWaterMarkOffset() int64
func consumeByPartition(consumer sarama.Consumer, partitionId int32, wg *sync.WaitGroup) {
	defer wg.Done()
	// consumer.ConsumePartition() 方法：引入分区消费者概念（PartitionConsumer），指定消费位点为 sarama.OffsetOldest
	partitionConsumer, err := consumer.ConsumePartition(conf.Topic, partitionId, sarama.OffsetOldest)
	if err != nil {
		log.Fatal("ConsumePartition err: ", err)
	}
	defer partitionConsumer.Close() // 注意关闭

	// TODO: 为什么所有 partition 的 hw 返回值都是 0?
	log.Printf("tolic: %v, partition: %v, hw: %v ", conf.Topic, partitionId, partitionConsumer.HighWaterMarkOffset())
	for message := range partitionConsumer.Messages() {
		log.Printf("[Consumer] partitionid: %d; offset:%d, value: %s\n", message.Partition, message.Offset, string(message.Value))
	}
}
