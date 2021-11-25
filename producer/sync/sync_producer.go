package sync

import (
	"log"
	"strconv"
	"time"

	"github.com/Shopify/sarama"

	"kafka-go-example/conf"
)

/*
	本例展示最简单的 同步生产者 的使用（除同步生产者外 kafka 还有异步生产者）
	名词 sync producer
*/

// sync producer api
// SendMessage(msg *ProducerMessage) (partition int32, offset int64, err error)
// SendMessages(msgs []*ProducerMessage) error
// Close() error
func Producer(topic string, limit int) {
	// 创建 sarama.Config 对象，调用该方法，填充 sarama.Config 对象的默认值
	config := sarama.NewConfig()

	// 同步生产者必须同时开启 Return.Successes 和 Return.Errors
	// 因为同步生产者在发送之后就必须返回状态，所以需要两个都返回
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true // 这个默认值就是 true 可以不用手动 赋值

	// 同步生产者和异步生产者逻辑是一致的，Success或者Errors都是通过channel返回的，
	// 只是同步生产者封装了一层，等channel返回之后才返回给调用者。方法是使用 sync.WaitGroup。
	// type syncProducer struct {
	// 	producer *asyncProducer
	// 	wg       sync.WaitGroup
	// }
	// asyncProducer 有以下两个字段:
	//  errors                    chan *ProducerError
	//  input, successes, retries chan *ProducerMessage
	//
	// 追踪该方法后，发现：newSyncProducerFromAsyncProducer 方法内部启动了两个 goroutine
	// 分别处理 Success Channel 和 Errors Channel
	producer, err := sarama.NewSyncProducer([]string{conf.HOST}, config)
	if err != nil {
		log.Fatal("NewSyncProducer err:", err)
	}
	defer producer.Close()

	for i := 0; i < limit; i++ {
		str := strconv.Itoa(int(time.Now().UnixNano()))
		msg := &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(str)}
		// 发送逻辑也是封装的异步发送逻辑，可以理解为将异步封装成了同步
		// 首先，sarama.ProducerMessage 有一个字段:  expectation    chan *ProducerError, 放置一个 channel 实例用于获取发送消息成功与否
		// 然后，将消息发送给 input channel
		// 最后，等待 expectation channel 的返回，返回后，ProducerMessage 也会填充一些信息，topic, partition 等
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Println("SendMessage err: ", err)
			return
		}
		log.Printf("[Producer] partitionid: %d; offset:%d, value: %s\n", partition, offset, str)
	}
}
