package async

import (
	"log"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"

	"kafka-go-example/conf"
)

/*
	本例展示最简单的 异步生产者 的使用（除异步生产者外 kafka 还有同步生产者）
	名词 async producer
*/

var count int64

// sync producer API
// AsyncClose()
// Close() error
// Input() chan<- *ProducerMessage
// Successes() <-chan *ProducerMessage
// Errors() <-chan *ProducerError
func Producer(topic string, limit int) {
	config := sarama.NewConfig()

	config.Producer.Return.Errors = false   // 从 error channel 只接收 error
	config.Producer.Return.Successes = true // 从 success channel 接收 message

	// 调用 NewAsyncProducer
	producer, err := sarama.NewAsyncProducer([]string{conf.HOST}, config)
	if err != nil {
		log.Fatal("NewSyncProducer err:", err)
	}
	// AsyncClose triggers a shutdown of the producer. The shutdown has completed
	// when both the Errors and Successes channels have been closed. When calling
	// AsyncClose, you *must* continue to read from those channels in order to
	// drain the results of any messages in flight.
	defer producer.AsyncClose()

	go func() {
		// [!important] 异步生产者发送后必须把返回值从 Errors 或者 Successes 中读出来 不然会阻塞 sarama 内部处理逻辑 导致只能发出去一条消息
		// 这里的实现，显然是有问题的
		chSuccess := producer.Successes()
		chError := producer.Errors()
		for chError != nil || chSuccess != nil {
			select {
			case s := <-chSuccess:
				if s != nil {
					log.Printf("[Producer] Success: key:%v msg:%+v \n", s.Key, s.Value)
				} else {
					chSuccess = nil
				}

			case e := <-chError:
				if e != nil {
					log.Printf("[Producer] Errors：err:%v msg:%+v \n", e.Msg, e.Err)
				} else {
					chError = nil
				}
			}
		}
		log.Printf("success channel && error channel done")
	}()

	// 异步发送
	for i := 0; i < limit; i++ {
		str := strconv.Itoa(int(time.Now().UnixNano()))
		// The partitioning key for this message. 主要是用来为 message 进行分区的
		msg := &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(str)}
		// 异步发送只是写入内存了就返回了，并没有真正发送出去
		// sarama 库中用的是一个 channel 来接收，后台 goroutine 异步从该 channel 中取出消息并真正发送
		producer.Input() <- msg
		atomic.AddInt64(&count, 1)
		if atomic.LoadInt64(&count)%1000 == 0 {
			log.Printf("已发送消息数:%v\n", count)
		}
	}
	log.Printf("发送完毕 总发送消息数:%v\n", limit)
}
