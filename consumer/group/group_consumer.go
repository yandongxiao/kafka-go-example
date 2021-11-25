package group

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/Shopify/sarama"

	"kafka-go-example/conf"
)

/*
	本例展示最简单的 消费者组 的使用（除消费者组外 kafka 还有独立消费者）

	加入消费者组的方式：创建消费者时提供相同的 groupID 即可自动加入 groupID 对应的消费者组。

	组中所有消费者**以分区为单位**，拆分被消费的topic
	例如: 该 topic 中有 10 个分区，该组中有两个消费者，那么每个消费者会消费 5 个分区。
	但是如果该 topic 只有 1 个分区那只能一个消费者能消费，另一个消费者一条消息都消费不到。
	因为是以分区为单位拆分的。

	名词: consumerGroup、partition、 claim 、session
*/

// 关键概念：
// 什么是 sarama.ConsumerGroup interface ? 它由 Consume、Errors、Close 三个方法组成。
//    Consume joins a cluster of consumers for a given list of topics and starts a blocking sarama.ConsumerGroupSession through the sarama.ConsumerGroupHandler.
//    ConsumerGroupHandler 负责消费消息，ConsumerGroupSession 负责什么？ 1. 负责调用 ConsumerGroupHandler 的各个方法；2. 一些持久化的内容，以及 consumer 关心的数据
// 什么是 sarama.ConsumerGroupClaim? 它是 ConsumerGroupHandler 的 ConsumeClaim 方法的参数，用于表征一个 partition
//
// 代码 https://github.com/Shopify/sarama/blob/master/consumer_group.go
//
// sarama 上面暴露的这些接口，对于使用者来说，最关心的是 sarama.ConsumerGroupHandler
//
// type ConsumerGroupHandler interface {
//	// Setup is run at the beginning of a new session, before ConsumeClaim.
//	Setup(ConsumerGroupSession) error
//
//	// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
//	// but before the offsets are committed for the very last time.
//	Cleanup(ConsumerGroupSession) error
//
//	// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
//	// Once the Messages() channel is closed, the Handler must finish its processing
//	// loop and exit. ConsumerGroupClaim 有点充当了 Offset Manager 的作用
//	ConsumeClaim(ConsumerGroupSession, ConsumerGroupClaim) error
// }
//

// 什么是 ConsumerGroupSession？
// The life-cycle of a session is represented by the following steps:
//
// 1. The consumers join the group (as explained in https://kafka.apache.org/documentation/#intro_consumers)
//    and is assigned their "fair share" of partitions, aka 'claims'.
// 2. Before processing starts, the handler's Setup() hook is called to notify the user
//    of the claims and allow any necessary preparation or alteration of state.
// 3. For each of the assigned claims the handler's ConsumeClaim() function is then called
//    in a separate goroutine which requires it to be thread-safe. Any state must be carefully protected
//    from concurrent reads/writes.
// 4. The session will persist until one of the ConsumeClaim() functions exits. This can be either when the
//    parent context is canceled or when a server-side rebalance cycle is initiated.
// 5. Once all the ConsumeClaim() loops have exited, the handler's Cleanup() hook is called
//    to allow the user to perform any final tasks before a rebalance.
// 6. Finally, marked offsets are committed one last time before claims are released.
//
// 站在 ConsumerGroupHandler 的角度，ConsumerGroupSession 充当了 OffsetManager 的作用
// ConsumerGroupSession represents a consumer group member(group中的一个消费者) session.
//
// type ConsumerGroupSession interface {
// 	// Claims returns information about the claimed partitions by topic.
// 	Claims() map[string][]int32 // key 是 topic, value 是 partition number
//
// 	// MemberID returns the cluster member ID.
// 	MemberID() string   // one of consumer id in consumer group
//
// 	// GenerationID returns the current generation ID.
// 	GenerationID() int32    // 应该也是kafka分配的ID，用于标识这个session
//
// 	// MarkOffset marks the provided offset, alongside a metadata string
// 	// that represents the state of the partition consumer at that point in time. The
// 	// metadata string can be used by another consumer to restore that state, so it
// 	// can resume consumption.
// 	//
// 	// To follow upstream conventions, you are expected to mark the offset of the
// 	// next message to read, not the last message read. Thus, when calling `MarkOffset`
// 	// you should typically add one to the offset of the last consumed message.
// 	//
// 	// Note: calling MarkOffset does not necessarily commit the offset to the backend
// 	// store immediately for efficiency reasons, and it may never be committed if
// 	// your application crashes. This means that you may end up processing the same
// 	// message twice, and your processing should ideally be idempotent.
// 	MarkOffset(topic string, partition int32, offset int64, metadata string)
//
// 	// Commit the offset to the backend
// 	//
// 	// Note: calling Commit performs a blocking synchronous operation.
// 	Commit()
//
// 	// ResetOffset resets to the provided offset, alongside a metadata string that
// 	// represents the state of the partition consumer at that point in time. Reset
// 	// acts as a counterpart to MarkOffset, the difference being that it allows to
// 	// reset an offset to an earlier or smaller value, where MarkOffset only
// 	// allows incrementing the offset. cf MarkOffset for more details.
// 	ResetOffset(topic string, partition int32, offset int64, metadata string)
//
// 	// MarkMessage marks a message as consumed.
// 	MarkMessage(msg *ConsumerMessage, metadata string)      // 后端调用 MarkOffset
//
// 	// Context returns the session context.
// 	Context() context.Context
// }
//
//

// ConsumerGroupClaim 充当了OffsetManager的作用
// type ConsumerGroupClaim interface {
// 	// Topic returns the consumed topic name.
// 	Topic() string      // 消费的消息
//
// 	// Partition returns the consumed partition.
// 	Partition() int32
//
// 	// InitialOffset returns the initial offset that was used as a starting point for this claim.
// 	InitialOffset() int64
//
// 	// HighWaterMarkOffset returns the high water mark offset of the partition,
// 	// i.e. the offset that will be used for the next message that will be produced.
// 	// You can use this to determine how far behind the processing is.
// 	HighWaterMarkOffset() int64
//
// 	// Messages returns the read channel for the messages that are returned by
// 	// the broker. The messages channel will be closed when a new rebalance cycle
// 	// is due. You must finish processing and mark offsets within
// 	// Config.Consumer.Group.Session.Timeout before the topic/partition is eventually
// 	// re-assigned to another group member.
// 	Messages() <-chan *ConsumerMessage
// }

//

// MyConsumerGroupHandler 实现 sarama.ConsumerGroupHandler 接口，作为自定义 ConsumerGroupHandler
type MyConsumerGroupHandler struct {
	mutex *sync.Mutex
	name  string
	count int64
}

// Setup 执行在 获得新 session 后 的第一步, 在 ConsumeClaim() 之前
func (*MyConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error { return nil }

// Cleanup 执行在 session 结束前, 当所有 ConsumeClaim goroutines 都退出时
func (*MyConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim 具体的消费逻辑
func (h *MyConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("[consumer] name:%s topic:%q partition:%d offset:%d\n", h.name, msg.Topic, msg.Partition, msg.Offset)
		// 标记消息已被消费 内部会更新 consumer offset
		// 每个消费者管理自己的位移
		sess.MarkMessage(msg, "")
		h.mutex.Lock()
		h.count++ // TODO: 这个并发不安全
		if h.count%1 == 0 {
			fmt.Printf("name:%s 消费数:%v\n", h.name, h.count)
		}
		h.mutex.Unlock()
	}
	return nil
}

func ConsumerGroup(topic, group, name string) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true // 后面并没有专有协程，从 ConsumerGroup 中读取 errors
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建 consumer group
	cg, err := sarama.NewConsumerGroup([]string{conf.HOST}, group, config)
	if err != nil {
		log.Fatal("NewConsumerGroup err: ", err)
	}
	defer cg.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		handler := &MyConsumerGroupHandler{name: name, mutex: &sync.Mutex{}}
		for {
			fmt.Println("running: ", name)
			/*
				应该在一个无限循环中不停地调用 Consume()
				因为每次 Rebalance 后需要再次执行 Consume() 来恢复连接
				Consume 开始发起 Join Group 请求 如果当前消费者加入后成为了 消费者组 leader，则还会进行 Rebalance 过程，从新分配
				组内每个消费组需要消费的 topic 和 partition，最后 Sync Group 后才开始消费
			*/
			err = cg.Consume(ctx, []string{topic}, handler) // 并没有显式地创建消费者，而是通过多次调用 cg.Consume 来隐式地创建消费者
			if err != nil {
				log.Println("Consume err: ", err)
			}
			// 如果 context 被 cancel 了，那么退出。
			if ctx.Err() != nil {
				return
			}
		}
	}()
	wg.Wait()
}
