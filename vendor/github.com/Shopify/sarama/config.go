package sarama

import (
	"compress/gzip"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net"
	"regexp"
	"time"

	"github.com/rcrowley/go-metrics"
	"golang.org/x/net/proxy"
)

const defaultClientID = "sarama"

var validID = regexp.MustCompile(`\A[A-Za-z0-9._-]+\z`)

// Config is used to pass multiple configuration options to Sarama's constructors.
type Config struct {
	// Admin is the namespace for ClusterAdmin properties used by the administrative Kafka client.
	Admin struct {
		Retry struct {
			// The total number of times to retry sending (retriable) admin requests (default 5).
			// Similar to the `retries` setting of the JVM AdminClientConfig.
			Max int
			// Backoff time between retries of a failed request (default 100ms)
			Backoff time.Duration
		}
		// The maximum duration the administrative Kafka client will wait for ClusterAdmin operations,
		// including topics, brokers, configurations and ACLs (defaults to 3 seconds).
		Timeout time.Duration
	}

	// Net is the namespace for network-level properties used by the Broker, and
	// shared by the Client/Producer/Consumer.
	Net struct {
		// How many outstanding requests a connection is allowed to have before
		// sending on it blocks (default 5).
		MaxOpenRequests int

		// All three of the below configurations are similar to the
		// `socket.timeout.ms` setting in JVM kafka. All of them default
		// to 30 seconds.
		DialTimeout  time.Duration // How long to wait for the initial connection.
		ReadTimeout  time.Duration // How long to wait for a response.
		WriteTimeout time.Duration // How long to wait for a transmit.

		TLS struct {
			// Whether or not to use TLS when connecting to the broker
			// (defaults to false).
			Enable bool
			// The TLS configuration to use for secure connections if
			// enabled (defaults to nil).
			Config *tls.Config
		}

		// SASL based authentication with broker. While there are multiple SASL authentication methods
		// the current implementation is limited to plaintext (SASL/PLAIN) authentication
		SASL struct {
			// Whether or not to use SASL authentication when connecting to the broker
			// (defaults to false).
			Enable bool
			// SASLMechanism is the name of the enabled SASL mechanism.
			// Possible values: OAUTHBEARER, PLAIN (defaults to PLAIN).
			Mechanism SASLMechanism
			// Version is the SASL Protocol Version to use
			// Kafka > 1.x should use V1, except on Azure EventHub which use V0
			Version int16
			// Whether or not to send the Kafka SASL handshake first if enabled
			// (defaults to true). You should only set this to false if you're using
			// a non-Kafka SASL proxy.
			Handshake bool
			// AuthIdentity is an (optional) authorization identity (authzid) to
			// use for SASL/PLAIN authentication (if different from User) when
			// an authenticated user is permitted to act as the presented
			// alternative user. See RFC4616 for details.
			AuthIdentity string
			// User is the authentication identity (authcid) to present for
			// SASL/PLAIN or SASL/SCRAM authentication
			User string
			// Password for SASL/PLAIN authentication
			Password string
			// authz id used for SASL/SCRAM authentication
			SCRAMAuthzID string
			// SCRAMClientGeneratorFunc is a generator of a user provided implementation of a SCRAM
			// client used to perform the SCRAM exchange with the server.
			SCRAMClientGeneratorFunc func() SCRAMClient
			// TokenProvider is a user-defined callback for generating
			// access tokens for SASL/OAUTHBEARER auth. See the
			// AccessTokenProvider interface docs for proper implementation
			// guidelines.
			TokenProvider AccessTokenProvider

			GSSAPI GSSAPIConfig
		}

		// KeepAlive specifies the keep-alive period for an active network connection (defaults to 0).
		// If zero or positive, keep-alives are enabled.
		// If negative, keep-alives are disabled.
		KeepAlive time.Duration

		// LocalAddr is the local address to use when dialing an
		// address. The address must be of a compatible type for the
		// network being dialed.
		// If nil, a local address is automatically chosen.
		LocalAddr net.Addr

		Proxy struct {
			// Whether or not to use proxy when connecting to the broker
			// (defaults to false).
			Enable bool
			// The proxy dialer to use enabled (defaults to nil).
			Dialer proxy.Dialer
		}
	}

	// Metadata is the namespace for metadata management properties used by the
	// Client, and shared by the Producer/Consumer.
	Metadata struct {
		Retry struct {
			// The total number of times to retry a metadata request when the
			// cluster is in the middle of a leader election (default 3).
			Max int
			// How long to wait for leader election to occur before retrying
			// (default 250ms). Similar to the JVM's `retry.backoff.ms`.
			Backoff time.Duration
			// Called to compute backoff time dynamically. Useful for implementing
			// more sophisticated backoff strategies. This takes precedence over
			// `Backoff` if set.
			BackoffFunc func(retries, maxRetries int) time.Duration
		}
		// How frequently to refresh the cluster metadata in the background.
		// Defaults to 10 minutes. Set to 0 to disable. Similar to
		// `topic.metadata.refresh.interval.ms` in the JVM version.
		RefreshFrequency time.Duration

		// Whether to maintain a full set of metadata for all topics, or just
		// the minimal set that has been necessary so far. The full set is simpler
		// and usually more convenient, but can take up a substantial amount of
		// memory if you have many topics and partitions. Defaults to true.
		Full bool

		// How long to wait for a successful metadata response.
		// Disabled by default which means a metadata request against an unreachable
		// cluster (all brokers are unreachable or unresponsive) can take up to
		// `Net.[Dial|Read]Timeout * BrokerCount * (Metadata.Retry.Max + 1) + Metadata.Retry.Backoff * Metadata.Retry.Max`
		// to fail.
		Timeout time.Duration
	}

	// Producer is the namespace for configuration related to producing messages,
	// used by the Producer.
	Producer struct {
		// The maximum permitted size of a message (defaults to 1000000). Should be
		// set equal to or smaller than the broker's `message.max.bytes`.
		MaxMessageBytes int

		// The level of acknowledgement reliability needed from the broker (defaults
		// to WaitForLocal). Equivalent to the `request.required.acks` setting of the
		// JVM producer.
		// 它 是 Producer 的一个参数，代表了Producer对“已提交”消息的定义(不是broker端)。
		// 如果设置成 all，则表明所有副本 Broker 都要接收到消息，该消息才算是“已提交”。这是最高等级的“已提交”定义。
		// 字段的具体解释如下：https://www.lixueduan.com/post/kafka/09-avoid-msg-lost/
		// 如果要严格保证消息不丢失，请设置为 all 或 -1；
		// 如果允许存在丢失，建议设置为 1；
		// 一般不建议设为 0，除非无所谓消息丢不丢失。
		RequiredAcks RequiredAcks

		// The maximum duration the broker will wait the receipt of the number of
		// RequiredAcks (defaults to 10 seconds). This is only relevant when
		// RequiredAcks is set to WaitForAll or a number > 1. Only supports
		// millisecond resolution, nanoseconds will be truncated. Equivalent to
		// the JVM producer's `request.timeout.ms` setting.
		Timeout time.Duration
		// The type of compression to use on messages (defaults to no compression).
		// Similar to `compression.codec` setting of the JVM producer.
		Compression CompressionCodec
		// The level of compression to use on messages. The meaning depends
		// on the actual compression type used and defaults to default compression
		// level for the codec.
		CompressionLevel int
		// Generates partitioners for choosing the partition to send messages to
		// (defaults to hashing the message key). Similar to the `partitioner.class`
		// setting for the JVM producer.
		// 默认值是 hash the message key
		Partitioner PartitionerConstructor
		// If enabled, the producer will ensure that exactly one copy of each message is
		// written.
		// 什么是幂等性：在 Broker 端多保存一些字段。当 Producer 发送了具有相同字段值的消息后，Broker 能够自动知晓这些消息已经重复了，于是可以在后台默默地把它们“丢弃”掉。
		// 参见：https://www.lixueduan.com/post/kafka/10-exactly-once-impl/
		// 实现原理： 初始化时像向 Broker 申请一个 ProducerID；为每条消息绑定一个 SequenceNumber
		// Broker 端记录成功提交的 SequenceNumber，Producer 重复发送了， Broker 端会将其过滤掉。
		// 首先，它只能保证单分区上的幂等性。因为 SequenceNumber 是以 Topic + Partition 为单位单调递增的，如果一条消息被发送到了多个分区必然会分配到不同的 SequenceNumber ,导致重复问题。
		// 其次，它只能实现单会话上的幂等性。重启 Producer 后会分配一个新的 ProducerID，相当于之前保存的 SequenceNumber 就丢失了。
		Idempotent bool

		// Return specifies what channels will be populated. If they are set to true,
		// you must read from the respective channels to prevent deadlock. If,
		// however, this config is used to create a `SyncProducer`, both must be set
		// to true and you shall not read from the channels since the producer does
		// this internally.
		Return struct {
			// If enabled, successfully delivered messages will be returned on the
			// Successes channel (default disabled).
			Successes bool

			// If enabled, messages that failed to deliver will be returned on the
			// Errors channel, including error (default enabled).
			Errors bool
		}

		// The following config options control how often messages are batched up and
		// sent to the broker. By default, messages are sent as fast as possible, and
		// all messages received while the current batch is in-flight are placed
		// into the subsequent batch.
		Flush struct {
			// The best-effort number of bytes needed to trigger a flush. Use the
			// global sarama.MaxRequestSize to set a hard upper limit.
			Bytes int
			// The best-effort number of messages needed to trigger a flush. Use
			// `MaxMessages` to set a hard upper limit.
			Messages int
			// The best-effort frequency of flushes. Equivalent to
			// `queue.buffering.max.ms` setting of JVM producer.
			Frequency time.Duration
			// The maximum number of messages the producer will send in a single
			// broker request. Defaults to 0 for unlimited. Similar to
			// `queue.buffering.max.messages` in the JVM producer.
			MaxMessages int
		}

		// 设置 retries 为一个较大的值。这里的 retries 同样是 Producer 的参数，对应前面提到的 Producer 自动重试。
		// 当出现网络的瞬时抖动时，消息发送可能会失败，此时配置了 retries > 0 的 Producer 能够自动重试消息发送，避免消息丢失。
		Retry struct {
			// The total number of times to retry sending a message (default 3).
			// Similar to the `message.send.max.retries` setting of the JVM producer.
			Max int
			// How long to wait for the cluster to settle between retries
			// (default 100ms). Similar to the `retry.backoff.ms` setting of the
			// JVM producer.
			Backoff time.Duration
			// Called to compute backoff time dynamically. Useful for implementing
			// more sophisticated backoff strategies. This takes precedence over
			// `Backoff` if set.
			BackoffFunc func(retries, maxRetries int) time.Duration
		}

		// Interceptors to be called when the producer dispatcher reads the
		// message for the first time. Interceptors allows to intercept and
		// possible mutate the message before they are published to Kafka
		// cluster. *ProducerMessage modified by the first interceptor's
		// OnSend() is passed to the second interceptor OnSend(), and so on in
		// the interceptor chain.
		Interceptors []ProducerInterceptor
	}

	// Consumer is the namespace for configuration related to consuming messages,
	// used by the Consumer.
	// Consumer 没有正确消费消息，就把位移提交了，导致 Kafka 认为该消息已经被消费了，从而导致消息丢失。
	// 确定消费完成后才提交消息，如果是多线程异步处理消费消息，Consumer 程序不要开启自动提交位移，而是要应用程序手动提交位移。
	// 你可以开启自动提交位移，但是 MarkOffset 需要手动调用
	Consumer struct {

		// Group is the namespace for configuring consumer group.
		Group struct {
			Session struct {
				// The timeout used to detect consumer failures when using Kafka's group management facility.
				// The consumer sends periodic heartbeats to indicate its liveness to the broker.
				// If no heartbeats are received by the broker before the expiration of this session timeout,
				// then the broker will remove this consumer from the group and initiate a rebalance.
				// Note that the value must be in the allowable range as configured in the broker configuration
				// by `group.min.session.timeout.ms` and `group.max.session.timeout.ms` (default 10s)
				// 该参数的默认值是 10 秒，即如果 Coordinator 在 10 秒之内没有收到 Group 下某 Consumer 实例的心跳，它就会认为这个 Consumer 实例已经挂了。
				Timeout time.Duration
			}
			Heartbeat struct {
				// The expected time between heartbeats to the consumer coordinator when using Kafka's group
				// management facilities. Heartbeats are used to ensure that the consumer's session stays active and
				// to facilitate rebalancing when new consumers join or leave the group.
				// The value must be set lower than Consumer.Group.Session.Timeout, but typically should be set no
				// higher than 1/3 of that value.
				// It can be adjusted even lower to control the expected time for normal rebalances (default 3s)
				// Consumer 还提供了一个允许你控制发送心跳请求频率的参数，就是 heartbeat.interval.ms。这个值设置得越小，Consumer 实例发送心跳请求的频率就越高。
				// 目前 Coordinator 通知各个 Consumer 实例开启 Rebalance 的方法，就是将 REBALANCE_NEEDED 标志封装进心跳请求的响应体中。
				Interval time.Duration
			}

			// 组成员数发生变更。比如有新的 Consumer 实例加入组或者离开组，亦或是有 Consumer 实例崩溃被“踢出”组。
			// 订阅主题数发生变更。Consumer Group 可以使用正则表达式的方式订阅主题，比如 consumer.subscribe(Pattern.compile(“t.*c”)) 就表明该 Group 订阅所有以字母 t 开头、字母 c 结尾的主题。在 Consumer Group 的运行过程中，你新创建了一个满足这样条件的主题，那么该 Group 就会发生 Rebalance。
			// 订阅主题的分区数发生变更。Kafka 当前只能允许增加一个主题的分区数。当分区数增加时，就会触发订阅该主题的所有 Group 开启 Rebalance。
			//
			// Consumer 实例可能会被 Coordinator 错误地认为“已停止”从而被“踢出”Group。
			// Coordinator 即协调者，它专门为 Consumer Group 服务，负责为 Group 执行 Rebalance 以及提供位移管理和组成员管理等。
			// 目前 Rebalance 的设计是所有 Consumer 实例共同参与，全部重新分配所有分区。需要达成共识！！！
			// 
			// 关于 max.poll.interval.ms(JAVA中的参数) 的描述：Consumer 端还有一个参数，用于控制 Consumer 实际消费能力对 Rebalance 的影响，
			// 即 max.poll.interval.ms 参数。它限定了 Consumer 端应用程序两次调用 poll 方法的最大时间间隔。它的默认值是 5 分钟，表示
			// 你的 Consumer 程序如果在 5 分钟之内无法消费完 poll 方法返回的消息，那么 Consumer 会主动发起“离开组”的请求，Coordinator 也会开启新一轮 Rebalance。
			// TODO: sarama 的 consumer 应该没有主动发起离开组的请求，所以可以忽略该参数
			Rebalance struct {
				// Strategy for allocating topic partitions to members (default BalanceStrategyRange)
				Strategy BalanceStrategy
				// The maximum allowed time for each worker to join the group once a rebalance has begun.
				// This is basically a limit on the amount of time needed for all tasks to flush any pending
				// data and commit offsets. If the timeout is exceeded, then the worker will be removed from
				// the group, which will cause offset commit failures (default 60s).
				Timeout time.Duration

				Retry struct {
					// When a new consumer joins a consumer group the set of consumers attempt to "rebalance"
					// the load to assign partitions to each consumer. If the set of consumers changes while
					// this assignment is taking place the rebalance will fail and retry. This setting controls
					// the maximum number of attempts before giving up (default 4).
					Max int
					// Backoff time between retries during rebalance (default 2s)
					Backoff time.Duration
				}
			}
			Member struct {
				// Custom metadata to include when joining the group. The user data for all joined members
				// can be retrieved by sending a DescribeGroupRequest to the broker that is the
				// coordinator for the group.
				UserData []byte
			}
		}

		Retry struct {
			// How long to wait after a failing to read from a partition before
			// trying again (default 2s).
			Backoff time.Duration
			// Called to compute backoff time dynamically. Useful for implementing
			// more sophisticated backoff strategies. This takes precedence over
			// `Backoff` if set.
			BackoffFunc func(retries int) time.Duration
		}

		// Fetch is the namespace for controlling how many bytes are retrieved by any
		// given request.
		Fetch struct {
			// The minimum number of message bytes to fetch in a request - the broker
			// will wait until at least this many are available. The default is 1,
			// as 0 causes the consumer to spin when no messages are available.
			// Equivalent to the JVM's `fetch.min.bytes`.
			Min int32
			// The default number of message bytes to fetch from the broker in each
			// request (default 1MB). This should be larger than the majority of
			// your messages, or else the consumer will spend a lot of time
			// negotiating sizes and not actually consuming. Similar to the JVM's
			// `fetch.message.max.bytes`.
			Default int32
			// The maximum number of message bytes to fetch from the broker in a
			// single request. Messages larger than this will return
			// ErrMessageTooLarge and will not be consumable, so you must be sure
			// this is at least as large as your largest message. Defaults to 0
			// (no limit). Similar to the JVM's `fetch.message.max.bytes`. The
			// global `sarama.MaxResponseSize` still applies.
			Max int32
		}
		// The maximum amount of time the broker will wait for Consumer.Fetch.Min
		// bytes to become available before it returns fewer than that anyways. The
		// default is 250ms, since 0 causes the consumer to spin when no events are
		// available. 100-500ms is a reasonable range for most cases. Kafka only
		// supports precision up to milliseconds; nanoseconds will be truncated.
		// Equivalent to the JVM's `fetch.wait.max.ms`.
		MaxWaitTime time.Duration

		// The maximum amount of time the consumer expects a message takes to
		// process for the user. If writing to the Messages channel takes longer
		// than this, that partition will stop fetching more messages until it
		// can proceed again.
		// Note that, since the Messages channel is buffered, the actual grace time is
		// (MaxProcessingTime * ChannelBufferSize). Defaults to 100ms.
		// If a message is not written to the Messages channel between two ticks
		// of the expiryTicker then a timeout is detected.
		// Using a ticker instead of a timer to detect timeouts should typically
		// result in many fewer calls to Timer functions which may result in a
		// significant performance improvement if many messages are being sent
		// and timeouts are infrequent.
		// The disadvantage of using a ticker instead of a timer is that
		// timeouts will be less accurate. That is, the effective timeout could
		// be between `MaxProcessingTime` and `2 * MaxProcessingTime`. For
		// example, if `MaxProcessingTime` is 100ms then a delay of 180ms
		// between two messages being sent may not be recognized as a timeout.
		MaxProcessingTime time.Duration

		// Return specifies what channels will be populated. If they are set to true,
		// you must read from them to prevent deadlock.
		Return struct {
			// If enabled, any errors that occurred while consuming are returned on
			// the Errors channel (default disabled).
			Errors bool
		}

		// Offsets specifies configuration for how and when to commit consumed
		// offsets. This currently requires the manual use of an OffsetManager
		// but will eventually be automated.
		Offsets struct {
			// Deprecated: CommitInterval exists for historical compatibility
			// and should not be used. Please use Consumer.Offsets.AutoCommit
			CommitInterval time.Duration

			// AutoCommit specifies configuration for commit messages automatically.
			AutoCommit struct {
				// Whether or not to auto-commit updated offsets back to the broker.
				// (default enabled).
				Enable bool

				// How frequently to commit updated offsets. Ineffective unless
				// auto-commit is enabled (default 1s)
				Interval time.Duration
			}

			// The initial offset to use if no offset was previously committed.
			// Should be OffsetNewest or OffsetOldest. Defaults to OffsetNewest.
			Initial int64

			// The retention duration for committed offsets. If zero, disabled
			// (in which case the `offsets.retention.minutes` option on the
			// broker will be used).  Kafka only supports precision up to
			// milliseconds; nanoseconds will be truncated. Requires Kafka
			// broker version 0.9.0 or later.
			// (default is 0: disabled).
			Retention time.Duration

			Retry struct {
				// The total number of times to retry failing commit
				// requests during OffsetManager shutdown (default 3).
				Max int
			}
		}

		// IsolationLevel support 2 mode:
		// 	- use `ReadUncommitted` (default) to consume and return all messages in message channel
		//	- use `ReadCommitted` to hide messages that are part of an aborted transaction
		IsolationLevel IsolationLevel

		// Interceptors to be called just before the record is sent to the
		// messages channel. Interceptors allows to intercept and possible
		// mutate the message before they are returned to the client.
		// *ConsumerMessage modified by the first interceptor's OnConsume() is
		// passed to the second interceptor OnConsume(), and so on in the
		// interceptor chain.
		Interceptors []ConsumerInterceptor
	}

	// A user-provided string sent with every request to the brokers for logging,
	// debugging, and auditing purposes. Defaults to "sarama", but you should
	// probably set it to something specific to your application.
	ClientID string
	// A rack identifier for this client. This can be any string value which
	// indicates where this client is physically located.
	// It corresponds with the broker config 'broker.rack'
	RackID string
	// The number of events to buffer in internal and external channels. This
	// permits the producer and consumer to continue processing some messages
	// in the background while user code is working, greatly improving throughput.
	// Defaults to 256.
	ChannelBufferSize int
	// The version of Kafka that Sarama will assume it is running against.
	// Defaults to the oldest supported stable version. Since Kafka provides
	// backwards-compatibility, setting it to a version older than you have
	// will not break anything, although it may prevent you from using the
	// latest features. Setting it to a version greater than you are actually
	// running may lead to random breakage.
	Version KafkaVersion
	// The registry to define metrics into.
	// Defaults to a local registry.
	// If you want to disable metrics gathering, set "metrics.UseNilMetrics" to "true"
	// prior to starting Sarama.
	// See Examples on how to use the metrics registry
	MetricRegistry metrics.Registry
}

// NewConfig returns a new configuration instance with sane defaults.
func NewConfig() *Config {
	c := &Config{}

	c.Admin.Retry.Max = 5
	c.Admin.Retry.Backoff = 100 * time.Millisecond
	c.Admin.Timeout = 3 * time.Second

	c.Net.MaxOpenRequests = 5
	c.Net.DialTimeout = 30 * time.Second
	c.Net.ReadTimeout = 30 * time.Second
	c.Net.WriteTimeout = 30 * time.Second
	c.Net.SASL.Handshake = true
	c.Net.SASL.Version = SASLHandshakeV0

	c.Metadata.Retry.Max = 3
	c.Metadata.Retry.Backoff = 250 * time.Millisecond
	c.Metadata.RefreshFrequency = 10 * time.Minute
	c.Metadata.Full = true

	c.Producer.MaxMessageBytes = 1000000
	c.Producer.RequiredAcks = WaitForLocal
	c.Producer.Timeout = 10 * time.Second
	c.Producer.Partitioner = NewHashPartitioner
	c.Producer.Retry.Max = 3
	c.Producer.Retry.Backoff = 100 * time.Millisecond
	c.Producer.Return.Errors = true
	c.Producer.CompressionLevel = CompressionLevelDefault

	c.Consumer.Fetch.Min = 1
	c.Consumer.Fetch.Default = 1024 * 1024
	c.Consumer.Retry.Backoff = 2 * time.Second
	c.Consumer.MaxWaitTime = 250 * time.Millisecond
	c.Consumer.MaxProcessingTime = 100 * time.Millisecond
	c.Consumer.Return.Errors = false
	c.Consumer.Offsets.AutoCommit.Enable = true
	c.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	c.Consumer.Offsets.Initial = OffsetNewest
	c.Consumer.Offsets.Retry.Max = 3

	c.Consumer.Group.Session.Timeout = 10 * time.Second
	c.Consumer.Group.Heartbeat.Interval = 3 * time.Second
	c.Consumer.Group.Rebalance.Strategy = BalanceStrategyRange
	c.Consumer.Group.Rebalance.Timeout = 60 * time.Second
	c.Consumer.Group.Rebalance.Retry.Max = 4
	c.Consumer.Group.Rebalance.Retry.Backoff = 2 * time.Second

	c.ClientID = defaultClientID
	c.ChannelBufferSize = 256
	c.Version = DefaultVersion
	c.MetricRegistry = metrics.NewRegistry()

	return c
}

// Validate checks a Config instance. It will return a
// ConfigurationError if the specified values don't make sense.
func (c *Config) Validate() error {
	// some configuration values should be warned on but not fail completely, do those first
	if !c.Net.TLS.Enable && c.Net.TLS.Config != nil {
		Logger.Println("Net.TLS is disabled but a non-nil configuration was provided.")
	}
	if !c.Net.SASL.Enable {
		if c.Net.SASL.User != "" {
			Logger.Println("Net.SASL is disabled but a non-empty username was provided.")
		}
		if c.Net.SASL.Password != "" {
			Logger.Println("Net.SASL is disabled but a non-empty password was provided.")
		}
	}
	if c.Producer.RequiredAcks > 1 {
		Logger.Println("Producer.RequiredAcks > 1 is deprecated and will raise an exception with kafka >= 0.8.2.0.")
	}
	if c.Producer.MaxMessageBytes >= int(MaxRequestSize) {
		Logger.Println("Producer.MaxMessageBytes must be smaller than MaxRequestSize; it will be ignored.")
	}
	if c.Producer.Flush.Bytes >= int(MaxRequestSize) {
		Logger.Println("Producer.Flush.Bytes must be smaller than MaxRequestSize; it will be ignored.")
	}
	if (c.Producer.Flush.Bytes > 0 || c.Producer.Flush.Messages > 0) && c.Producer.Flush.Frequency == 0 {
		Logger.Println("Producer.Flush: Bytes or Messages are set, but Frequency is not; messages may not get flushed.")
	}
	if c.Producer.Timeout%time.Millisecond != 0 {
		Logger.Println("Producer.Timeout only supports millisecond resolution; nanoseconds will be truncated.")
	}
	if c.Consumer.MaxWaitTime < 100*time.Millisecond {
		Logger.Println("Consumer.MaxWaitTime is very low, which can cause high CPU and network usage. See documentation for details.")
	}
	if c.Consumer.MaxWaitTime%time.Millisecond != 0 {
		Logger.Println("Consumer.MaxWaitTime only supports millisecond precision; nanoseconds will be truncated.")
	}
	if c.Consumer.Offsets.Retention%time.Millisecond != 0 {
		Logger.Println("Consumer.Offsets.Retention only supports millisecond precision; nanoseconds will be truncated.")
	}
	if c.Consumer.Group.Session.Timeout%time.Millisecond != 0 {
		Logger.Println("Consumer.Group.Session.Timeout only supports millisecond precision; nanoseconds will be truncated.")
	}
	if c.Consumer.Group.Heartbeat.Interval%time.Millisecond != 0 {
		Logger.Println("Consumer.Group.Heartbeat.Interval only supports millisecond precision; nanoseconds will be truncated.")
	}
	if c.Consumer.Group.Rebalance.Timeout%time.Millisecond != 0 {
		Logger.Println("Consumer.Group.Rebalance.Timeout only supports millisecond precision; nanoseconds will be truncated.")
	}
	if c.ClientID == defaultClientID {
		Logger.Println("ClientID is the default of 'sarama', you should consider setting it to something application-specific.")
	}

	// validate Net values
	switch {
	case c.Net.MaxOpenRequests <= 0:
		return ConfigurationError("Net.MaxOpenRequests must be > 0")
	case c.Net.DialTimeout <= 0:
		return ConfigurationError("Net.DialTimeout must be > 0")
	case c.Net.ReadTimeout <= 0:
		return ConfigurationError("Net.ReadTimeout must be > 0")
	case c.Net.WriteTimeout <= 0:
		return ConfigurationError("Net.WriteTimeout must be > 0")
	case c.Net.SASL.Enable:
		if c.Net.SASL.Mechanism == "" {
			c.Net.SASL.Mechanism = SASLTypePlaintext
		}

		switch c.Net.SASL.Mechanism {
		case SASLTypePlaintext:
			if c.Net.SASL.User == "" {
				return ConfigurationError("Net.SASL.User must not be empty when SASL is enabled")
			}
			if c.Net.SASL.Password == "" {
				return ConfigurationError("Net.SASL.Password must not be empty when SASL is enabled")
			}
		case SASLTypeOAuth:
			if c.Net.SASL.TokenProvider == nil {
				return ConfigurationError("An AccessTokenProvider instance must be provided to Net.SASL.TokenProvider")
			}
		case SASLTypeSCRAMSHA256, SASLTypeSCRAMSHA512:
			if c.Net.SASL.User == "" {
				return ConfigurationError("Net.SASL.User must not be empty when SASL is enabled")
			}
			if c.Net.SASL.Password == "" {
				return ConfigurationError("Net.SASL.Password must not be empty when SASL is enabled")
			}
			if c.Net.SASL.SCRAMClientGeneratorFunc == nil {
				return ConfigurationError("A SCRAMClientGeneratorFunc function must be provided to Net.SASL.SCRAMClientGeneratorFunc")
			}
		case SASLTypeGSSAPI:
			if c.Net.SASL.GSSAPI.ServiceName == "" {
				return ConfigurationError("Net.SASL.GSSAPI.ServiceName must not be empty when GSS-API mechanism is used")
			}

			if c.Net.SASL.GSSAPI.AuthType == KRB5_USER_AUTH {
				if c.Net.SASL.GSSAPI.Password == "" {
					return ConfigurationError("Net.SASL.GSSAPI.Password must not be empty when GSS-API " +
						"mechanism is used and Net.SASL.GSSAPI.AuthType = KRB5_USER_AUTH")
				}
			} else if c.Net.SASL.GSSAPI.AuthType == KRB5_KEYTAB_AUTH {
				if c.Net.SASL.GSSAPI.KeyTabPath == "" {
					return ConfigurationError("Net.SASL.GSSAPI.KeyTabPath must not be empty when GSS-API mechanism is used" +
						" and  Net.SASL.GSSAPI.AuthType = KRB5_KEYTAB_AUTH")
				}
			} else {
				return ConfigurationError("Net.SASL.GSSAPI.AuthType is invalid. Possible values are KRB5_USER_AUTH and KRB5_KEYTAB_AUTH")
			}
			if c.Net.SASL.GSSAPI.KerberosConfigPath == "" {
				return ConfigurationError("Net.SASL.GSSAPI.KerberosConfigPath must not be empty when GSS-API mechanism is used")
			}
			if c.Net.SASL.GSSAPI.Username == "" {
				return ConfigurationError("Net.SASL.GSSAPI.Username must not be empty when GSS-API mechanism is used")
			}
			if c.Net.SASL.GSSAPI.Realm == "" {
				return ConfigurationError("Net.SASL.GSSAPI.Realm must not be empty when GSS-API mechanism is used")
			}
		default:
			msg := fmt.Sprintf("The SASL mechanism configuration is invalid. Possible values are `%s`, `%s`, `%s`, `%s` and `%s`",
				SASLTypeOAuth, SASLTypePlaintext, SASLTypeSCRAMSHA256, SASLTypeSCRAMSHA512, SASLTypeGSSAPI)
			return ConfigurationError(msg)
		}
	}

	// validate the Admin values
	switch {
	case c.Admin.Timeout <= 0:
		return ConfigurationError("Admin.Timeout must be > 0")
	}

	// validate the Metadata values
	switch {
	case c.Metadata.Retry.Max < 0:
		return ConfigurationError("Metadata.Retry.Max must be >= 0")
	case c.Metadata.Retry.Backoff < 0:
		return ConfigurationError("Metadata.Retry.Backoff must be >= 0")
	case c.Metadata.RefreshFrequency < 0:
		return ConfigurationError("Metadata.RefreshFrequency must be >= 0")
	}

	// validate the Producer values
	switch {
	case c.Producer.MaxMessageBytes <= 0:
		return ConfigurationError("Producer.MaxMessageBytes must be > 0")
	case c.Producer.RequiredAcks < -1:
		return ConfigurationError("Producer.RequiredAcks must be >= -1")
	case c.Producer.Timeout <= 0:
		return ConfigurationError("Producer.Timeout must be > 0")
	case c.Producer.Partitioner == nil:
		return ConfigurationError("Producer.Partitioner must not be nil")
	case c.Producer.Flush.Bytes < 0:
		return ConfigurationError("Producer.Flush.Bytes must be >= 0")
	case c.Producer.Flush.Messages < 0:
		return ConfigurationError("Producer.Flush.Messages must be >= 0")
	case c.Producer.Flush.Frequency < 0:
		return ConfigurationError("Producer.Flush.Frequency must be >= 0")
	case c.Producer.Flush.MaxMessages < 0:
		return ConfigurationError("Producer.Flush.MaxMessages must be >= 0")
	case c.Producer.Flush.MaxMessages > 0 && c.Producer.Flush.MaxMessages < c.Producer.Flush.Messages:
		return ConfigurationError("Producer.Flush.MaxMessages must be >= Producer.Flush.Messages when set")
	case c.Producer.Retry.Max < 0:
		return ConfigurationError("Producer.Retry.Max must be >= 0")
	case c.Producer.Retry.Backoff < 0:
		return ConfigurationError("Producer.Retry.Backoff must be >= 0")
	}

	if c.Producer.Compression == CompressionLZ4 && !c.Version.IsAtLeast(V0_10_0_0) {
		return ConfigurationError("lz4 compression requires Version >= V0_10_0_0")
	}

	if c.Producer.Compression == CompressionGZIP {
		if c.Producer.CompressionLevel != CompressionLevelDefault {
			if _, err := gzip.NewWriterLevel(ioutil.Discard, c.Producer.CompressionLevel); err != nil {
				return ConfigurationError(fmt.Sprintf("gzip compression does not work with level %d: %v", c.Producer.CompressionLevel, err))
			}
		}
	}

	if c.Producer.Compression == CompressionZSTD && !c.Version.IsAtLeast(V2_1_0_0) {
		return ConfigurationError("zstd compression requires Version >= V2_1_0_0")
	}

	if c.Producer.Idempotent {
		if !c.Version.IsAtLeast(V0_11_0_0) {
			return ConfigurationError("Idempotent producer requires Version >= V0_11_0_0")
		}
		if c.Producer.Retry.Max == 0 {
			return ConfigurationError("Idempotent producer requires Producer.Retry.Max >= 1")
		}
		if c.Producer.RequiredAcks != WaitForAll {
			return ConfigurationError("Idempotent producer requires Producer.RequiredAcks to be WaitForAll")
		}
		if c.Net.MaxOpenRequests > 1 {
			return ConfigurationError("Idempotent producer requires Net.MaxOpenRequests to be 1")
		}
	}

	// validate the Consumer values
	switch {
	case c.Consumer.Fetch.Min <= 0:
		return ConfigurationError("Consumer.Fetch.Min must be > 0")
	case c.Consumer.Fetch.Default <= 0:
		return ConfigurationError("Consumer.Fetch.Default must be > 0")
	case c.Consumer.Fetch.Max < 0:
		return ConfigurationError("Consumer.Fetch.Max must be >= 0")
	case c.Consumer.MaxWaitTime < 1*time.Millisecond:
		return ConfigurationError("Consumer.MaxWaitTime must be >= 1ms")
	case c.Consumer.MaxProcessingTime <= 0:
		return ConfigurationError("Consumer.MaxProcessingTime must be > 0")
	case c.Consumer.Retry.Backoff < 0:
		return ConfigurationError("Consumer.Retry.Backoff must be >= 0")
	case c.Consumer.Offsets.AutoCommit.Interval <= 0:
		return ConfigurationError("Consumer.Offsets.AutoCommit.Interval must be > 0")
	case c.Consumer.Offsets.Initial != OffsetOldest && c.Consumer.Offsets.Initial != OffsetNewest:
		return ConfigurationError("Consumer.Offsets.Initial must be OffsetOldest or OffsetNewest")
	case c.Consumer.Offsets.Retry.Max < 0:
		return ConfigurationError("Consumer.Offsets.Retry.Max must be >= 0")
	case c.Consumer.IsolationLevel != ReadUncommitted && c.Consumer.IsolationLevel != ReadCommitted:
		return ConfigurationError("Consumer.IsolationLevel must be ReadUncommitted or ReadCommitted")
	}

	if c.Consumer.Offsets.CommitInterval != 0 {
		Logger.Println("Deprecation warning: Consumer.Offsets.CommitInterval exists for historical compatibility" +
			" and should not be used. Please use Consumer.Offsets.AutoCommit, the current value will be ignored")
	}

	// validate IsolationLevel
	if c.Consumer.IsolationLevel == ReadCommitted && !c.Version.IsAtLeast(V0_11_0_0) {
		return ConfigurationError("ReadCommitted requires Version >= V0_11_0_0")
	}

	// validate the Consumer Group values
	switch {
	case c.Consumer.Group.Session.Timeout <= 2*time.Millisecond:
		return ConfigurationError("Consumer.Group.Session.Timeout must be >= 2ms")
	case c.Consumer.Group.Heartbeat.Interval < 1*time.Millisecond:
		return ConfigurationError("Consumer.Group.Heartbeat.Interval must be >= 1ms")
	case c.Consumer.Group.Heartbeat.Interval >= c.Consumer.Group.Session.Timeout:
		return ConfigurationError("Consumer.Group.Heartbeat.Interval must be < Consumer.Group.Session.Timeout")
	case c.Consumer.Group.Rebalance.Strategy == nil:
		return ConfigurationError("Consumer.Group.Rebalance.Strategy must not be empty")
	case c.Consumer.Group.Rebalance.Timeout <= time.Millisecond:
		return ConfigurationError("Consumer.Group.Rebalance.Timeout must be >= 1ms")
	case c.Consumer.Group.Rebalance.Retry.Max < 0:
		return ConfigurationError("Consumer.Group.Rebalance.Retry.Max must be >= 0")
	case c.Consumer.Group.Rebalance.Retry.Backoff < 0:
		return ConfigurationError("Consumer.Group.Rebalance.Retry.Backoff must be >= 0")
	}

	// validate misc shared values
	switch {
	case c.ChannelBufferSize < 0:
		return ConfigurationError("ChannelBufferSize must be >= 0")
	case !validID.MatchString(c.ClientID):
		return ConfigurationError("ClientID is invalid")
	}

	return nil
}

func (c *Config) getDialer() proxy.Dialer {
	if c.Net.Proxy.Enable {
		Logger.Printf("using proxy %s", c.Net.Proxy.Dialer)
		return c.Net.Proxy.Dialer
	} else {
		return &net.Dialer{
			Timeout:   c.Net.DialTimeout,
			KeepAlive: c.Net.KeepAlive,
			LocalAddr: c.Net.LocalAddr,
		}
	}
}
