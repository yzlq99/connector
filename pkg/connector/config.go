package connector

import cfg "gitlab.mvalley.com/datapack/cain/pkg/config"

// Configuration 专用于构建 connector 的 config
type Configuration struct {
	// 基础Kafka信息
	KafkaConfig cfg.KafkaConfiguration
	// 对应初版中的 TopicPrefix，由于需要支持一个 connector 监控多个库，
	// 故改为 connector server name，database name 当作参数传入
	ServerName string
	// default: 1
	NumberOfConsumers int
	// default: 2.1.1
	KafkaVersion string
	// default: range
	Assignor string
	// only read latest message in topics
	Oldest  bool
	Verbose bool
}
