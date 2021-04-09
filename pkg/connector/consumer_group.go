package connector

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
)

// ConsumerGroup ...
type ConsumerGroup struct {
	// 反序列化类型信息
	Typ     reflect.Type
	Brokers []string
	Topic   string
	// 实际消费消息方法
	ConsumerHandlers []ConsumerHandler
	NumOfConsumers   int

	Version  string
	Group    string
	Assignor string
	Oldest   bool
	Verbose  bool
}

// Consumer ...
type Consumer struct {
	Typ              reflect.Type
	ConsumerHandlers []ConsumerHandler

	ready chan bool
}

// ConsumerHandler ...
type ConsumerHandler interface {
	Create(after interface{}) error
	Update(before interface{}, after interface{}) error
	Delete(before interface{}) error
}

// NewConsumerGroup ...
func NewConsumerGroup(
	typ reflect.Type,
	brokers []string,
	topic string,
	numOfConsumers int,
	kafkaVersion string,
	group string,
	assignor string,
	oldest bool,
	verbose bool,
) ConsumerGroup {

	if len(brokers) <= 0 {
		panic("brokers can not be empty")
	}
	if len(topic) <= 0 {
		panic("topic can not be empty")
	}
	if numOfConsumers < 1 {
		numOfConsumers = 1
	}
	if kafkaVersion == "" {
		kafkaVersion = "2.1.1"
	}
	if len(group) <= 0 {
		group = topic
	}
	if len(assignor) <= 0 {
		assignor = "range"
	}

	return ConsumerGroup{
		Typ:              typ,
		Brokers:          brokers,
		Topic:            topic,
		ConsumerHandlers: make([]ConsumerHandler, 0),
		NumOfConsumers:   numOfConsumers,

		Version:  kafkaVersion,
		Group:    group,
		Assignor: assignor,
		Oldest:   oldest,
		Verbose:  verbose,
	}
}

// AddHandler ...
func (c *ConsumerGroup) AddHandler(handler ConsumerHandler) {
	c.ConsumerHandlers = append(c.ConsumerHandlers, handler)
}

// Message ...
type Message struct {
	After  *json.RawMessage `json:"after"`
	Before *json.RawMessage `json:"before"`
	Op     string           `json:"op"`
}

// Schema ...
type Schema struct {
	Payload Message `json:"payload"`
}

// Start ...
func (c *ConsumerGroup) Start() {
	log.Println("Starting a new Sarama consumer")

	if c.Verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	version, err := sarama.ParseKafkaVersion(c.Version)
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	/**
	 * Construct a new Sarama configuration.
	 * The Kafka cluster version has to be defined before the consumer/producer is initialized.
	 */
	config := sarama.NewConfig()
	config.Version = version

	switch c.Assignor {
	case "sticky":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	case "roundrobin":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	case "range":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	default:
		log.Panicf("Unrecognized consumer group partition assignor: %s", c.Assignor)
	}

	if c.Oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
	client, err := sarama.NewConsumerGroup(c.Brokers, c.Group, config)

	ctx, cancel := context.WithCancel(context.Background())
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	consumer := Consumer{
		Typ:              c.Typ,
		ConsumerHandlers: c.ConsumerHandlers,

		ready: make(chan bool),
	}

	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(ctx, []string{c.Topic}, &consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()
	<-consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer", " up and running!...")
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-sigterm:
		log.Println("terminating: via signal")
	}
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *Consumer) Setup(session sarama.ConsumerGroupSession) error {
	close(c.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *Consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		if message.Value == nil {
			continue
		}

		var she Schema
		err := json.Unmarshal(message.Value, &she)
		if err != nil {
			log.Printf("%+v", err)
		}
		op := she.Payload.Op
		var before interface{}
		var after interface{}

		if (op == "d" || op == "u") && she.Payload.Before != nil {
			before = reflect.New(c.Typ).Interface()
			Deserialize([]byte(*she.Payload.Before), before)

		}

		if (op == "c" || op == "u") && she.Payload.After != nil {
			after = reflect.New(c.Typ).Interface()
			Deserialize([]byte(*she.Payload.After), after)
		}

		if after == nil && before == nil {
			continue
		}

		// 是否执行软删除，有各自的 handler 自行控制
		for _, handler := range c.ConsumerHandlers {
			switch op {
			case "c":
				err = handler.Create(after)
				if err != nil {
					log.Printf("Create, %+v", err)
				}
				break
			case "u":
				err = handler.Update(before, after)
				if err != nil {
					log.Printf("Update, %+v", err)
				}

				break
			case "d":
				err = handler.Delete(before)
				if err != nil {
					log.Printf("Delete, %+v", err)
				}
				break
			}
		}
		session.MarkMessage(message, "")
	}

	return nil
}
