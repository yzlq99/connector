package connector

import (
	"fmt"
	"log"
	"reflect"
	"sync"
)

// Connector ...
type Connector struct {
	consumerGroups map[string]*ConsumerGroup
	config         *Configuration
}

// NewConnector ...
func NewConnector(config *Configuration) *Connector {
	if config == nil {
		panic("config can not be nil")
	}
	cgs := make(map[string]*ConsumerGroup)
	return &Connector{
		consumerGroups: cgs,
		config:         config,
	}
}

var connector *Connector

// InitConnectorWithGroupMap ...
func InitConnectorWithGroupMap(config *Configuration, groupMaps ...map[string]RegistrationHelper) {
	if config == nil {
		panic("config can not be nil")
	}
	connector = NewConnector(config)
	if len(groupMaps) <= 0 {
		log.Printf("WARN: groupMap is empty")
		return
	}
	for _, groupMap := range groupMaps {
		connector.RegisterByGroupMap(groupMap)
	}
}

// StartConnector ...
func StartConnector() {
	if connector == nil {
		panic("Please initialize connector with the method [InitConnectorWithGroupMap]")
	}
	connector.Start()
}

// Start ...
func (c *Connector) Start() {
	wait := sync.WaitGroup{}
	for _, cg := range c.consumerGroups {
		for i := 0; i < cg.NumOfConsumers; i++ {
			wait.Add(1)
			go func(cg ConsumerGroup) {
				cg.Start()
				wait.Done()
			}(*cg)
		}
	}
	wait.Wait()
	log.Print("connector closed")
}

// HandlerFunc ...
type HandlerFunc func(before, after interface{}) error

// Register ...
func (c *Connector) Register(groupName string, dbName string, tableName string, model interface{}, handlers []ConsumerHandler) {
	_, ok := c.consumerGroups[groupName]
	if ok {
		panic("group already exists")
	}
	if dbName == "" {
		panic("database name can not be blank")
	}
	if tableName == "" {
		log.Print("WARN: tableName is blank")
	}
	if model == nil {
		panic("model can not be nil")
	}
	if len(handlers) <= 0 {
		log.Printf("WARN: handlers is empty")
	}
	cg := NewConsumerGroup(
		reflect.TypeOf(model),
		c.config.KafkaConfig.Brokers,
		// topic命名规则为 debezium mysql connector 配置文件中配置的 serverName.databaseName.tableName。
		fmt.Sprintf("%s.%s.%s", c.config.ServerName, dbName, tableName),
		c.config.NumberOfConsumers,
		c.config.KafkaVersion,
		groupName,
		c.config.Assignor,
		c.config.Oldest,
		c.config.Verbose,
	)
	for _, h := range handlers {
		cg.AddHandler(h)
	}
	c.consumerGroups[groupName] = &cg
}

// RegistrationHelper ...
type RegistrationHelper struct {
	DBName    string
	TableName string
	Model     interface{}
	Handlers  []ConsumerHandler
}

// RegisterByGroupMap key: GroupName; value: RegistrationHelper
func (c *Connector) RegisterByGroupMap(groupMap map[string]RegistrationHelper) {
	if len(groupMap) <= 0 {
		log.Printf("WARN: groupMap is empty")
		return
	}
	for groupName, helper := range groupMap {
		c.Register(groupName, helper.DBName, helper.TableName, helper.Model, helper.Handlers)
	}
}

// AddConsumerGroup ...
func (c *Connector) AddConsumerGroup(groupName string, cg *ConsumerGroup) {
	_, ok := c.consumerGroups[groupName]
	if ok {
		panic("group already exists")
	}
	if cg == nil {
		log.Print("WARN: consumer group can not be nil")
	}
	c.consumerGroups[groupName] = cg
}
