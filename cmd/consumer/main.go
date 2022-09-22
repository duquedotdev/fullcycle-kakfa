package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "kafka_go_kafka_1:9092",
		"client.id":         "go-consumer",
		"group.id":          "go-consumer-group",
	}

	c, err := kafka.NewConsumer(configMap)

	if err != nil {
		fmt.Println("Error: ", err.Error())
	}

	c.SubscribeTopics([]string{"teste"}, nil)

	for {
		msg, err := c.ReadMessage(-1)

		if err == nil {
			fmt.Println("Message: ", string(msg.Value))
		}
	}

}
