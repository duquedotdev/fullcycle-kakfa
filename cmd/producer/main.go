package main

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	deliveryChannel := make(chan kafka.Event)

	producer := NewKafkaProducer()

	Publish("Hello World", "teste", producer, nil, deliveryChannel)
	// Syncronous

	// event := <-deliveryChannel

	// msg := event.(*kafka.Message)

	// if msg.TopicPartition.Error != nil {
	// 	log.Println("Delivery failed: ", msg.TopicPartition.Error)
	// } else {
	// 	log.Println("Delivered message to topic ", msg.TopicPartition)
	// }

	// close(deliveryChannel)

	// Asyncronous (with goroutine)
	go DeliveryReport(deliveryChannel)
	producer.Flush(2000) // wait for max 2 ms for events to be delivered

}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":   "kafka_go_kafka_1:9092",
		"delivery.timeout.ms": 1000,
		"acks":                "all",
		"retries":             10,
		"enable.idempotence":  true, // Acks tem que ser all para funcionar
	}

	p, err := kafka.NewProducer(configMap)

	if err != nil {
		log.Println(err.Error())
	}

	return p
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		Value: []byte(msg),
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key: key,
	}
	err := producer.Produce(message, deliveryChan)
	if err != nil {
		return err
	}
	return nil
}

func DeliveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch event := e.(type) {
		case *kafka.Message:
			if event.TopicPartition.Error != nil {
				log.Println("Delivery failed: ", event.TopicPartition.Error)
			} else {
				log.Println("Delivered message to topic ", event.TopicPartition)
			}
		}
	}
}
