package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Message struct {
	ID      int    `json:"id"`
	Content string `json:"content"`
	Time    string `json:"time"`
}

func main() {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9094",
		"group.id":          "consumer-pull-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer func(consumer *kafka.Consumer) {
		err := consumer.Close()
		if err != nil {

		}
	}(consumer)

	consumer.SubscribeTopics([]string{"example-topic"}, nil)

	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			var message Message
			err := json.Unmarshal(msg.Value, &message)
			if err != nil {
				return
			}
			fmt.Printf("Received message: %+v\n", message)
		} else {
			log.Printf("Consumer error: %v", err)
		}
	}
}
