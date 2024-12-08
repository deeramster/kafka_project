package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

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
		"group.id":          "consumer-push-group",
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
		ev := consumer.Poll(1000)
		switch e := ev.(type) {
		case *kafka.Message:
			var message Message
			err := json.Unmarshal(e.Value, &message)
			if err != nil {
				return
			}
			fmt.Printf("Received message: %+v\n", message)
		case kafka.Error:
			fmt.Printf("Consumer error: %v", e)
		default:
			time.Sleep(1 * time.Second)
		}
	}
}
