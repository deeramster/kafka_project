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
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9094", "acks": "all"})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer producer.Close()

	topic := "example-topic"

	for i := 0; i < 10; i++ {
		msg := Message{
			ID:      i,
			Content: "Hello Kafka!",
			Time:    time.Now().String(),
		}
		msgBytes, _ := json.Marshal(msg)

		fmt.Printf("Sending message: %+v\n", msg) // Выводим отправляемое сообщение

		err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          msgBytes,
		}, nil)
		if err != nil {
			return
		}
		time.Sleep(1 * time.Second)
	}
}
