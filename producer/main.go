package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Message - структура для отправляемого сообщения
type Message struct {
	ID      int    `json:"id"`      // Уникальный числовой идентификатор сообщения
	Content string `json:"content"` // Текстовое содержимое сообщения
	Time    string `json:"time"`    // Время отправки сообщения
}

func main() {
	// Создаём продюсера с указанной конфигурацией
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9094", // Адрес Kafka-брокеров
		"acks":              "all",            // Гарантия доставки At Least Once: сообщение считается доставленным, если все реплики подтвердили получение
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer producer.Close()

	topic := "example-topic" // Название Kafka-топика, в который будут отправляться сообщения

	for i := 0; i < 10; i++ {
		// Формируем сообщение
		msg := Message{
			ID:      i,                   // Порядковый номер сообщения
			Content: "Hello Kafka!",      // Текст сообщения
			Time:    time.Now().String(), // Текущая временная метка
		}
		msgBytes, _ := json.Marshal(msg) // Сериализация сообщения в JSON

		// Выводим сообщение на консоль перед отправкой
		fmt.Printf("Sending message: %+v\n", msg)

		// Отправляем сообщение в Kafka
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,             // Указываем топик
				Partition: kafka.PartitionAny, // Автоматический выбор партиции
			},
			Value: msgBytes, // Сериализованные данные сообщения
		}, nil)

		// Небольшая пауза между отправками
		time.Sleep(1 * time.Second)
	}
}
