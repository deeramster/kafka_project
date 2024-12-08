package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Message - структура для получаемого сообщения
type Message struct {
	ID      int    `json:"id"`      // Уникальный числовой идентификатор сообщения
	Content string `json:"content"` // Текстовое содержимое сообщения
	Time    string `json:"time"`    // Время отправки сообщения
}

func main() {
	// Создаём консьюмера с указанной конфигурацией
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9094",      // Адрес Kafka-брокеров
		"group.id":           "consumer-pull-group", // Уникальный идентификатор группы консьюмеров
		"auto.offset.reset":  "earliest",            // Начать чтение с самого старого сообщения, если смещение отсутствует
		"enable.auto.commit": false,                 // Ручной коммит смещений (гарантия точного контроля над обработкой)
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer consumer.Close()

	// Подписываемся на указанный топик
	consumer.SubscribeTopics([]string{"example-topic"}, nil)

	for {
		// Читаем сообщение из топика
		msg, err := consumer.ReadMessage(-1) // Блокирующий вызов до получения сообщения
		if err == nil {
			var message Message
			json.Unmarshal(msg.Value, &message)            // Десериализуем JSON
			fmt.Printf("Received message: %+v\n", message) // Выводим сообщение на консоль

			// Коммитим смещение вручную
			_, commitErr := consumer.Commit()
			if commitErr != nil {
				log.Printf("Commit failed: %v\n", commitErr)
			}
		} else {
			log.Printf("Consumer error: %v\n", err)
		}
	}
}
