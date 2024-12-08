package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

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
		"group.id":           "consumer-push-group", // Уникальный идентификатор группы консьюмеров
		"auto.offset.reset":  "earliest",            // Начать чтение с самого старого сообщения
		"enable.auto.commit": true,                  // Автоматический коммит смещений
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer consumer.Close()

	// Подписываемся на указанный топик
	consumer.SubscribeTopics([]string{"example-topic"}, nil)

	for {
		// Пытаемся получить событие с таймаутом
		ev := consumer.Poll(1000) // Ждём до 1000 мс
		switch e := ev.(type) {
		case *kafka.Message:
			var message Message
			json.Unmarshal(e.Value, &message)              // Десериализуем JSON
			fmt.Printf("Received message: %+v\n", message) // Выводим сообщение на консоль
		case kafka.Error:
			// Логируем ошибку
			fmt.Printf("Consumer error: %v\n", e)
		default:
			// Пустой цикл при отсутствии сообщений
			time.Sleep(1 * time.Second)
		}
	}
}
