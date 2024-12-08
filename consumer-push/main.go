package main

import (
	"encoding/json"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/deeramster/kafka_project/config"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Message struct {
	ID      int    `json:"id"`      // Уникальный числовой идентификатор сообщения
	Content string `json:"content"` // Текстовое содержимое сообщения
	Time    string `json:"time"`    // Время отправки сообщения
}

func main() {
	// Инициализация логгера
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{}))

	cfg := config.LoadConfig()
	logger.Info("Loaded configuration")

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  cfg.BootstrapServers,
		"group.id":           "consumer-push-group",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": true,
		"fetch.min.bytes":    1024,
	})
	if err != nil {
		logger.Error("Failed to create consumer", "error", err)
		return
	}
	defer func(consumer *kafka.Consumer) {
		err := consumer.Close()
		if err != nil {
			logger.Error("Failed to close consumer", "error", err)
		}
	}(consumer)

	if err := consumer.SubscribeTopics([]string{cfg.Topic}, nil); err != nil {
		logger.Error("Failed to subscribe to topic", "topic", cfg.Topic, "error", err)
		return
	}
	logger.Info("Subscribed to topic", "topic", cfg.Topic)

	t, err := strconv.Atoi(cfg.ConsumerTimeout)
	if err != nil {
		logger.Error("Failed to convert timeout value", "timeout", cfg.ConsumerTimeout, "error", err)
	}
	for {
		ev := consumer.Poll(t)
		switch e := ev.(type) {
		case *kafka.Message:
			var message Message
			if err := json.Unmarshal(e.Value, &message); err != nil {
				logger.Error("Failed to deserialize message", "error", err, "raw_message", string(e.Value))
				continue
			}
			logger.Info("Received message", "message", message)
		case kafka.Error:
			logger.Error("Consumer error", "error", e)
		default:
			time.Sleep(1 * time.Second)
		}
	}
}
