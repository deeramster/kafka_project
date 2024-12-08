package main

import (
	"encoding/json"
	"log/slog"
	"os"

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
		"group.id":           cfg.PullGroupId,
		"auto.offset.reset":  cfg.PullAutoOffsetReset,
		"enable.auto.commit": cfg.PullEnableAutoCommit,
		"fetch.min.bytes":    cfg.PullFetchMinBytes,
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

	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			var message Message
			if err := json.Unmarshal(msg.Value, &message); err != nil {
				logger.Error("Failed to deserialize message", "error", err, "raw_message", string(msg.Value))
				continue
			}
			logger.Info("Received message", "message", message)

			_, commitErr := consumer.Commit()
			if commitErr != nil {
				logger.Error("Failed to commit offsets", "error", commitErr)
			} else {
				logger.Info("Offsets committed")
			}
		} else {
			logger.Error("Consumer error", "error", err)
		}
	}
}
