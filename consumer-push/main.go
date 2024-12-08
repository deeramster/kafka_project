package main

import (
	"encoding/json"
	"log/slog"
	"os"
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
		"group.id":           cfg.PUSH_GROUP_ID,
		"auto.offset.reset":  cfg.PUSH_AUTO_OFFSET_RESET,
		"enable.auto.commit": cfg.PUSH_ENABLE_AUTO_COMMIT,
		"fetch.min.bytes":    cfg.PUSH_FETCH_MIN_BYTES,
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
		ev := consumer.Poll(cfg.ConsumerTimeout)
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
