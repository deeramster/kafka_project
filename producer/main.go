package main

import (
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
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

	// Загрузка конфигурации
	cfg := config.LoadConfig()
	logger.Info("Loaded configuration", "config", cfg)

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.BootstrapServers,
		"acks":              "all", // Гарантия доставки At Least Once
	})
	if err != nil {
		logger.Error("Failed to create producer", "error", err)
		return
	}
	defer producer.Close()

	// Канал для синхронизации завершения работы
	var wg sync.WaitGroup

	// Обработка delivery reports
	go func() {
		for event := range producer.Events() {
			switch e := event.(type) {
			case *kafka.Message:
				if e.TopicPartition.Error != nil {
					logger.Error("Message delivery failed", "error", e.TopicPartition.Error, "message", string(e.Value))
				} else {
					logger.Info("Message delivered", "partition", e.TopicPartition.Partition, "offset", e.TopicPartition.Offset)
				}
				wg.Done() // Уменьшаем счетчик ожидания
			default:
				logger.Warn("Ignored event", "event", e)
			}
		}
	}()

	// Перехват сигналов завершения работы
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigchan
		logger.Info("Shutdown signal received")
		producer.Flush(cfg.ProducerTimeout) // Ждём завершения отправки сообщений
		os.Exit(0)
	}()

	for i := 0; i < 10; i++ {
		msg := Message{
			ID:      i,
			Content: "Hello Kafka!",
			Time:    time.Now().String(),
		}

		msgBytes, err := json.Marshal(msg)
		if err != nil {
			logger.Error("Failed to serialize message", "error", err, "message", msg)
			continue
		}

		wg.Add(1)

		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &cfg.Topic,
				Partition: kafka.PartitionAny,
			},
			Value: msgBytes,
		}, nil)

		if err != nil {
			logger.Error("Failed to produce message", "error", err, "message", msg)
			wg.Done()
			return
		}

		logger.Info("Message queued for delivery", "message", msg)
		time.Sleep(1 * time.Second)
	}

	// Ждем завершения всех delivery reports
	wg.Wait()

	logger.Info("All messages processed, exiting")
}
