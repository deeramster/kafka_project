package config

import (
	"log/slog"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// Config - структура для хранения параметров конфигурации
type Config struct {
	BootstrapServers     string // Адреса Kafka-брокеров
	Topic                string // Название Kafka-топика
	ConsumerTimeout      int    // Таймаут для Poll
	ProducerTimeout      int    // Таймаут завершения работы producer
	PushGroupId          string
	PushAutoOffsetReset  string
	PushEnableAutoCommit bool
	PushFetchMinBytes    int
	PullGroupId          string
	PullAutoOffsetReset  string
	PullEnableAutoCommit bool
	PullFetchMinBytes    int
}

func parseInt(str string, defaultValue int) int {
	val, err := strconv.Atoi(str)
	if err != nil {
		slog.Warn("Invalid integer value", "value", str, "default", defaultValue)
		return defaultValue
	}
	return val
}

func parseBool(str string, defaultValue bool) bool {
	val, err := strconv.ParseBool(str)
	if err != nil {
		slog.Warn("Invalid boolean value", "value", str, "default", defaultValue)
		return defaultValue
	}
	return val
}

// LoadConfig загружает конфигурацию из переменных окружения или файла
func LoadConfig() Config {
	err := godotenv.Load()
	if err != nil {
		slog.Warn(".env file not found, using environment variables")
	}

	config := Config{
		BootstrapServers:     getEnv("BOOTSTRAP_SERVERS", "localhost:9092"),
		Topic:                getEnv("TOPIC", "example-topic"),
		ConsumerTimeout:      parseInt(getEnv("CONSUMER_TIMEOUT", "1000"), 1000),
		ProducerTimeout:      parseInt(getEnv("PRODUCER_TIMEOUT", "15000"), 15000),
		PushGroupId:          getEnv("PUSH_GROUP_ID", "example-group"),
		PushAutoOffsetReset:  getEnv("PUSH_AUTO_OFFSET_RESET", "earliest"),
		PushEnableAutoCommit: parseBool(getEnv("PUSH_ENABLE_AUTO_COMMIT", "true"), true),
		PushFetchMinBytes:    parseInt(getEnv("PUSH_FETCH_MIN_BYTES", "1024"), 1024),
		PullGroupId:          getEnv("PULL_GROUP_ID", "pull-group"),
		PullAutoOffsetReset:  getEnv("PULL_AUTO_OFFSET_RESET", "earliest"),
		PullEnableAutoCommit: parseBool(getEnv("PULL_ENABLE_AUTO_COMMIT", "false"), false),
		PullFetchMinBytes:    parseInt(getEnv("PULL_FETCH_MIN_BYTES", "10cc24"), 1024),
	}

	return config
}

// getEnv возвращает значение переменной окружения или значение по умолчанию
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
