package config

import (
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// Config - структура для хранения параметров конфигурации
type Config struct {
	BootstrapServers        string // Адреса Kafka-брокеров
	Topic                   string // Название Kafka-топика
	ConsumerTimeout         int    // Таймаут для Poll
	ProducerTimeout         int    // Таймаут завершения работы producer
	PUSH_GROUP_ID           string
	PUSH_AUTO_OFFSET_RESET  string
	PUSH_ENABLE_AUTO_COMMIT bool
	PUSH_FETCH_MIN_BYTES    int
	PULL_GROUP_ID           string
	PULL_AUTO_OFFSET_RESET  string
	PULL_ENABLE_AUTO_COMMIT bool
	PULL_FETCH_MIN_BYTES    int
}

func parseInt(str string, defaultValue int) int {
	val, err := strconv.Atoi(str)
	if err != nil {
		log.Printf("Warning: Invalid integer value for '%s', using default: %d\n", str, defaultValue)
		return defaultValue
	}
	return val
}

func parseBool(str string, defaultValue bool) bool {
	val, err := strconv.ParseBool(str)
	if err != nil {
		log.Printf("Warning: Invalid boolean value for '%s', using default: %t\n", str, defaultValue)
		return defaultValue
	}
	return val
}

// LoadConfig загружает конфигурацию из переменных окружения или файла
func LoadConfig() Config {
	// Попробуем загрузить переменные из файла .env (для локального запуска)
	err := godotenv.Load()
	if err != nil {
		log.Printf("Warning: .env file not found, using environment variables\n")
	}

	config := Config{
		BootstrapServers:        getEnv("BOOTSTRAP_SERVERS", "localhost:9092"),
		Topic:                   getEnv("TOPIC", "example-topic"),
		ConsumerTimeout:         parseInt(getEnv("CONSUMER_TIMEOUT", "1000"), 1000),
		ProducerTimeout:         parseInt(getEnv("PRODUCER_TIMEOUT", "15000"), 15000),
		PUSH_GROUP_ID:           getEnv("PUSH_GROUP_ID", "example-group"),
		PUSH_AUTO_OFFSET_RESET:  getEnv("PUSH_AUTO_OFFSET_RESET", "earliest"),
		PUSH_ENABLE_AUTO_COMMIT: parseBool(getEnv("PUSH_ENABLE_AUTO_COMMIT", "true"), true),
		PUSH_FETCH_MIN_BYTES:    parseInt(getEnv("PUSH_FETCH_MIN_BYTES", "1024"), 1024),
		PULL_GROUP_ID:           getEnv("PULL_GROUP_ID", "pull-group"),
		PULL_AUTO_OFFSET_RESET:  getEnv("PULL_AUTO_OFFSET_RESET", "earliest"),
		PULL_ENABLE_AUTO_COMMIT: parseBool(getEnv("PULL_ENABLE_AUTO_COMMIT", "false"), false),
		PULL_FETCH_MIN_BYTES:    parseInt(getEnv("PULL_FETCH_MIN_BYTES", "1024"), 1024),
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
