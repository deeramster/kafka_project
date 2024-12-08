package config

import (
	"os"

	"github.com/joho/godotenv"
)

// Config - структура для хранения параметров конфигурации
type Config struct {
	BootstrapServers string // Адреса Kafka-брокеров
	Topic            string // Название Kafka-топика
	ConsumerTimeout  string // Таймаут для Poll
	ProducerTimeout  string // Таймаут завершения работы producer
}

// LoadConfig загружает конфигурацию из переменных окружения или файла
func LoadConfig() Config {
	// Попробуем загрузить переменные из файла .env (для локального запуска)
	_ = godotenv.Load()

	config := Config{
		BootstrapServers: getEnv("BOOTSTRAP_SERVERS", "localhost:9092"),
		Topic:            getEnv("TOPIC", "example-topic"),
		ConsumerTimeout:  getEnv("CONSUMER_TIMEOUT", "1000"),
		ProducerTimeout:  getEnv("PRODUCER_TIMEOUT", "15000"),
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
