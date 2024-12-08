# Kafka Project

## Overview

This project demonstrates a Kafka setup with a producer and two types of consumers.

### Features
- Producer that sends JSON messages.
- Pull-based consumer.
- Push-emulated consumer.

## Setup

1. Start the Kafka cluster:
    ```bash
    docker-compose up -d
    ```

2. Create the topic:
    ```bash
    kafka-topics.sh --create --topic example-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 2
    ```

3. Run the producer and consumers:
    ```bash
    go run producer/main.go
    go run consumer-pull/main.go
    go run consumer-push/main.go
    ```
