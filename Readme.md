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
    docker exec -it <CONTAINER ID> bash
    kafka-topics.sh --create --topic example-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 2
    ```
3. Create .env file:

    ```bash
    echo >> .env BOOTSTRAP_SERVERS=localhost:9094
    echo >> .env TOPIC=example-topic
    echo >> .env CONSUMER_TIMEOUT=1000
    echo >> .env PRODUCER_TIMEOUT=15000
    echo >> .env PUSH_GROUP_ID=consumer-push-group
    echo >> .env PUSH_AUTO_OFFSET_RESET=earliest
    echo >> .env PUSH_ENABLE_AUTO_COMMIT=true
    echo >> .env PUSH_FETCH_MIN_BYTES=1024
    echo >> .env PULL_GROUP_ID=consumer-pull-group
    echo >> .env PULL_AUTO_OFFSET_RESET=earliest
    echo >> .env PULL_ENABLE_AUTO_COMMIT=false
    echo >> .env PULL_FETCH_MIN_BYTES=1024
    ```

4. Run the producer and consumers:
    ```bash
    go run producer/main.go
    go run consumer-pull/main.go
    go run consumer-push/main.go
    ```
