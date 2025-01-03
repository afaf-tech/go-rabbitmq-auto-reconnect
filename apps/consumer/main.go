package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"

	"afaf.internal/pkg/rabbitmq"
)

var conn *rabbitmq.Connection

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const rbmqURI = "amqp://guest:guest@localhost:5672/"

	conn = rabbitmq.NewConnection(&rabbitmq.Config{URI: rbmqURI, MaxChannels: 4})
	if err := conn.Connect(); err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	// Start consumers
	startConsumer(ctx, conn, "sms", "sms", messageHandlerPrintOnly)
	log.Print("start sms consumer")
	startConsumer(ctx, conn, "ConsumerCreation", "consumer-creation", messageHandlerCreateConsumer)
	log.Print("start CunsomerCreation consumer")

	// Wait for interrupt signal to gracefully shutdown
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	<-signalCh

	cancel() // Stop all consumers
}

func startConsumer(ctx context.Context, conn *rabbitmq.Connection, name, queue string, handler func(ctx context.Context, msg *rabbitmq.Message)) {
	consumer, err := rabbitmq.NewConsumer(conn, name, queue, rabbitmq.QueueOptions{
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
	})
	if err != nil {
		log.Fatalf("Failed to create consumer [%s]: %v", name, err)
	}

	go consumer.Start(ctx, handler)
}

func messageHandlerPrintOnly(ctx context.Context, msg *rabbitmq.Message) {
	log.Printf("Consumer [%s] received message on queue [%s]: %s", msg.ConsumerID, msg.QueueName, msg.Body)
	msg.Ack()
}

func messageHandlerCreateConsumer(ctx context.Context, msg *rabbitmq.Message) {
	var payload struct {
		Name      string `json:"name"`
		QueueName string `json:"queue_name"`
	}

	if err := json.Unmarshal(msg.Body, &payload); err != nil {
		log.Printf("Failed to unmarshal message: %v", err)
		msg.Nack()
		return
	}

	log.Printf("Creating consumer: %s", payload.Name)
	startConsumer(ctx, conn, payload.Name, payload.QueueName, messageHandlerPrintOnly)
	msg.Ack()
}
