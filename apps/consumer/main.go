package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"

	amqp "github.com/rabbitmq/amqp091-go"

	"afaf.internal/pkg/rabbitmq"
)

// TODO: consumers that created from queue open new connections when reopen

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const rbmqURI = "amqp://guest:guest@localhost:5672/"

	conn := rabbitmq.NewConnection(rbmqURI)
	err := conn.Connect()
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// Create and run consumers
	// dummy cunsomer
	smsConsumer := rabbitmq.NewConsumer(conn, "sms", "sms")
	log.Print("start sms consumer")
	go smsConsumer.Start(ctx, messageHandlerPrintOnly)

	// // creation cunsomer
	creationConsumer := rabbitmq.NewConsumer(conn, "CunsomerCreation", "cunsomer-creation")
	log.Print("start CunsomerCreation consumer")
	go creationConsumer.Start(ctx, messageHandlerCreateCunsomer)

	// Wait for interrupt signal to gracefully shutdown
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)

	// Block until a signal is received
	<-signalCh

	// Optionally: Gracefully shutdown consumers
	cancel() // Signal the context to cancel
}

// Sample struct
type Notif struct {
	Title   string `json:"title"`
	Content string `json:"content"`
}

func messageHandlerPrintOnly(conn *rabbitmq.Connection, cName string, q string, deliveries *<-chan amqp.Delivery) {
	for d := range *deliveries {
		//handle the custom message
		log.Printf("Got message from consumer %s, queue %s, message %s", cName, q, d.Body)
		d.Ack(false)
	}
}

type CunsomerCreate struct {
	Name      string `json:"name"`
	QueueName string `json:"queue_name"`
}

func messageHandlerCreateCunsomer(conn *rabbitmq.Connection, cName string, q string, deliveries *<-chan amqp.Delivery) {
	for d := range *deliveries {
		// handle the custom message
		log.Printf("Got message from consumer %s, queue %s, message %s", cName, q, d.Body)

		// check ?
		// create consumer
		var cunsomerCreate CunsomerCreate
		err := json.Unmarshal(d.Body, &cunsomerCreate)
		if err != nil {
			log.Printf("err parsing %s", err.Error())
			continue
		}

		// Use mutex before creating a new consumer
		newCunsomer := rabbitmq.NewConsumer(conn, cunsomerCreate.Name, cunsomerCreate.QueueName)

		// Start the new consumer in a goroutine
		go newCunsomer.Start(context.Background(), messageHandlerPrintOnly)

		d.Ack(false)
	}
}

// failOnError logs an error and exits the application if the error is not nil
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}