package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"afaf.internal/pkg/rabbitmq"
)

// Notif represents a sample notification message
type Notif struct {
	Title   string `json:"title"`
	Content string `json:"content"`
}

// ConsumerCreate represents a request to create a new consumer
type ConsumerCreate struct {
	Name      string `json:"name"`
	QueueName string `json:"queue_name"`
}

func main() {
	const rbmqURI = "amqp://guest:guest@localhost:5672/"

	conn := rabbitmq.NewConnection(&rabbitmq.Config{URI: rbmqURI, MaxChannels: 25})
	err := conn.Connect()
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// Define queue options
	queueOptions := rabbitmq.QueueOptions{
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args:       nil,
	}

	// Initialize producers
	consumerCreateProducer, err := rabbitmq.NewProducer(conn, "consumer-create-producer", "consumer-creation", queueOptions)
	failOnError(err, "Failed to create consumer creation producer")
	defer consumerCreateProducer.Close()

	// Setup a scanner for user input
	scanner := bufio.NewScanner(os.Stdin)

	// Wait for interrupt signal to gracefully shutdown
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)

	// Interactive prompt
	for {
		fmt.Println("\nEnter 'notif' to send a notification, 'consumer' to create a consumer, or 'exit' to quit:")
		scanner.Scan()
		if err := scanner.Err(); err != nil {
			log.Printf("Error reading input: %v", err)
			break
		}

		input := strings.TrimSpace(scanner.Text())

		if input == "exit" {
			break // Exit the loop on 'exit' command

		}

		// TODO: producer can't handle reconnection
		switch input {
		case "notif":
			// Prompt user for the queue name
			fmt.Println("Enter the queue name for the notification:")
			scanner.Scan()
			if err := scanner.Err(); err != nil {
				log.Printf("Error reading input: %v", err)
				break
			}
			queueName := scanner.Text()

			// Create new notification producer with dynamic queue name
			dynamicNotifProducer, err := rabbitmq.NewProducer(conn, "notif-producer", queueName, queueOptions)
			if err != nil {
				log.Printf("Failed to create dynamic notification producer for queue %s: %v", queueName, err)
				continue
			}
			defer dynamicNotifProducer.Close()

			// Prompt user for notification details
			fmt.Println("Enter notification title:")
			scanner.Scan()
			if err := scanner.Err(); err != nil {
				log.Printf("Error reading input: %v", err)
				break
			}
			title := scanner.Text()

			fmt.Println("Enter notification content:")
			scanner.Scan()
			if err := scanner.Err(); err != nil {
				log.Printf("Error reading input: %v", err)
				break
			}
			content := scanner.Text()

			// Create notification and publish to RabbitMQ
			notif := Notif{
				Title:   title,
				Content: content,
			}

			message, err := json.Marshal(notif)
			if err != nil {
				log.Fatalf("Failed to marshal notification: %v", err)
			}

			err = dynamicNotifProducer.Publish(context.Background(), message)
			if err != nil {
				log.Printf("Failed to publish notification: %v", err)
			} else {
				log.Printf("Published notification to queue %s: %s", queueName, message)
			}

		case "consumer":
			// Prompt user for consumer details
			fmt.Println("Enter consumer name:")
			scanner.Scan()
			if err := scanner.Err(); err != nil {
				log.Printf("Error reading input: %v", err)
				break
			}
			name := scanner.Text()
			fmt.Println("Enter queue name:")
			scanner.Scan()
			if err := scanner.Err(); err != nil {
				log.Printf("Error reading input: %v", err)
				break
			}
			queueName := scanner.Text()

			// Create consumer creation request and publish to RabbitMQ
			consumerCreate := ConsumerCreate{
				Name:      name,
				QueueName: queueName, // Example queue name
			}

			message, err := json.Marshal(consumerCreate)
			if err != nil {
				log.Fatalf("Failed to marshal consumer creation: %v", err)
			}

			err = consumerCreateProducer.Publish(context.Background(), message)
			if err != nil {
				log.Printf("Failed to publish consumer creation request: %v", err)
			} else {
				log.Printf("Published consumer creation request: %s", message)
			}
		default:
			fmt.Println("Invalid input. Please enter 'notif', 'consumer', or 'exit'.")
		}
	}

	os.Exit(0)
}

// failOnError logs an error and exits the application if the error is not nil
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
