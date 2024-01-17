package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"

	"afaf.internal/pkg/rabbitmq"
	"github.com/rabbitmq/amqp091-go"
)

// Sample struct
type Notif struct {
	Title   string `json:"title"`
	Content string `json:"content"`
}

type CunsomerCreate struct {
	Name      string `json:"name"`
	QueueName string `json:"queue_name"`
}

// TODO: encapsulate
func main() {
	const rbmqURI = "amqp://guest:guest@localhost:5672/"

	conn, err := rabbitmq.ConnectRabbitMQ(rbmqURI)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "error opening channel")

	// Declare Exchange, Route and Queue
	notifExchange := "notification"
	err = ch.ExchangeDeclare(notifExchange, "direct", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("exchange.declare: %s", err)
	}

	cunsomerExchange := "cunsomer"
	err = ch.ExchangeDeclare(cunsomerExchange, "direct", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("exchange.declare: %s", err)
	}

	// Establish our queue topologies that we are responsible for
	type bind struct {
		queue string
		key   string
	}

	notificationRouteBindings := []bind{
		{"sms", "sms"},
	}
	for _, b := range notificationRouteBindings {
		_, err = ch.QueueDeclare(b.queue, true, false, false, false, nil)
		if err != nil {
			log.Fatalf("queue.declare: %v", err)
		}

		err = ch.QueueBind(b.queue, b.key, notifExchange, false, nil)
		if err != nil {
			log.Fatalf("queue.bind: %v", err)
		}
	}

	cunsomerCreateRouteQueue := "cunsomer.create"
	_, err = ch.QueueDeclare(cunsomerCreateRouteQueue, true, false, false, false, nil)
	if err != nil {
		log.Fatalf("queue.declare: %v", err)
	}

	err = ch.QueueBind(cunsomerCreateRouteQueue, cunsomerCreateRouteQueue, cunsomerExchange, false, nil)
	if err != nil {
		log.Fatalf("queue.bind: %v", err)
	}

	ctx := context.Background()
	// // publish to notification exchange and route sms
	// for i := 0; i < 10; i++ {
	// 	notif, err := json.Marshal(Notif{
	// 		Title:   "Transfer duit masuk",
	// 		Content: "berapa hayooo???",
	// 	})
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	message := amqp091.Publishing{
	// 		Headers: amqp091.Table{
	// 			"sample": "value",
	// 		},
	// 		Body: notif,
	// 	}

	// 	err = ch.PublishWithContext(ctx, notifExchange, "sms", false, false, message)
	// 	failOnError(err, "Publishing message err")
	// }

	// publish create cunsomer
	cunsomerCreate, err := json.Marshal(CunsomerCreate{
		Name:      "sms2",
		QueueName: "sms",
	})
	if err != nil {
		log.Fatal(err)
	}
	message := amqp091.Publishing{
		Headers: amqp091.Table{
			"sample": "value",
		},
		Body: cunsomerCreate,
	}

	err = ch.PublishWithContext(ctx, notifExchange, cunsomerCreateRouteQueue, false, false, message)
	failOnError(err, "Publishing cunsomer.create message err")

	// Wait for interrupt signal to gracefully shutdown
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)

	// Block until a signal is received
	<-signalCh
}

// failOnError logs an error and exits the application if the error is not nil
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
