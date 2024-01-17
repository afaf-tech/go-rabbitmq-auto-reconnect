package rabbitmq

import (
	"context"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Connection represents a RabbitMQ connection
type Connection struct {
	*amqp.Connection
	uri string
	mu  sync.Mutex // Mutex to synchronize access to the connection
}

// Consumer represents a RabbitMQ consumer
type Consumer struct {
	conn      *Connection
	channel   *amqp.Channel
	queueName string
	name      string
}

func NewConsumer(conn *Connection, name, queue string) *Consumer {
	//TODO: add option to bind/declare queue?
	ch, err := conn.Channel()
	if err != nil {
		failOnError(err, "Failed to open a channel")
		defer ch.Close()
	}

	// Declare the queue
	_, err = ch.QueueDeclare(
		queue, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	return &Consumer{
		conn:      conn,
		channel:   ch,
		queueName: queue,
		name:      name,
	}
}

// Start begins the consumer's main loop
func (c *Consumer) Start(ctx context.Context, fn func(*Connection, string, string, *<-chan amqp.Delivery)) {
	var err error

	for {
		if c.conn.IsClosed() {
			log.Printf("[%s] Connection is closed. Attempting to reconnect...", c.name)
			err = c.conn.Reconnect()
			if err != nil {
				log.Printf("[%s] Failed to reconnect: %v. Retrying in 5 seconds.", c.name, err)
				time.Sleep(5 * time.Second)
				continue
			}
			log.Printf("[%s] Reconnected to RabbitMQ", c.name)

			c.conn.mu.Lock()
			c.channel, err = c.conn.Channel()
			c.conn.mu.Unlock()

			failOnError(err, "Reopen a channel")
		}

		msgs, err := c.channel.ConsumeWithContext(
			ctx,
			c.queueName, // queue
			c.name,      // consumer
			false,       // auto-ack
			false,       // exclusive
			false,       // no-local
			false,       // no-wait
			nil,         // args
		)
		if err != nil {
			log.Printf("[%s] Failed to register a consumer: %v. Retrying in 5 seconds.", c.name, err)

			c.conn.mu.Lock()
			c.channel.Close() // Close the channel before retrying
			c.conn.mu.Unlock()

			time.Sleep(5 * time.Second)
			continue
		}

		fn(c.conn, c.name, c.queueName, &msgs)

		c.conn.mu.Lock()
		c.channel.Close()
		c.conn.mu.Unlock()
	}
}

// Close closes the RabbitMQ connection
func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.Connection != nil {
		err := c.Connection.Close()
		c.Connection = nil
		return err
	}
	return nil
}

// Reconnect attempts to reconnect to RabbitMQ
func (c *Connection) Reconnect() error {
	if err := c.Close(); err != nil {
		log.Printf("Error closing RabbitMQ connection: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	var err error
	conn, err := ConnectRabbitMQ(c.uri)
	if err == nil {
		c.Connection = conn.Connection
	}
	return err
}

// ConnectRabbitMQ creates a new RabbitMQ connection
func ConnectRabbitMQ(url string) (*Connection, error) {
	conn, err := amqp.Dial(url)
	if err == nil {
		log.Println("Connected to RabbitMQ")
		return &Connection{Connection: conn, uri: url}, nil
	}

	if err, ok := err.(*amqp.Error); ok {
		// Handle AMQP errors
		log.Printf("AMQP Error: Code=%d, Reason=%s. Retrying...", err.Code, err.Reason)
	} else {
		// Handle other errors
		log.Printf("Failed to connect to RabbitMQ: %v. Retrying in 5 seconds.", err)
	}

	const retryDuration = 5 * time.Second
	time.Sleep(retryDuration) // Adjust the delay as needed

	return ConnectRabbitMQ(url)
}

// failOnError logs an error and exits the application if the error is not nil
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

// TODO: publisher?
/*
// Publisher represents a RabbitMQ publisher
type Publisher struct {
	connection *Connection
	exchange   string
} */
