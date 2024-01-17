package rabbitmq

import (
	"context"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Connection represents a RabbitMQ connection
// TODO: add config
type Connection struct {
	*amqp.Connection
	uri          string
	mu           sync.RWMutex // Mutex to synchronize access to the connection
	reconnecting bool         // Flag to indicate ongoing reconnection
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

// IsClosed checks if the connection is closed
func (c *Connection) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.Connection == nil || c.Connection.IsClosed()
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
			// Lock and check if the connection is still nil after reconnect
			if c.conn.Connection == nil {
				c.conn.mu.Unlock()
				log.Printf("[%s] Connection is nil after reconnect. Retrying in 5 seconds.", c.name)
				time.Sleep(5 * time.Second)
				continue
			}

			c.channel, err = c.conn.Channel()
			c.conn.mu.Unlock()

			failOnError(err, "Reopen a channel")
		}

		// Check if the channel is nil
		if c.channel == nil || c.channel.IsClosed() {
			log.Printf("[%s] Channel is nil. Try reopen.", c.name)
			c.channel, err = c.conn.Channel()
			failOnError(err, "Reopen a channel")
			log.Printf("[%s] Channel is reopenned", c.name)
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

			time.Sleep(5 * time.Second)
			continue
		}

		fn(c.conn, c.name, c.queueName, &msgs)

		if c.channel != nil {
			c.channel.Close()
		}
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
func (c *Connection) Reconnect() error {
	// Close the existing connection
	if err := c.Close(); err != nil {
		log.Printf("Error closing RabbitMQ connection: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if already in the process of reconnecting
	if c.reconnecting {
		return nil
	}

	// Set the reconnecting flag
	c.reconnecting = true

	// Attempt to create a new RabbitMQ connection
	newConn, err := ConnectRabbitMQ(c.uri)
	if err != nil {
		log.Printf("Failed to reconnect to RabbitMQ: %v", err)

		// Unset the reconnecting flag in case of failure
		c.reconnecting = false

		// Handle the error or return it as needed
		return err
	}

	// Unset the reconnecting flag upon successful reconnection
	c.reconnecting = false

	if newConn == nil || newConn.Connection == nil {
		log.Printf("Reconnected connection is nil")
		// Handle the nil connection case
		return err
	}

	c.Connection = newConn.Connection

	return nil
}

// ConnectRabbitMQ creates a new RabbitMQ connection
func ConnectRabbitMQ(url string) (*Connection, error) {
	for {
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
	}
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
