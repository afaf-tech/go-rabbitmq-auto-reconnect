package rabbitmq

import (
	"context"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	retryDuration = time.Second * 5
)

type Connection struct {
	*amqp.Connection
	uri          string
	mu           sync.RWMutex // Mutex to synchronize access to the connection
	reconnecting bool         // Flag to indicate ongoing reconnection
}

// TODO: add config parameter amqp.Config{}
// NewConnection creates a new Connection object without establishing the connection
func NewConnection(url string) *Connection {
	return &Connection{uri: url}
}

// Connect establishes the RabbitMQ connection
func (c *Connection) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.reconnecting {
		return nil // Already in the process of reconnecting
	}

	for {
		conn, err := amqp.Dial(c.uri)
		if err == nil {
			log.Println("Connected to RabbitMQ")
			c.Connection = conn
			c.reconnecting = false

			return nil
		}

		if err, ok := err.(*amqp.Error); ok {
			// Handle AMQP errors
			log.Printf("AMQP Error: Code=%d, Reason=%s. Retrying...", err.Code, err.Reason)
		} else {
			// Handle other errors
			log.Printf("Failed to connect to RabbitMQ: %v. Retrying in 5 seconds.", err)
		}

		c.reconnecting = true
		time.Sleep(retryDuration) // Adjust the delay as needed
	}

}

func (c *Connection) Reconnect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.reconnecting {
		log.Println("Already in the process of reconnecting. Skipping.")
		return nil
	}

	c.reconnecting = true

	conn, err := amqp.Dial(c.uri)
	if err == nil {
		log.Println("Connected to RabbitMQ")
		c.Connection = conn
		c.reconnecting = false
		return nil
	}

	log.Printf("Failed to reconnect to RabbitMQ: %v. Retrying in 5 seconds.", err)

	c.reconnecting = false
	return err
}

func (c *Connection) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.Connection == nil || c.Connection.IsClosed()
}

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
		err = c.checkConnection()
		if err != nil {
			time.Sleep(retryDuration)
			continue
		}

		// Register the consumer and handle any errors
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
			time.Sleep(retryDuration)
			continue
		}

		// Execute the user-defined function
		fn(c.conn, c.name, c.queueName, &msgs)

		// Close the channel after consuming messages
		if c.channel != nil {
			c.channel.Close()
		}
	}
}

func (c *Consumer) checkConnection() error {
	var err error
	if c.conn.IsClosed() {
		log.Printf("[%s] Connection is closed. Attempting to reconnect...", c.name)
		err = c.conn.Reconnect()
		if err != nil {
			log.Printf("[%s] Failed to reconnect: %v. Retrying in 5 seconds.", c.name, err)
			return err
		}
		log.Printf("[%s] Reconnected to RabbitMQ", c.name)
	}

	if c.channel == nil || c.channel.IsClosed() {
		log.Printf("[%s] Channel is nil or closed. Trying to reopen...", c.name)
		c.channel, err = c.conn.Channel()
		if err != nil {
			log.Printf("[%s] Failed to reopen channel: %v. Retrying in 5 seconds.", c.name, err)
			return err
		}
		log.Printf("[%s] Channel reopened", c.name)
	}

	return err
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
