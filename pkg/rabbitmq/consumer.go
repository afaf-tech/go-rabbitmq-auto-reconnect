package rabbitmq

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	conn        *Connection
	channel     *amqp.Channel
	queueName   string
	name        string
	queueConfig QueueOptions
}

// NewConsumer initializes a new Consumer instance and declares the queue with the specified options
func NewConsumer(conn *Connection, name, queue string, options QueueOptions) (*Consumer, error) {
	// Ensure the connection is established
	err := conn.Connect()
	if err != nil {
		log.Printf("[%s] Failed to connect to RabbitMQ", name)
		return nil, err
	}

	// Create a new channel
	ch, err := conn.OpenChannel()
	if err != nil {
		log.Printf("[%s] Failed to open a channel", name)
		return nil, err
	}

	// Declare the queue
	_, err = ch.QueueDeclare(
		queue,
		options.Durable,
		options.AutoDelete,
		options.Exclusive,
		options.NoWait,
		options.Args,
	)
	if err != nil {
		log.Printf("[%s] Failed to declare queue: %v", name, err)
		defer ch.Close()
		return nil, err
	}

	return &Consumer{
		conn:        conn,
		channel:     ch,
		queueName:   queue,
		name:        name,
		queueConfig: options,
	}, nil
}

// Start begins the consumer's main loop
func (c *Consumer) Start(ctx context.Context, fn func(*Connection, string, string, *<-chan amqp.Delivery)) {
	var err error

	for {
		// Ensure connection and channel are valid before starting consumption
		err = c.checkConnectionAndChannel()
		if err != nil {
			time.Sleep(c.conn.config.RetryDuration)
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
			time.Sleep(c.conn.config.RetryDuration)
			continue
		}

		// Execute the user-defined function
		fn(c.conn, c.name, c.queueName, &msgs)

		// Close the channel after consuming messages (if necessary)
		if c.channel != nil {
			c.channel.Close()
		}
	}
}

// checkConnectionAndChannel ensures that both the connection and channel are open
func (c *Consumer) checkConnectionAndChannel() error {
	var err error
	// Check if the connection is closed
	if c.conn.IsClosed() {
		log.Printf("[%s] Connection is closed. Attempting to reconnect...", c.name)
		err = c.conn.Reconnect()
		if err != nil {
			log.Printf("[%s] Failed to reconnect: %v. Retrying in 5 seconds.", c.name, err)
			return err
		}
		log.Printf("[%s] Reconnected to RabbitMQ", c.name)
	}

	// Check if the channel is closed or nil
	if c.channel == nil || c.channel.IsClosed() {
		log.Printf("[%s] Channel is nil or closed. Trying to reopen...", c.name)
		c.channel, err = c.conn.OpenChannel()
		if err != nil {
			log.Printf("[%s] Failed to reopen channel: %v. Retrying in 5 seconds.", c.name, err)
			return err
		}
		log.Printf("[%s] Channel reopened", c.name)
	}

	return nil
}
