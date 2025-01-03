package rabbitmq

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Message represents a message received by a consumer from a RabbitMQ queue.
// It includes the message body and methods for acknowledging, negatively acknowledging,
// or rejecting the message, as well as metadata about the message's origin.
type Message struct {
	// Body is the content of the message received from the queue.
	// It contains the actual data sent by the producer.
	Body []byte

	// Ack is a function that, when called, acknowledges the message.
	// It informs RabbitMQ that the message has been successfully processed.
	Ack func()

	// Nack is a function that, when called, negatively acknowledges the message.
	// It informs RabbitMQ that the message was not processed successfully, and depending on
	// the RabbitMQ configuration, it may be re-delivered or discarded.
	Nack func()

	// Reject is a function that, when called, rejects the message.
	// It informs RabbitMQ that the message was not processed and does not want it to be re-delivered.
	Reject func()

	// QueueName is the name of the RabbitMQ queue from which the message was consumed.
	// It helps to identify the source of the message.
	QueueName string

	// ConsumerID is the unique identifier of the consumer that received the message.
	// It can be used for logging or to distinguish between multiple consumers.
	ConsumerID string
}

// Consumer represents an AMQP consumer that consumes messages from a RabbitMQ queue.
// It manages the connection, channel, and queue configurations for the consumer.
type Consumer struct {
	// conn is the underlying AMQP connection used by the consumer to communicate with RabbitMQ.
	// It allows for opening channels and managing message consumption.
	conn *Connection

	// channel is the AMQP channel through which the consumer will consume messages from the queue.
	// A channel is used to send and receive messages between the consumer and the broker.
	channel *amqp.Channel

	// queueName is the name of the RabbitMQ queue from which this consumer will consume messages.
	// It helps the consumer identify which queue to connect to.
	queueName string

	// name is the unique name of this consumer, used for identification purposes.
	// It is often used for logging or distinguishing between multiple consumers.
	name string

	// queueConfig holds the configuration options for the queue, such as whether the queue is durable,
	// auto-delete, and other properties that affect its behavior.
	queueConfig QueueOptions
}

// NewConsumer initializes a new Consumer instance and declares the queue with the specified options
func NewConsumer(conn *Connection, name, queue string, options QueueOptions) (*Consumer, error) {
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

func (c *Consumer) Start(ctx context.Context, handler func(ctx context.Context, msg *Message)) {
	for {
		if err := c.checkConnectionAndChannel(); err != nil {
			time.Sleep(c.conn.config.RetryDuration)
			continue
		}

		deliveries, err := c.channel.ConsumeWithContext(
			ctx,
			c.queueName,
			c.name,
			c.queueConfig.AutoAck,
			c.queueConfig.Exclusive,
			c.queueConfig.NoLocal,
			c.queueConfig.NoWait,
			c.queueConfig.Args,
		)
		if err != nil {
			log.Printf("[%s] Failed to start consuming: %v. Retrying...", c.name, err)
			time.Sleep(c.conn.config.RetryDuration)
			continue
		}

		for d := range deliveries {
			msg := &Message{
				Body:       d.Body,
				Ack:        func() { d.Ack(false) },
				Nack:       func() { d.Nack(false, true) },
				Reject:     func() { d.Reject(false) },
				QueueName:  c.queueName,
				ConsumerID: c.name,
			}

			handler(ctx, msg)
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
