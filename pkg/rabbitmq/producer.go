package rabbitmq

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Producer represents an AMQP producer that sends messages to a specified RabbitMQ queue.
type Producer struct {
	// conn is the underlying AMQP connection that this producer uses to communicate with the RabbitMQ broker.
	// It provides the connection functionality for creating channels and sending messages.
	conn *Connection

	// channel is the AMQP channel used by this producer to publish messages to RabbitMQ.
	// A channel is a lightweight connection to the broker and is used to send and receive messages.
	channel *amqp.Channel

	// queueName is the name of the RabbitMQ queue to which this producer sends messages.
	// It identifies the target queue for message delivery.
	queueName string

	// name is the unique name of this producer, often used for logging or identifying the producer.
	// It helps distinguish multiple producers in the system.
	name string
}

// NewProducer initializes a new Producer instance and declares the queue with the specified options
func NewProducer(conn *Connection, name, queue string) (*Producer, error) {
	// Ensure the connection is established
	if conn.IsClosed() {
		err := conn.Connect()
		if err != nil {
			log.Printf("[%s] Failed to connect to RabbitMQ", name)
			return nil, err
		}
	}

	ch, err := conn.OpenChannel()
	if err != nil {
		log.Printf("[%s] Failed to open a channel", name)
		return nil, err
	}

	return &Producer{
		conn:      conn,
		channel:   ch,
		queueName: queue,
		name:      name,
	}, nil
}

// PublishOptions represents the configuration options for publishing a message to RabbitMQ.
type PublishOptions struct {
	// Exchange is the name of the exchange to which the message will be published.
	// The exchange determines how the message will be routed to queues.
	Exchange string

	// RoutingKey is the routing key used by the exchange to decide where to route the message.
	// The value depends on the type of exchange (e.g., direct, topic).
	RoutingKey string

	// ContentType specifies the content type of the message. It helps the consumer interpret the message body.
	// For example, "text/plain" or "application/json".
	ContentType string

	// Body is the actual message content, represented as a byte slice.
	// This is the payload of the message being sent to the RabbitMQ exchange.
	Body []byte

	// Mandatory indicates whether the message is mandatory.
	// If true, RabbitMQ will return the message to the producer if it cannot be routed to a queue.
	Mandatory bool

	// Immediate indicates whether the message is immediate.
	// If true, RabbitMQ will try to deliver the message to a consumer immediately, if possible.
	Immediate bool

	// Headers is an optional map of headers that can be included with the message.
	// These headers can carry metadata or additional information to help the consumer process the message.
	Headers amqp.Table
}

// Publish sends a message to the queue with retry logic and context support
func (p *Producer) Publish(ctx context.Context, message []byte, options PublishOptions) error {
	if ctx == nil {
		// Default to a background context with a timeout
		defaultCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ctx = defaultCtx
	}

	retryDuration := p.conn.config.RetryDuration

	// Retry indefinitely until the message is successfully published
	for {
		// Check if connection or channel is closed, attempt to reconnect if necessary
		err := p.checkConnection()
		if err != nil {
			time.Sleep(retryDuration)
			continue
		}

		// Attempt to publish the message
		err = p.channel.PublishWithContext(
			ctx,
			options.Exchange,   // Use the exchange from the options
			options.RoutingKey, // Use the routing key from the options
			options.Mandatory,  // Use the mandatory flag from the options
			options.Immediate,  // Use the immediate flag from the options
			amqp.Publishing{
				ContentType: options.ContentType, // Use the content type from the options
				Body:        options.Body,        // Use the message body from the options
				Headers:     options.Headers,     // Use the headers from the options (if any)
			},
		)

		if err == nil {
			return nil
		}

		log.Printf("[%s] Failed to publish message: %v. Retrying in %v...", p.name, err, retryDuration)
		time.Sleep(retryDuration)
	}
}

// checkConnection checks if the connection and channel are valid and reconnects if necessary
func (p *Producer) checkConnection() error {
	// If the connection is closed, try to reconnect
	if p.conn.IsClosed() {
		log.Printf("[%s] Connection is closed. Attempting to reconnect...", p.name)
		err := p.conn.Reconnect()
		if err != nil {
			log.Printf("[%s] Failed to reconnect: %v. Retrying...", p.name, err)
			return err
		}
		log.Printf("[%s] Reconnected to RabbitMQ", p.name)
	}

	// If the channel is closed or nil, try to open a new one
	if p.channel == nil || p.channel.IsClosed() {
		log.Printf("[%s] Channel is closed or nil. Reopening channel...", p.name)
		ch, err := p.conn.OpenChannel()
		if err != nil {
			log.Printf("[%s] Failed to open channel: %v. Retrying...", p.name, err)
			return err
		}
		p.channel = ch
		log.Printf("[%s] Channel reopened", p.name)
	}

	return nil
}

// Close gracefully shuts down the producer
func (p *Producer) Close() {
	if p.channel != nil {
		p.channel.Close()
	}
	if p.conn != nil {
		p.conn.Close()
	}
}
