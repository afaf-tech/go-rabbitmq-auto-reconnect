package rabbitmq

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer struct {
	conn      *Connection
	channel   *amqp.Channel
	queueName string
	name      string
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

type PublishOptions struct {
	Exchange    string     // The exchange to publish the message to
	RoutingKey  string     // The routing key to use for the message
	ContentType string     // The content type of the message (e.g., "text/plain")
	Body        []byte     // The actual message body
	Mandatory   bool       // Whether the message is mandatory
	Immediate   bool       // Whether the message is immediate
	Headers     amqp.Table // Optional headers for the message
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
