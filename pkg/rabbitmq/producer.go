package rabbitmq

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer struct {
	conn        *Connection
	channel     *amqp.Channel
	queueName   string
	name        string
	queueConfig QueueOptions
}

// NewProducer initializes a new Producer instance and declares the queue with the specified options
func NewProducer(conn *Connection, name, queue string, options QueueOptions) (*Producer, error) {
	ch, err := conn.OpenChannel() // Use the OpenChannel method which ensures auto-reconnection
	if err != nil {
		log.Printf("[%s] Failed to open a channel", name)
		return nil, err
	}

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

	return &Producer{
		conn:        conn,
		channel:     ch,
		queueName:   queue,
		name:        name,
		queueConfig: options,
	}, nil
}

// Publish sends a message to the queue with retry logic and context support
func (p *Producer) Publish(ctx context.Context, message []byte) error {
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
			log.Printf("[%s] Connection or channel issue: %v. Retrying in %v...", p.name, err, retryDuration)
			time.Sleep(retryDuration)
			continue
		}

		// Attempt to publish the message
		err = p.channel.PublishWithContext(
			ctx,
			"",          // exchange
			p.queueName, // routing key
			false,       // mandatory
			false,       // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        message,
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
