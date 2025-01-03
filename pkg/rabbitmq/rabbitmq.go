package rabbitmq

import (
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// QueueOptions represents the configuration options for declaring a queue in RabbitMQ.
type QueueOptions struct {
	// Durable specifies whether the queue should survive broker restarts. If true, the queue will
	// be durable, meaning it will persist even if RabbitMQ crashes or restarts.
	Durable bool

	// AutoAck enables or disables automatic message acknowledgment. If true, messages are automatically
	// acknowledged by the broker once they are received by the consumer.
	AutoAck bool

	// AutoDelete determines whether the queue should be automatically deleted when no consumers are
	// connected. If true, the queue will be deleted when the last consumer disconnects.
	AutoDelete bool

	// Exclusive makes the queue private to the connection that created it. If true, the queue can only
	// be used by the connection that declared it and will be deleted once that connection closes.
	Exclusive bool

	// NoWait prevents the server from sending a response to the queue declaration. If true, the declaration
	// will not wait for an acknowledgment from the server and no error will be returned if the queue already exists.
	NoWait bool

	// NoLocal prevents the delivery of messages to the connection that published them. If true, messages
	// will not be delivered to the connection that created the queue.
	NoLocal bool

	// Args allows additional arguments to be passed when declaring the queue. This can be used for advanced
	// RabbitMQ configurations, such as setting arguments for policies or defining queue TTLs (Time-To-Live).
	Args amqp.Table
}

const (
	defaultRetryDuration = time.Second * 5
	defaultURI           = "amqp://guest:guest@localhost:5672/" // Default RabbitMQ URI
	defaultMaxChannels   = 10                                   // Default maximum number of channels
)

// Config holds the configuration options for establishing a RabbitMQ connection and managing
// consumer channels.
type Config struct {
	// URI is the connection string for RabbitMQ. It should follow the AMQP URI format, e.g.,
	// "amqp://guest:guest@localhost:5672/". It is used to establish the connection to the RabbitMQ broker.
	URI string

	// RetryDuration specifies the time duration to wait before retrying a failed connection attempt.
	// This is useful to implement a backoff strategy in case of temporary network issues.
	RetryDuration time.Duration

	// AMQPConfig holds AMQP-specific configuration options. If nil, default AMQP configurations
	// are used. This can include settings like heartbeat intervals and other advanced AMQP features.
	AMQPConfig *amqp.Config

	// MaxChannels defines the maximum number of channels that can be opened to the RabbitMQ server.
	// If the value is 0 or negative, the default is used (which may be 10 channels).
	MaxChannels int
}

// Connection wraps the actual AMQP connection and provides reconnection logic, ensuring
// that the connection and channels remain active or are re-established when necessary.
type Connection struct {
	// Connection is the underlying AMQP connection provided by the AMQP client library.
	// It provides the basic connection functionalities such as opening channels, closing the connection, etc.
	*amqp.Connection

	// config holds the custom configuration settings for the RabbitMQ connection, such as URI, retry duration, etc.
	// These settings are used for connection management and reconnection attempts.
	config *Config

	// mu is a read-write mutex used to synchronize access to the connection and channels,
	// ensuring thread-safe operations for shared resources.
	mu sync.RWMutex

	// reconnecting is a flag indicating whether a reconnection attempt is currently in progress.
	// This is used to prevent multiple concurrent reconnection attempts.
	reconnecting bool

	// channels holds a slice of active channels associated with the current connection.
	// Channels are used to send and receive messages within a specific connection context.
	channels []*amqp.Channel
}

// NewConnection creates a new Connection object and initializes it with the provided configuration
func NewConnection(config *Config) *Connection {
	// Use default values if not provided
	if config.URI == "" {
		config.URI = defaultURI
	}
	if config.RetryDuration == 0 {
		config.RetryDuration = defaultRetryDuration
	}
	if config.AMQPConfig == nil {
		config.AMQPConfig = &amqp.Config{} // Use default AMQP config if none is provided
	}
	if config.MaxChannels == 0 {
		config.MaxChannels = defaultMaxChannels
	}

	return &Connection{
		config:   config,
		channels: make([]*amqp.Channel, 0, config.MaxChannels), // Pre-allocate slice for channels
	}
}

// Connect establishes the RabbitMQ connection with automatic retry on failure
func (c *Connection) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.reconnecting {
		return nil // Skip if already reconnecting
	}

	for {
		conn, err := amqp.DialConfig(c.config.URI, *c.config.AMQPConfig)
		if err == nil {
			log.Println("Connected to RabbitMQ")
			c.Connection = conn
			c.reconnecting = false
			return nil
		}

		// Handle AMQP-specific errors or other types of errors
		if amqpErr, ok := err.(*amqp.Error); ok {
			log.Printf("AMQP Error: Code=%d, Reason=%s. Retrying...", amqpErr.Code, amqpErr.Reason)
		} else {
			log.Printf("Failed to connect to RabbitMQ: %v. Retrying in 5 seconds.", err)
		}

		c.reconnecting = true
		time.Sleep(c.config.RetryDuration) // Retry after a delay
	}
}

// IsClosed checks if the RabbitMQ connection is closed
func (c *Connection) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.Connection == nil || c.Connection.IsClosed()
}

// Close closes the RabbitMQ connection and all open channels
func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.Connection != nil {
		// Close all channels before closing the connection
		for _, ch := range c.channels {
			if ch != nil {
				_ = ch.Close()
			}
		}
		c.Connection = nil
		return c.Connection.Close()
	}
	return nil
}

// Reconnect attempts to reconnect if the connection was lost
func (c *Connection) Reconnect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.reconnecting {
		log.Println("Already in the process of reconnecting. Skipping.")
		return nil
	}

	c.reconnecting = true

	conn, err := amqp.DialConfig(c.config.URI, *c.config.AMQPConfig)
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

// OpenChannel opens a new channel with retry logic, ensuring it doesn't exceed MaxChannels
func (c *Connection) OpenChannel() (*amqp.Channel, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Remove any closed channels from the active list
	c.cleanupClosedChannels()

	if len(c.channels) >= c.config.MaxChannels {
		return nil, fmt.Errorf("maximum number of channels (%d) reached", c.config.MaxChannels)
	}

	var channel *amqp.Channel
	channel, err := c.Connection.Channel()
	if err == nil {
		// Add the new channel to the list of channels
		c.channels = append(c.channels, channel)
		return channel, nil
	}

	log.Printf("Failed to open channel: %v. ", err)

	return nil, fmt.Errorf("failed to open channel after retries: %w", err)
}

func (c *Connection) cleanupClosedChannels() {
	var openChannels []*amqp.Channel
	for _, ch := range c.channels {
		if ch != nil && !ch.IsClosed() {
			openChannels = append(openChannels, ch)
		} else if ch != nil {
			_ = ch.Close() // remove closed channel
		}
	}
	c.channels = openChannels
}
