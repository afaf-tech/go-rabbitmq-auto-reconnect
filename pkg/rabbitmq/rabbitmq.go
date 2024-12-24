package rabbitmq

import (
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	defaultRetryDuration = time.Second * 5
	defaultURI           = "amqp://guest:guest@localhost:5672/" // Default RabbitMQ URI
	defaultMaxChannels   = 10                                   // Default maximum number of channels
)

// TODO: logger interface config
// Config holds the configuration for connecting to RabbitMQ
type Config struct {
	URI           string        // URI for RabbitMQ connection (e.g., "amqp://guest:guest@localhost:5672/")
	RetryDuration time.Duration // Time to wait before retrying a failed connection attempt
	AMQPConfig    *amqp.Config  // AMQP-specific configuration, can be nil to use defaults
	MaxChannels   int           // Maximum number of channels allowed (default is 10)
}

// Connection wraps the actual AMQP connection and provides reconnection logic
type Connection struct {
	*amqp.Connection
	config       *Config           // Custom configuration for the connection
	mu           sync.RWMutex      // Mutex to synchronize access to the connection
	reconnecting bool              // Flag to indicate ongoing reconnection
	channels     [](*amqp.Channel) // Slice to hold active channels
}

// QueueOptions defines RabbitMQ queue configurations
type QueueOptions struct {
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
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

// OpenChannel opens a new channel with retry logic, ensuring it doesn't exceed MaxChannels
func (c *Connection) OpenChannel() (*amqp.Channel, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.channels) >= c.config.MaxChannels {
		return nil, fmt.Errorf("maximum number of channels (%d) reached", c.config.MaxChannels)
	}

	var channel *amqp.Channel
	var err error
	retryDuration := c.config.RetryDuration

	for attempts := 0; attempts < 5; attempts++ { // Retry up to 5 times, for example
		channel, err = c.Connection.Channel()
		if err == nil {
			// Add the new channel to the list of channels
			c.channels = append(c.channels, channel)
			return channel, nil
		}

		log.Printf("Failed to open channel: %v. Retrying in %v...", err, retryDuration)
		time.Sleep(retryDuration)
	}

	return nil, fmt.Errorf("failed to open channel after retries: %w", err)
}

// ReconnectChannel attempts to reconnect a channel if it's lost
func (c *Connection) ReconnectChannel(channel *amqp.Channel) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Logic for determining if the channel is closed or broken (could use specific error types)
	if channel != nil && channel.IsClosed() {
		log.Printf("Channel is closed, attempting to reopen it...")
		// Try to open a new channel
		newChannel, err := c.OpenChannel()
		if err != nil {
			return fmt.Errorf("failed to reconnect channel: %w", err)
		}

		// Replace the closed channel with the new one
		for i, ch := range c.channels {
			if ch == channel {
				c.channels[i] = newChannel
				break
			}
		}

		log.Println("Successfully reconnected channel.")
		return nil
	}

	return nil
}

// failOnError logs an error and exits the application if the error is not nil
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
