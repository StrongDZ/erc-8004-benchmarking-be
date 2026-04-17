package rabbitmq

// client.go — RabbitMQ connection factory
// Provides an *amqp.Connection for use in cmd/ wiring.

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// NewConn dials a RabbitMQ broker and returns a ready-to-use connection.
// Caller is responsible for calling conn.Close().
func NewConn(uri string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(uri)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq dial %s: %w", uri, err)
	}
	return conn, nil
}
