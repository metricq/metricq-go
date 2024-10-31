package metricq

import (
	"context"
	"fmt"
	"log"
	"net/url"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Connection struct {
	connection *amqp.Connection
	channel    *amqp.Channel
}

// Connect establishes the connection to the AMQP server and sets up a Channel.
// Pass in the URL and a name for the connection.
// It returns any errors
func (con *Connection) Connect(url *url.URL, name string) error {
	table := amqp.Table{}
	table.SetClientConnectionName(name)
	config := amqp.Config{Properties: table}

	connection, err := amqp.DialConfig(url.String(), config)
	if err != nil {
		return fmt.Errorf("failed to connect (%s): %w", url.Redacted(), err)
	}

	con.connection = connection

	channel, err := connection.Channel()
	if err != nil {
		return fmt.Errorf("failed to create connect channel: %w", err)
	}

    errorHandler := make(chan *amqp.Error)

    channel.NotifyClose(errorHandler)

    go func() {
        select {
        case err := <- errorHandler:
            log.Printf("Channel closed: %s", err)
        }
    }()

	con.channel = channel

	return nil
}

func (con *Connection) Publish(ctx context.Context, exchange string, routingKey string, payload []byte, contentType string) {

	message := amqp.Publishing{
		Headers:         amqp.Table{},
		ContentType:     contentType,
		ContentEncoding: "",
		DeliveryMode:    amqp.Persistent,
		Priority:        0,
		AppId:           "",
		Body:            payload,
	}

	err := con.channel.PublishWithContext(ctx, exchange, routingKey, true, true, message)
	if err != nil {
		log.Panicf("Failed to publish message: %s", err)
	}
}

func (con *Connection) Close() {
	if con.connection != nil {
		con.connection.Close()
	}
}
