package metrigo

import (
	"context"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Connection struct {
	exchange   string
	connection *amqp.Connection
	channel    *amqp.Channel
}

func (con *Connection) Connect(url string) {
	table := amqp.Table{}
	table.SetClientConnectionName("go connection name")
	config := amqp.Config{Properties: table}

	connection, err := amqp.DialConfig(url, config)
	if err != nil {
		log.Panicf("%s (%s): %s", "Failed to connect to MetricQ Server", url, err)
	}

	con.connection = connection

	channel, err := connection.Channel()
	if err != nil {
		log.Panicf("%s: %s", "Failed to create connect channel", err)
	}

	con.channel = channel
}

func (con *Connection) Publish(ctx context.Context, routingKey string, payload []byte) {

	message := amqp.Publishing{
		Headers:         amqp.Table{},
		ContentType:     "text/plain",
		ContentEncoding: "",
		DeliveryMode:    amqp.Persistent,
		Priority:        0,
		AppId:           "",
		Body:            payload,
	}

	err := con.channel.PublishWithContext(ctx, con.exchange, routingKey, true, true, message)
	if err != nil {
		log.Panicf("Failed to publish message: %s", err)
	}
}

func (con *Connection) Close() {
	if con.connection != nil {
		con.connection.Close()
	}
}
