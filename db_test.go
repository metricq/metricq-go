package metricq

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

type dbAck struct{ ack chan struct{} }

func (a dbAck) Ack(uint64, bool) error        { close(a.ack); return nil }
func (a dbAck) Nack(uint64, bool, bool) error { return nil }
func (a dbAck) Reject(uint64, bool) error     { return nil }
func TestDBAckOnlyAfterDurability(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ack := dbAck{make(chan struct{})}
	messages := make(chan amqp.Delivery, 1)
	b, _ := proto.Marshal(&DataChunk{TimeDelta: []int64{100}, Value: []float64{2}})
	messages <- amqp.Delivery{Acknowledger: ack, Body: b, RoutingKey: "x"}
	entered := make(chan struct{})
	durable := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- consumeDBData(ctx, messages, func(context.Context, string, *DataChunk) error { close(entered); <-durable; return nil })
	}()
	<-entered
	select {
	case <-ack.ack:
		t.Fatal("ACK before durability")
	default:
	}
	close(durable)
	select {
	case <-ack.ack:
	case <-time.After(time.Second):
		t.Fatal("no ACK after durability")
	}
	cancel()
	<-done
}
func TestDBFailureDoesNotAck(t *testing.T) {
	ack := dbAck{make(chan struct{})}
	messages := make(chan amqp.Delivery, 1)
	b, _ := proto.Marshal(&DataChunk{TimeDelta: []int64{100}, Value: []float64{2}})
	messages <- amqp.Delivery{Acknowledger: ack, Body: b}
	err := consumeDBData(context.Background(), messages, func(context.Context, string, *DataChunk) error { return errors.New("disk full") })
	if err == nil {
		t.Fatal("lost handler error")
	}
	select {
	case <-ack.ack:
		t.Fatal("ACK despite failure")
	default:
	}
}
func TestDBRegistration(t *testing.T) {
	for _, raw := range []string{`{"dataServerAddress":"vhost:/data","dataQueue":"data","historyQueue":"history","config":{"metrics":{}}}`, `{"dataServerAddress":"vhost:/data","dataQueue":"data","historyQueue":"history","config":"{\"metrics\":{}}"}`} {
		r, err := decodeDBRegistration([]byte(raw))
		if err != nil || string(r.Config) != `{"metrics":{}}` {
			t.Fatalf("%+v %v", r, err)
		}
	}
	if _, err := decodeDBRegistration([]byte(`{"error":"no config"}`)); err == nil {
		t.Fatal("ignored manager error")
	}
}
