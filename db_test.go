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

type recordingAck struct {
	acks chan [2]uint64
}

func (a recordingAck) Ack(tag uint64, multiple bool) error {
	m := uint64(0)
	if multiple {
		m = 1
	}
	a.acks <- [2]uint64{tag, m}
	return nil
}
func (a recordingAck) Nack(uint64, bool, bool) error { return nil }
func (a recordingAck) Reject(uint64, bool) error     { return nil }

func TestDBDataBatchAcksBufferedDeliveriesOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ack := recordingAck{make(chan [2]uint64, 8)}
	messages := make(chan amqp.Delivery, 8)
	for i := uint64(1); i <= 5; i++ {
		b, _ := proto.Marshal(&DataChunk{TimeDelta: []int64{int64(i)}, Value: []float64{float64(i)}})
		messages <- amqp.Delivery{Acknowledger: ack, Body: b, RoutingKey: "x", DeliveryTag: i}
	}
	batches := make(chan []DataMessage, 4)
	durable := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- consumeDBDataBatches(ctx, messages, 3, func(_ context.Context, m []DataMessage) error {
			batches <- m
			<-durable
			return nil
		})
	}()
	first := <-batches
	if len(first) != 3 || first[0].Input != "x" || first[2].Chunk.Value[0] != 3 {
		t.Fatalf("first batch %+v", first)
	}
	select {
	case a := <-ack.acks:
		t.Fatalf("ACK %v before durability", a)
	case <-time.After(20 * time.Millisecond):
	}
	durable <- struct{}{}
	if a := <-ack.acks; a != [2]uint64{3, 1} {
		t.Fatalf("first ACK %v", a)
	}
	second := <-batches
	if len(second) != 2 {
		t.Fatalf("second batch has %d deliveries", len(second))
	}
	durable <- struct{}{}
	if a := <-ack.acks; a != [2]uint64{5, 1} {
		t.Fatalf("second ACK %v", a)
	}
	cancel()
	<-done
}

func TestDBDataBatchFailureDoesNotAck(t *testing.T) {
	ack := recordingAck{make(chan [2]uint64, 4)}
	messages := make(chan amqp.Delivery, 2)
	for i := uint64(1); i <= 2; i++ {
		b, _ := proto.Marshal(&DataChunk{TimeDelta: []int64{100}, Value: []float64{2}})
		messages <- amqp.Delivery{Acknowledger: ack, Body: b, DeliveryTag: i}
	}
	err := consumeDBDataBatches(context.Background(), messages, 8, func(context.Context, []DataMessage) error { return errors.New("disk full") })
	if err == nil {
		t.Fatal("lost handler error")
	}
	select {
	case a := <-ack.acks:
		t.Fatalf("ACK %v despite failure", a)
	default:
	}
}
