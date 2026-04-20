package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/avinash-gupta-rdz/rdstail/internal/config"
	kafkasink "github.com/avinash-gupta-rdz/rdstail/internal/sink/kafka"
	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

type mockProducer struct {
	records []*kgo.Record
	err     error
	closed  bool
}

func (m *mockProducer) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	results := make(kgo.ProduceResults, 0, len(rs))
	for _, r := range rs {
		m.records = append(m.records, r)
		results = append(results, kgo.ProduceResult{Record: r, Err: m.err})
	}
	return results
}

func (m *mockProducer) Close() { m.closed = true }

func TestWrite_SetsTopicKeyHeaders(t *testing.T) {
	m := &mockProducer{}
	s, err := kafkasink.New(kafkasink.Opts{
		Name:     "kafka",
		Cfg:      &config.KafkaSink{Brokers: []string{"localhost:9092"}, Topic: "rds-logs"},
		Producer: m,
	})
	if err != nil {
		t.Fatal(err)
	}
	recs := []logrecord.LogRecord{
		{InstanceID: "db-1", Engine: "postgres", LogFile: "pg.log", Message: "m1", BatchID: "b1"},
		{InstanceID: "db-1", Engine: "postgres", LogFile: "pg.log", Message: "m2", BatchID: "b1"},
	}
	if err := s.Write(context.Background(), recs); err != nil {
		t.Fatal(err)
	}
	if len(m.records) != 2 {
		t.Fatalf("expected 2 msgs, got %d", len(m.records))
	}
	if m.records[0].Topic != "rds-logs" {
		t.Fatalf("bad topic: %q", m.records[0].Topic)
	}
	if string(m.records[0].Key) != "db-1|pg.log" {
		t.Fatalf("bad key: %q", m.records[0].Key)
	}
	var got logrecord.LogRecord
	if err := json.Unmarshal(m.records[0].Value, &got); err != nil {
		t.Fatalf("value not JSON: %v", err)
	}
	if got.Message != "m1" {
		t.Fatalf("bad value: %+v", got)
	}
	// header check
	var foundBatch bool
	for _, h := range m.records[0].Headers {
		if h.Key == "batch-id" && string(h.Value) == "b1" {
			foundBatch = true
		}
	}
	if !foundBatch {
		t.Fatal("batch-id header missing")
	}
}

func TestWrite_TopicTemplate(t *testing.T) {
	m := &mockProducer{}
	s, _ := kafkasink.New(kafkasink.Opts{
		Name:     "k",
		Cfg:      &config.KafkaSink{Brokers: []string{"b"}, TopicTemplate: "rds-logs-{engine}"},
		Producer: m,
	})
	_ = s.Write(context.Background(), []logrecord.LogRecord{
		{InstanceID: "db-1", Engine: "mysql", LogFile: "f", Message: "m"},
	})
	if m.records[0].Topic != "rds-logs-mysql" {
		t.Fatalf("template not applied: %q", m.records[0].Topic)
	}
}

func TestWrite_ProducerErr_Propagates(t *testing.T) {
	m := &mockProducer{err: errors.New("broker down")}
	s, _ := kafkasink.New(kafkasink.Opts{
		Name:     "k",
		Cfg:      &config.KafkaSink{Brokers: []string{"b"}, Topic: "t"},
		Producer: m,
	})
	err := s.Write(context.Background(), []logrecord.LogRecord{{InstanceID: "i", LogFile: "f", Message: "m"}})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestClose_ClosesProducer(t *testing.T) {
	m := &mockProducer{}
	s, _ := kafkasink.New(kafkasink.Opts{
		Name:     "k",
		Cfg:      &config.KafkaSink{Brokers: []string{"b"}, Topic: "t"},
		Producer: m,
	})
	_ = s.Close()
	if !m.closed {
		t.Fatal("producer not closed")
	}
}
