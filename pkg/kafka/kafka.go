package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafkago.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafkago.Writer{
			Addr:         kafkago.TCP(brokers...),
			Topic:        topic,
			RequiredAcks: kafkago.RequireAll,
			BatchTimeout: 10 * time.Millisecond,
			Async:        false,
		},
	}
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

func (p *Producer) ProduceJSON(ctx context.Context, key int64, payload any) error {
	bytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	msg := kafkago.Message{
		Key:   []byte(fmt.Sprintf("%d", key)),
		Value: bytes,
	}
	return p.writer.WriteMessages(ctx, msg)
}

type Consumer struct {
	reader *kafkago.Reader
}

func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	cfg := kafkago.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		CommitInterval: 0,
		MinBytes:       1,
		MaxBytes:       10e6,
	}
	return &Consumer{reader: kafkago.NewReader(cfg)}
}

func (c *Consumer) Fetch(ctx context.Context) (kafkago.Message, error) {
	return c.reader.FetchMessage(ctx)
}

func (c *Consumer) Commit(ctx context.Context, msg kafkago.Message) error {
	return c.reader.CommitMessages(ctx, msg)
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

func ParseBrokers(raw string) []string {
	if raw == "" {
		return []string{"localhost:9092"}
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	if len(out) == 0 {
		return []string{"localhost:9092"}
	}
	return out
}
