package producer

import (
	"context"

	"github.com/lhx/nexuschat/pkg/kafka"
	"github.com/lhx/nexuschat/pkg/model"
)

type IncomingProducer struct {
	producer *kafka.Producer
}

func NewIncomingProducer(brokers []string) *IncomingProducer {
	return &IncomingProducer{producer: kafka.NewProducer(brokers, "chat.msg.incoming")}
}

func (p *IncomingProducer) Send(ctx context.Context, msg model.IncomingMessage) error {
	return p.producer.ProduceJSON(ctx, msg.ConversationID, msg)
}

func (p *IncomingProducer) Close() error {
	return p.producer.Close()
}
