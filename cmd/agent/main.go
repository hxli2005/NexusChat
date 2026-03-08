package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lhx/nexuschat/internal/agent/detector"
	"github.com/lhx/nexuschat/internal/gateway/producer"
	"github.com/lhx/nexuschat/pkg/kafka"
	"github.com/lhx/nexuschat/pkg/model"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	agentBotID, _ := strconv.ParseInt(os.Getenv("AGENT_BOT_USER_ID"), 10, 64)
	if agentBotID == 0 {
		agentBotID = 1
	}

	brokers := kafka.ParseBrokers(os.Getenv("KAFKA_BROKERS"))
	consumer := kafka.NewConsumer(brokers, "chat.msg.outgoing", "agent-worker")
	defer func() { _ = consumer.Close() }()
	incomingProducer := producer.NewIncomingProducer(brokers)
	defer func() { _ = incomingProducer.Close() }()

	log.Println("agent worker started")
	for {
		if ctx.Err() != nil {
			return
		}

		msg, err := consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("fetch outgoing failed: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		var out model.OutgoingMessage
		if err := json.Unmarshal(msg.Value, &out); err != nil {
			_ = consumer.Commit(ctx, msg)
			continue
		}
		if out.SenderID == agentBotID {
			_ = consumer.Commit(ctx, msg)
			continue
		}
		if !detector.IsAITriggered(out.Content, false) {
			_ = consumer.Commit(ctx, msg)
			continue
		}

		reply := buildReply(out.Content)
		in := model.IncomingMessage{
			TempID:         strconv.FormatInt(time.Now().UnixNano(), 10),
			SenderID:       agentBotID,
			ConversationID: out.ConversationID,
			Content:        reply,
			SentAt:         time.Now().UnixMilli(),
		}
		if err := incomingProducer.Send(ctx, in); err != nil {
			log.Printf("send ai reply failed: %v", err)
			continue
		}
		if err := consumer.Commit(ctx, msg); err != nil {
			log.Printf("commit agent offset failed: %v", err)
		}
	}
}

func buildReply(content string) string {
	trimmed := strings.TrimSpace(strings.ReplaceAll(content, "@AI", ""))
	if trimmed == "" {
		return "I am NexusChat AI assistant. Ask me anything."
	}
	return "AI reply: " + trimmed
}
