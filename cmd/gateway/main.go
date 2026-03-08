package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lhx/nexuschat/internal/gateway/client"
	"github.com/lhx/nexuschat/internal/gateway/handler"
	"github.com/lhx/nexuschat/internal/gateway/hub"
	"github.com/lhx/nexuschat/internal/gateway/producer"
	"github.com/lhx/nexuschat/pkg/kafka"
	"github.com/lhx/nexuschat/pkg/model"
	"github.com/lhx/nexuschat/pkg/redisclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	port := os.Getenv("GATEWAY_PORT")
	if port == "" {
		port = "8080"
	}
	userServiceAddr := os.Getenv("USER_SERVICE_ADDR")
	if userServiceAddr == "" {
		userServiceAddr = "localhost:50051"
	}
	messageServiceAddr := os.Getenv("MESSAGE_SERVICE_ADDR")
	if messageServiceAddr == "" {
		messageServiceAddr = "localhost:50052"
	}
	userConn, err := grpc.NewClient(userServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = userConn.Close() }()
	messageConn, err := grpc.NewClient(messageServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = messageConn.Close() }()

	userClient := client.NewUserClient(userConn)
	messageClient := client.NewMessageClient(messageConn)
	connHub := hub.New()

	var incomingProducer *producer.IncomingProducer
	brokers := kafka.ParseBrokers(os.Getenv("KAFKA_BROKERS"))
	incomingProducer = producer.NewIncomingProducer(brokers)
	defer func() {
		if incomingProducer != nil {
			_ = incomingProducer.Close()
		}
	}()

	redisAddr := os.Getenv("REDIS_ADDR")
	redisCli := redisclient.New(redisAddr)
	outgoingConsumer := kafka.NewConsumer(brokers, "chat.msg.outgoing", "gateway-"+port)
	defer func() {
		_ = outgoingConsumer.Close()
	}()
	go consumeOutgoing(ctx, outgoingConsumer, connHub, userClient)

	r := handler.NewRouter(handler.RouterDeps{
		UserClient:    userClient,
		MessageClient: messageClient,
		Hub:           connHub,
		Incoming:      incomingProducer,
		Redis:         redisCli,
		SelfAddr:      os.Getenv("SELF_ADDR"),
	})
	log.Printf("gateway starting on :%s", port)
	if err := r.Run(":" + port); err != nil {
		log.Fatal(err)
	}
}

func consumeOutgoing(ctx context.Context, consumer *kafka.Consumer, connHub *hub.ConnHub, userClient *client.UserClient) {
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
			log.Printf("invalid outgoing payload: %v", err)
			_ = consumer.Commit(ctx, msg)
			continue
		}

		members, err := userClient.GetConversationMembers(ctx, out.ConversationID)
		if err != nil {
			log.Printf("query members failed: %v", err)
			continue
		}
		if len(members) == 0 {
			members = []int64{out.SenderID}
		}
		for _, uid := range members {
			_ = connHub.Push(uid, msg.Value)
		}

		if err := consumer.Commit(ctx, msg); err != nil {
			log.Printf("commit outgoing failed: %v", err)
		}
	}
}
