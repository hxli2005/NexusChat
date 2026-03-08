package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	messagev1 "github.com/lhx/nexuschat/api/message/v1"
	userv1 "github.com/lhx/nexuschat/api/user/v1"
	msgrepo "github.com/lhx/nexuschat/internal/message/repository"
	msgsvc "github.com/lhx/nexuschat/internal/message/service"
	"github.com/lhx/nexuschat/pkg/kafka"
	"github.com/lhx/nexuschat/pkg/model"
	"github.com/lhx/nexuschat/pkg/redisclient"
	"github.com/lhx/nexuschat/pkg/snowflake"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type membershipChecker struct {
	cli userv1.UserServiceClient
}

func (m *membershipChecker) CheckMembership(ctx context.Context, userID, conversationID int64) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	resp, err := m.cli.CheckMembership(ctx, &userv1.CheckMembershipRequest{
		UserId:         userID,
		ConversationId: conversationID,
	})
	if err != nil {
		return false, err
	}
	return resp.GetIsMember(), nil
}

type messageGRPCServer struct {
	messagev1.UnimplementedMessageServiceServer
	svc *msgsvc.Service
}

func (s *messageGRPCServer) GetHistory(ctx context.Context, req *messagev1.GetHistoryRequest) (*messagev1.GetHistoryResponse, error) {
	rows, err := s.svc.GetHistory(ctx, req.GetConversationId(), req.GetBeforeMsgId(), int(req.GetLimit()))
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to query history")
	}
	out := &messagev1.GetHistoryResponse{Messages: make([]*messagev1.MessageRecord, 0, len(rows))}
	for _, row := range rows {
		out.Messages = append(out.Messages, &messagev1.MessageRecord{
			MsgId:          row.MsgID,
			SenderId:       row.SenderID,
			ConversationId: row.ConversationID,
			Content:        row.Content,
			CreatedAt:      row.CreatedAt,
			IsAi:           row.IsAI,
		})
	}
	return out, nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	port := os.Getenv("MESSAGE_GRPC_PORT")
	if port == "" {
		port = "50052"
	}
	mysqlDSN := os.Getenv("MYSQL_DSN")
	if mysqlDSN == "" {
		mysqlDSN = "root:nexuschat@tcp(localhost:3306)/nexuschat?parseTime=true"
	}
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	userServiceAddr := os.Getenv("USER_SERVICE_ADDR")
	if userServiceAddr == "" {
		userServiceAddr = "localhost:50051"
	}
	agentBotID, _ := strconv.ParseInt(os.Getenv("AGENT_BOT_USER_ID"), 10, 64)

	idGen, err := snowflake.NewFromEnv()
	if err != nil {
		log.Fatal(err)
	}

	repo, err := msgrepo.New(mysqlDSN)
	if err != nil {
		log.Fatal(err)
	}
	userConn, err := grpc.NewClient(userServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = userConn.Close() }()
	checker := &membershipChecker{cli: userv1.NewUserServiceClient(userConn)}

	redisCli := redisclient.New(redisAddr)
	outgoingProducer := kafka.NewProducer(kafka.ParseBrokers(os.Getenv("KAFKA_BROKERS")), "chat.msg.outgoing")
	defer func() {
		_ = outgoingProducer.Close()
	}()
	service := msgsvc.New(repo, idGen, outgoingProducer, redisCli, agentBotID, checker)

	brokers := kafka.ParseBrokers(os.Getenv("KAFKA_BROKERS"))
	consumer := kafka.NewConsumer(brokers, "chat.msg.incoming", "message-service")
	defer func() {
		_ = consumer.Close()
	}()

	go consumeIncoming(ctx, consumer, service)

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}

	s := grpc.NewServer()
	messagev1.RegisterMessageServiceServer(s, &messageGRPCServer{svc: service})
	log.Printf("message service grpc listening on :%s", port)
	go func() {
		<-ctx.Done()
		s.GracefulStop()
	}()

	if err := s.Serve(lis); err != nil {
		log.Fatal(err)
	}
}

func consumeIncoming(ctx context.Context, consumer *kafka.Consumer, service *msgsvc.Service) {
	for {
		if ctx.Err() != nil {
			return
		}

		msg, err := consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("fetch incoming failed: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		var incoming model.IncomingMessage
		if err := json.Unmarshal(msg.Value, &incoming); err != nil {
			log.Printf("invalid incoming payload, skip commit: %v", err)
			if err := consumer.Commit(ctx, msg); err != nil {
				log.Printf("commit invalid payload offset failed: %v", err)
			}
			continue
		}

		if err := service.HandleIncoming(ctx, incoming); err != nil {
			log.Printf("handle incoming failed, will retry: %v", err)
			continue
		}

		if err := consumer.Commit(ctx, msg); err != nil {
			log.Printf("commit offset failed: %v", err)
		}
	}
}
