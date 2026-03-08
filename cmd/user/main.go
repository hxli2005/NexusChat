package main

import (
	"context"
	"errors"
	"log"
	"net"
	"os"

	userv1 "github.com/lhx/nexuschat/api/user/v1"
	userrepo "github.com/lhx/nexuschat/internal/user/repository"
	usersvc "github.com/lhx/nexuschat/internal/user/service"
	"github.com/lhx/nexuschat/pkg/snowflake"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type userGRPCServer struct {
	userv1.UnimplementedUserServiceServer
	svc *usersvc.Service
}

func (s *userGRPCServer) Register(ctx context.Context, req *userv1.RegisterRequest) (*userv1.RegisterResponse, error) {
	userID, err := s.svc.Register(ctx, usersvc.RegisterInput{
		Username: req.GetUsername(),
		Password: req.GetPassword(),
		Nickname: req.GetNickname(),
	})
	if err != nil {
		switch {
		case errors.Is(err, usersvc.ErrInvalidInput):
			return nil, status.Error(codes.InvalidArgument, err.Error())
		case errors.Is(err, usersvc.ErrUserExists):
			return nil, status.Error(codes.AlreadyExists, err.Error())
		default:
			return nil, status.Error(codes.Internal, "register failed")
		}
	}
	return &userv1.RegisterResponse{UserId: userID}, nil
}

func (s *userGRPCServer) Login(ctx context.Context, req *userv1.LoginRequest) (*userv1.LoginResponse, error) {
	token, userID, err := s.svc.Login(ctx, usersvc.LoginInput{Username: req.GetUsername(), Password: req.GetPassword()})
	if err != nil {
		switch {
		case errors.Is(err, usersvc.ErrInvalidInput):
			return nil, status.Error(codes.InvalidArgument, err.Error())
		case errors.Is(err, usersvc.ErrInvalidCreds):
			return nil, status.Error(codes.Unauthenticated, err.Error())
		default:
			return nil, status.Error(codes.Internal, "login failed")
		}
	}
	return &userv1.LoginResponse{Token: token, UserId: userID}, nil
}

func (s *userGRPCServer) CheckFriendship(ctx context.Context, req *userv1.CheckFriendshipRequest) (*userv1.CheckFriendshipResponse, error) {
	ok, err := s.svc.CheckFriendship(ctx, req.GetUserIdA(), req.GetUserIdB())
	if err != nil {
		return nil, status.Error(codes.Internal, "check friendship failed")
	}
	return &userv1.CheckFriendshipResponse{IsFriend: ok}, nil
}

func (s *userGRPCServer) CheckMembership(ctx context.Context, req *userv1.CheckMembershipRequest) (*userv1.CheckMembershipResponse, error) {
	ok, err := s.svc.CheckMembership(ctx, req.GetUserId(), req.GetConversationId())
	if err != nil {
		return nil, status.Error(codes.Internal, "check membership failed")
	}
	return &userv1.CheckMembershipResponse{IsMember: ok}, nil
}

func (s *userGRPCServer) GetConversationMembers(ctx context.Context, req *userv1.GetConversationMembersRequest) (*userv1.GetConversationMembersResponse, error) {
	ids, err := s.svc.GetConversationMembers(ctx, req.GetConversationId())
	if err != nil {
		return nil, status.Error(codes.Internal, "get members failed")
	}
	return &userv1.GetConversationMembersResponse{UserIds: ids}, nil
}

func main() {
	port := os.Getenv("USER_GRPC_PORT")
	if port == "" {
		port = "50051"
	}
	mysqlDSN := os.Getenv("MYSQL_DSN")
	if mysqlDSN == "" {
		mysqlDSN = "root:nexuschat@tcp(localhost:3306)/nexuschat?parseTime=true"
	}

	idGen, err := snowflake.NewFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	repo, err := userrepo.New(mysqlDSN)
	if err != nil {
		log.Fatal(err)
	}
	svc := usersvc.New(repo, idGen)

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}

	s := grpc.NewServer()
	userv1.RegisterUserServiceServer(s, &userGRPCServer{svc: svc})
	log.Printf("user service grpc listening on :%s", port)
	if err := s.Serve(lis); err != nil {
		log.Fatal(err)
	}
}
