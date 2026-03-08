package client

import (
	"context"

	userv1 "github.com/lhx/nexuschat/api/user/v1"
	"google.golang.org/grpc"
)

type UserClient struct {
	cli userv1.UserServiceClient
}

func NewUserClient(conn *grpc.ClientConn) *UserClient {
	return &UserClient{cli: userv1.NewUserServiceClient(conn)}
}

func (c *UserClient) Register(ctx context.Context, username, password, nickname string) (int64, error) {
	resp, err := c.cli.Register(ctx, &userv1.RegisterRequest{
		Username: username,
		Password: password,
		Nickname: nickname,
	})
	if err != nil {
		return 0, err
	}
	return resp.GetUserId(), nil
}

func (c *UserClient) Login(ctx context.Context, username, password string) (string, int64, error) {
	resp, err := c.cli.Login(ctx, &userv1.LoginRequest{
		Username: username,
		Password: password,
	})
	if err != nil {
		return "", 0, err
	}
	return resp.GetToken(), resp.GetUserId(), nil
}

func (c *UserClient) GetConversationMembers(ctx context.Context, conversationID int64) ([]int64, error) {
	resp, err := c.cli.GetConversationMembers(ctx, &userv1.GetConversationMembersRequest{ConversationId: conversationID})
	if err != nil {
		return nil, err
	}
	return resp.GetUserIds(), nil
}
