package client

import (
	"context"

	messagev1 "github.com/lhx/nexuschat/api/message/v1"
	"github.com/lhx/nexuschat/pkg/model"
	"google.golang.org/grpc"
)

type MessageClient struct {
	cli messagev1.MessageServiceClient
}

func NewMessageClient(conn *grpc.ClientConn) *MessageClient {
	return &MessageClient{cli: messagev1.NewMessageServiceClient(conn)}
}

func (c *MessageClient) GetHistory(ctx context.Context, conversationID, beforeMsgID int64, limit int32) ([]model.OutgoingMessage, error) {
	resp, err := c.cli.GetHistory(ctx, &messagev1.GetHistoryRequest{
		ConversationId: conversationID,
		BeforeMsgId:    beforeMsgID,
		Limit:          limit,
	})
	if err != nil {
		return nil, err
	}
	out := make([]model.OutgoingMessage, 0, len(resp.GetMessages()))
	for _, m := range resp.GetMessages() {
		out = append(out, model.OutgoingMessage{
			MsgID:          m.GetMsgId(),
			SenderID:       m.GetSenderId(),
			ConversationID: m.GetConversationId(),
			Content:        m.GetContent(),
			CreatedAt:      m.GetCreatedAt(),
			IsAI:           m.GetIsAi(),
		})
	}
	return out, nil
}
