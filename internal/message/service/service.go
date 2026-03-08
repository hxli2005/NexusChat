package service

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/lhx/nexuschat/internal/message/repository"
	"github.com/lhx/nexuschat/pkg/kafka"
	"github.com/lhx/nexuschat/pkg/model"
	"github.com/lhx/nexuschat/pkg/redisclient"
	"github.com/lhx/nexuschat/pkg/snowflake"
)

var ErrPermissionDenied = errors.New("permission denied")

type MembershipChecker interface {
	CheckMembership(ctx context.Context, userID, conversationID int64) (bool, error)
}

// Service will handle message validation, persistence and fan-out publishing.
type Service struct {
	repo     *repository.Repository
	idGen    *snowflake.Generator
	outgoing *kafka.Producer
	redis    *redisclient.Client
	agentID  int64
	checker  MembershipChecker
}

func New(repo *repository.Repository, idGen *snowflake.Generator, outgoing *kafka.Producer, redisClient *redisclient.Client, agentID int64, checker MembershipChecker) *Service {
	return &Service{repo: repo, idGen: idGen, outgoing: outgoing, redis: redisClient, agentID: agentID, checker: checker}
}

func (s *Service) HandleIncoming(ctx context.Context, raw model.IncomingMessage) error {
	_ = ctx
	msg := &repository.Message{
		ID:             s.idGen.NextID(),
		ConversationID: raw.ConversationID,
		SenderID:       raw.SenderID,
		Content:        raw.Content,
		IsAI:           raw.SenderID == s.agentID,
		CreatedAt:      repository.NowMS(),
	}
	if s.checker != nil && msg.SenderID != s.agentID {
		ok, err := s.checker.CheckMembership(ctx, msg.SenderID, msg.ConversationID)
		if err != nil {
			return err
		}
		if !ok {
			return ErrPermissionDenied
		}
	}
	if err := s.repo.Save(msg); err != nil {
		return err
	}

	out := model.OutgoingMessage{
		MsgID:          msg.ID,
		SenderID:       msg.SenderID,
		ConversationID: msg.ConversationID,
		Content:        msg.Content,
		CreatedAt:      msg.CreatedAt,
		IsAI:           msg.IsAI,
	}
	if s.redis != nil {
		if bytes, err := json.Marshal(out); err == nil {
			_ = s.redis.CacheRecentMessage(ctx, out.ConversationID, bytes, 100)
		}
	}
	if s.outgoing != nil {
		if err := s.outgoing.ProduceJSON(ctx, out.ConversationID, out); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) GetHistory(ctx context.Context, conversationID, beforeMsgID int64, limit int) ([]model.OutgoingMessage, error) {
	_ = ctx
	rows, err := s.repo.GetHistory(conversationID, beforeMsgID, limit)
	if err != nil {
		return nil, err
	}
	out := make([]model.OutgoingMessage, 0, len(rows))
	for _, r := range rows {
		out = append(out, model.OutgoingMessage{
			MsgID:          r.ID,
			SenderID:       r.SenderID,
			ConversationID: r.ConversationID,
			Content:        r.Content,
			CreatedAt:      r.CreatedAt,
			IsAI:           r.IsAI,
		})
	}
	return out, nil
}
