package service

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/lhx/nexuschat/internal/user/repository"
	"github.com/lhx/nexuschat/pkg/middleware"
	"github.com/lhx/nexuschat/pkg/snowflake"
	"golang.org/x/crypto/bcrypt"
)

// Service keeps user-related business logic.
type Service struct {
	repo     *repository.Repository
	idGen    *snowflake.Generator
	tokenTTL time.Duration
}

type RegisterInput struct {
	Username string
	Password string
	Nickname string
}

type LoginInput struct {
	Username string
	Password string
}

var (
	ErrInvalidInput = errors.New("invalid input")
	ErrUserExists   = errors.New("username already exists")
	ErrInvalidCreds = errors.New("invalid username or password")
)

func New(repo *repository.Repository, idGen *snowflake.Generator) *Service {
	return &Service{repo: repo, idGen: idGen, tokenTTL: 7 * 24 * time.Hour}
}

func (s *Service) Register(ctx context.Context, in RegisterInput) (int64, error) {
	if strings.TrimSpace(in.Username) == "" || strings.TrimSpace(in.Password) == "" {
		return 0, ErrInvalidInput
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(in.Password), bcrypt.DefaultCost)
	if err != nil {
		return 0, err
	}
	user := &repository.User{
		ID:       s.idGen.NextID(),
		Username: strings.TrimSpace(in.Username),
		Password: string(hash),
		Nickname: strings.TrimSpace(in.Nickname),
	}
	if err := s.repo.CreateUser(user); err != nil {
		if repository.IsDuplicateUsername(err) {
			return 0, ErrUserExists
		}
		return 0, err
	}
	return user.ID, nil
}

func (s *Service) Login(ctx context.Context, in LoginInput) (string, int64, error) {
	if strings.TrimSpace(in.Username) == "" || strings.TrimSpace(in.Password) == "" {
		return "", 0, ErrInvalidInput
	}
	user, err := s.repo.GetUserByUsername(strings.TrimSpace(in.Username))
	if err != nil {
		if repository.IsNotFound(err) {
			return "", 0, ErrInvalidCreds
		}
		return "", 0, err
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(in.Password)); err != nil {
		return "", 0, ErrInvalidCreds
	}
	token, err := middleware.GenerateToken(user.ID, s.tokenTTL)
	if err != nil {
		return "", 0, err
	}
	return token, user.ID, nil
}

func (s *Service) CheckFriendship(ctx context.Context, a, b int64) (bool, error) {
	_ = ctx
	return s.repo.CheckFriendship(a, b)
}

func (s *Service) CheckMembership(ctx context.Context, userID, conversationID int64) (bool, error) {
	_ = ctx
	return s.repo.CheckMembership(userID, conversationID)
}

func (s *Service) GetConversationMembers(ctx context.Context, conversationID int64) ([]int64, error) {
	_ = ctx
	return s.repo.GetConversationMembers(conversationID)
}
