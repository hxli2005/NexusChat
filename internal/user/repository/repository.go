package repository

import (
	"errors"
	"strings"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type Repository struct {
	db *gorm.DB
}

type User struct {
	ID        int64     `gorm:"primaryKey"`
	Username  string    `gorm:"size:64;uniqueIndex;not null"`
	Password  string    `gorm:"size:128;not null"`
	Nickname  string    `gorm:"size:64"`
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

func (User) TableName() string { return "tb_users" }

type Friendship struct {
	ID      int64 `gorm:"primaryKey;autoIncrement"`
	UserIDA int64 `gorm:"column:user_id_a;not null;uniqueIndex:uk_friendship"`
	UserIDB int64 `gorm:"column:user_id_b;not null;uniqueIndex:uk_friendship"`
}

func (Friendship) TableName() string { return "tb_friendships" }

type ConversationMember struct {
	ConversationID int64     `gorm:"primaryKey;column:conversation_id"`
	UserID         int64     `gorm:"primaryKey;column:user_id"`
	JoinedAt       time.Time `gorm:"autoCreateTime"`
}

func (ConversationMember) TableName() string { return "tb_conversation_members" }

func New(mysqlDSN string) (*Repository, error) {
	db, err := gorm.Open(mysql.Open(mysqlDSN), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	r := &Repository{db: db}
	if err := r.AutoMigrate(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Repository) DB() *gorm.DB {
	return r.db
}

func (r *Repository) AutoMigrate() error {
	return r.db.AutoMigrate(&User{}, &Friendship{}, &ConversationMember{})
}

func (r *Repository) CreateUser(user *User) error {
	return r.db.Create(user).Error
}

func (r *Repository) GetUserByUsername(username string) (*User, error) {
	var u User
	err := r.db.Where("username = ?", username).First(&u).Error
	if err != nil {
		return nil, err
	}
	return &u, nil
}

func (r *Repository) CheckFriendship(a, b int64) (bool, error) {
	if a == b {
		return true, nil
	}
	if a > b {
		a, b = b, a
	}
	var count int64
	err := r.db.Model(&Friendship{}).Where("user_id_a = ? AND user_id_b = ?", a, b).Count(&count).Error
	return count > 0, err
}

func (r *Repository) CheckMembership(userID, conversationID int64) (bool, error) {
	var count int64
	err := r.db.Model(&ConversationMember{}).
		Where("conversation_id = ? AND user_id = ?", conversationID, userID).
		Count(&count).Error
	return count > 0, err
}

func (r *Repository) GetConversationMembers(conversationID int64) ([]int64, error) {
	var members []ConversationMember
	err := r.db.Where("conversation_id = ?", conversationID).Find(&members).Error
	if err != nil {
		return nil, err
	}
	ids := make([]int64, 0, len(members))
	for _, m := range members {
		ids = append(ids, m.UserID)
	}
	return ids, nil
}

func IsDuplicateUsername(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "duplicate") ||
		strings.Contains(strings.ToLower(err.Error()), "unique")
}

func IsNotFound(err error) bool {
	return errors.Is(err, gorm.ErrRecordNotFound)
}
