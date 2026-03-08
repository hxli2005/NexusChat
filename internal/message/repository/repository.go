package repository

import (
	"errors"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type Repository struct {
	db *gorm.DB
}

type Message struct {
	ID             int64  `gorm:"primaryKey;column:id"`
	ConversationID int64  `gorm:"column:conversation_id;not null;index:idx_conv_id,priority:1"`
	SenderID       int64  `gorm:"column:sender_id;not null"`
	Content        string `gorm:"column:content;type:text;not null"`
	IsAI           bool   `gorm:"column:is_ai;default:false"`
	CreatedAt      int64  `gorm:"column:created_at;not null;index:idx_conv_id,priority:2"`
}

func (Message) TableName() string { return "tb_messages" }

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
	return r.db.AutoMigrate(&Message{})
}

func (r *Repository) Save(msg *Message) error {
	return r.db.Create(msg).Error
}

func (r *Repository) GetHistory(conversationID, beforeMsgID int64, limit int) ([]Message, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	query := r.db.Where("conversation_id = ?", conversationID)
	if beforeMsgID > 0 {
		query = query.Where("id < ?", beforeMsgID)
	}

	var rows []Message
	err := query.Order("id DESC").Limit(limit).Find(&rows).Error
	return rows, err
}

func IsNotFound(err error) bool {
	return errors.Is(err, gorm.ErrRecordNotFound)
}

func NowMS() int64 {
	return time.Now().UnixMilli()
}
