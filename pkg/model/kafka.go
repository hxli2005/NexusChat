package model

// IncomingMessage is sent by Gateway to chat.msg.incoming.
type IncomingMessage struct {
	TempID         string `json:"temp_id"`
	SenderID       int64  `json:"sender_id"`
	ConversationID int64  `json:"conversation_id"`
	Content        string `json:"content"`
	SentAt         int64  `json:"sent_at"`
}

// OutgoingMessage is sent by Message Service to chat.msg.outgoing.
type OutgoingMessage struct {
	MsgID          int64  `json:"msg_id"`
	SenderID       int64  `json:"sender_id"`
	ConversationID int64  `json:"conversation_id"`
	Content        string `json:"content"`
	CreatedAt      int64  `json:"created_at"`
	IsAI           bool   `json:"is_ai"`
}
