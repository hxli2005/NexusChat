package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/lhx/nexuschat/internal/gateway/client"
	"github.com/lhx/nexuschat/internal/gateway/hub"
	"github.com/lhx/nexuschat/internal/gateway/producer"
	"github.com/lhx/nexuschat/pkg/middleware"
	"github.com/lhx/nexuschat/pkg/model"
	"github.com/lhx/nexuschat/pkg/redisclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RouterDeps struct {
	UserClient    *client.UserClient
	MessageClient *client.MessageClient
	Hub           *hub.ConnHub
	Incoming      *producer.IncomingProducer
	Redis         *redisclient.Client
	SelfAddr      string
}

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func NewRouter(deps RouterDeps) *gin.Engine {
	r := gin.Default()
	r.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	})

	v1 := r.Group("/api/v1")
	{
		v1.POST("/auth/register", func(c *gin.Context) {
			var req struct {
				Username string `json:"username"`
				Password string `json:"password"`
				Nickname string `json:"nickname"`
			}
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
				return
			}
			userID, err := deps.UserClient.Register(c.Request.Context(), req.Username, req.Password, req.Nickname)
			if err != nil {
				if st, ok := status.FromError(err); ok {
					switch st.Code() {
					case codes.InvalidArgument:
						c.JSON(http.StatusBadRequest, gin.H{"error": st.Message()})
					case codes.AlreadyExists:
						c.JSON(http.StatusConflict, gin.H{"error": st.Message()})
					default:
						c.JSON(http.StatusInternalServerError, gin.H{"error": "register failed"})
					}
				} else {
					c.JSON(http.StatusInternalServerError, gin.H{"error": "register failed"})
				}
				return
			}
			c.JSON(http.StatusCreated, gin.H{"user_id": userID})
		})
		v1.POST("/auth/login", func(c *gin.Context) {
			var req struct {
				Username string `json:"username"`
				Password string `json:"password"`
			}
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
				return
			}
			token, userID, err := deps.UserClient.Login(c.Request.Context(), req.Username, req.Password)
			if err != nil {
				if st, ok := status.FromError(err); ok {
					switch st.Code() {
					case codes.InvalidArgument, codes.Unauthenticated:
						c.JSON(http.StatusUnauthorized, gin.H{"error": st.Message()})
					default:
						c.JSON(http.StatusInternalServerError, gin.H{"error": "login failed"})
					}
				} else {
					c.JSON(http.StatusInternalServerError, gin.H{"error": "login failed"})
				}
				return
			}
			c.JSON(http.StatusOK, gin.H{"token": token, "user_id": userID})
		})
		v1.GET("/ws", middleware.AuthRequired(), func(c *gin.Context) {
			uidVal, _ := c.Get("user_id")
			userID, ok := uidVal.(int64)
			if !ok {
				c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid token payload"})
				return
			}

			conn, err := wsUpgrader.Upgrade(c.Writer, c.Request, nil)
			if err != nil {
				return
			}
			if deps.SelfAddr == "" {
				deps.SelfAddr = "gateway-local"
			}
			if deps.Redis != nil {
				_ = deps.Redis.SetOnline(context.Background(), userID, deps.SelfAddr, 90*time.Second)
			}
			deps.Hub.Register(userID, conn)
			defer func() {
				deps.Hub.Unregister(userID)
				if deps.Redis != nil {
					_ = deps.Redis.DelOnline(context.Background(), userID)
				}
			}()

			_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
			conn.SetPongHandler(func(string) error {
				_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
				if deps.Redis != nil {
					_ = deps.Redis.SetOnline(context.Background(), userID, deps.SelfAddr, 90*time.Second)
				}
				return nil
			})
			go func() {
				ticker := time.NewTicker(30 * time.Second)
				defer ticker.Stop()
				for range ticker.C {
					if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
						return
					}
					if deps.Redis != nil {
						_ = deps.Redis.SetOnline(context.Background(), userID, deps.SelfAddr, 90*time.Second)
					}
				}
			}()

			for {
				_, payload, err := conn.ReadMessage()
				if err != nil {
					return
				}
				var in model.IncomingMessage
				if err := json.Unmarshal(payload, &in); err != nil {
					_ = conn.WriteJSON(gin.H{"status": "error", "error": "invalid message format"})
					continue
				}
				in.SenderID = userID
				if in.SentAt == 0 {
					in.SentAt = time.Now().UnixMilli()
				}
				if deps.Incoming != nil {
					if err := deps.Incoming.Send(context.Background(), in); err != nil {
						_ = conn.WriteJSON(gin.H{"status": "error", "error": "message enqueue failed"})
						continue
					}
				}
				_ = conn.WriteJSON(gin.H{"status": "sending", "temp_id": in.TempID})
			}
		})
		v1.GET("/messages/:conv_id", middleware.AuthRequired(), func(c *gin.Context) {
			convID, err := strconv.ParseInt(c.Param("conv_id"), 10, 64)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid conversation id"})
				return
			}
			beforeMsgID, _ := strconv.ParseInt(c.DefaultQuery("before_msg_id", "0"), 10, 64)
			limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))

			if deps.Redis != nil && beforeMsgID == 0 {
				cached, err := deps.Redis.GetRecentMessages(c.Request.Context(), convID, int64(limit))
				if err == nil && len(cached) > 0 {
					messages := make([]model.OutgoingMessage, 0, len(cached))
					for _, raw := range cached {
						var m model.OutgoingMessage
						if err := json.Unmarshal([]byte(raw), &m); err == nil {
							messages = append(messages, m)
						}
					}
					if len(messages) > 0 {
						c.JSON(http.StatusOK, gin.H{"messages": messages, "source": "redis"})
						return
					}
				}
			}

			messages, err := deps.MessageClient.GetHistory(c.Request.Context(), convID, beforeMsgID, int32(limit))
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to query history"})
				return
			}
			c.JSON(http.StatusOK, gin.H{"messages": messages, "source": "mysql"})
		})
	}

	return r
}
