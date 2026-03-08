# NexusChat — AI Coding 实现规范文档

> 本文档面向 AI 编程助手，提供完整的目录结构、接口契约、数据模型、核心逻辑与实现顺序，可直接用于代码生成。

---

## 一、项目简介

**NexusChat** 是一个分布式实时聊天平台，支持单聊/群聊，并集成 AI Agent（对接 MemoryOS）。
语言：**Go 1.22+**，采用 **Monorepo** 布局，4 个独立微服务通过 Kafka 解耦，通过 gRPC 做服务间同步调用。

---

## 二、简化后的技术决策

| 保留 | 移除/推迟 |
|---|---|
| Gin HTTP + WebSocket (gorilla/websocket) | OpenTelemetry / Jaeger（可选扩展） |
| gRPC + Protobuf（服务间同步调用） | 消息分表（设计层说明即可） |
| Kafka（异步消息总线） | Service Mesh / 服务注册发现 |
| MySQL + GORM（持久化） | 分布式事务 |
| Redis（在线状态 + 消息缓存） | — |
| Snowflake（全局唯一消息 ID） | — |
| JWT（用户鉴权） | — |

---

## 三、Monorepo 目录结构

```
NexusChat/
├── api/                        # Protobuf 定义（唯一真相来源）
│   ├── user/v1/user.proto
│   └── message/v1/message.proto
│
├── cmd/                        # 各微服务入口
│   ├── gateway/main.go         # 服务1：网关 & WebSocket
│   ├── user/main.go            # 服务2：用户服务
│   ├── message/main.go         # 服务3：消息处理服务
│   └── agent/main.go           # 服务4：AI Agent Worker
│
├── internal/                   # 各服务私有业务逻辑
│   ├── gateway/
│   │   ├── handler/            # HTTP + WS 路由
│   │   ├── hub/                # WS 连接管理（ConnHub）
│   │   └── producer/           # Kafka 生产者封装
│   ├── user/
│   │   ├── service/            # 业务逻辑
│   │   └── repository/         # DB 操作
│   ├── message/
│   │   ├── service/
│   │   └── repository/
│   └── agent/
│       ├── detector/           # @AI 检测逻辑
│       └── llm/                # LLM / MemoryOS 调用
│
├── pkg/                        # 跨服务公共包
│   ├── snowflake/              # 雪花 ID 生成器
│   ├── kafka/                  # 生产者/消费者封装
│   ├── redisclient/            # Redis 封装
│   ├── middleware/             # JWT 中间件
│   └── model/                  # 共享数据结构（Kafka 消息体等）
│
├── deploy/
│   └── docker-compose.yml      # 基础设施：MySQL/Redis/Kafka(KRaft)
│
├── go.mod
└── go.sum
```

---

## 四、Protobuf 接口定义

### 4.1 `api/user/v1/user.proto`

```protobuf
syntax = "proto3";
package user.v1;
option go_package = "github.com/yourname/nexuschat/api/user/v1;userv1";

service UserService {
  // 验证两用户是否有好友关系
  rpc CheckFriendship(CheckFriendshipRequest) returns (CheckFriendshipResponse);
  // 验证用户是否在指定会话中
  rpc CheckMembership(CheckMembershipRequest) returns (CheckMembershipResponse);
  // 获取会话的所有成员 ID
  rpc GetConversationMembers(GetConversationMembersRequest) returns (GetConversationMembersResponse);
}

message CheckFriendshipRequest {
  int64 user_id_a = 1;
  int64 user_id_b = 2;
}
message CheckFriendshipResponse { bool is_friend = 1; }

message CheckMembershipRequest {
  int64 user_id       = 1;
  int64 conversation_id = 2;
}
message CheckMembershipResponse { bool is_member = 1; }

message GetConversationMembersRequest { int64 conversation_id = 1; }
message GetConversationMembersResponse { repeated int64 user_ids = 1; }
```

### 4.2 `api/message/v1/message.proto`

```protobuf
syntax = "proto3";
package message.v1;
option go_package = "github.com/yourname/nexuschat/api/message/v1;messagev1";

// 仅供内部查询历史消息（REST 接口已覆盖在线推送场景）
service MessageService {
  rpc GetHistory(GetHistoryRequest) returns (GetHistoryResponse);
}

message GetHistoryRequest {
  int64 conversation_id = 1;
  int64 before_msg_id   = 2; // 游标分页
  int32 limit           = 3;
}
message GetHistoryResponse { repeated MessageRecord messages = 1; }

message MessageRecord {
  int64  msg_id          = 1;
  int64  sender_id       = 2;
  int64  conversation_id = 3;
  string content         = 4;
  int64  created_at      = 5; // Unix timestamp ms
}
```

---

## 五、Kafka Topic 规范

| Topic | 生产者 | 消费者 | 说明 |
|---|---|---|---|
| `chat.msg.incoming` | Gateway | Message Service | 原始消息（未落库） |
| `chat.msg.outgoing` | Message Service | Gateway、Agent Worker | 已落库消息（含 msg_id） |

### Kafka 消息体结构 (`pkg/model/kafka.go`)

```go
// IncomingMessage — Gateway 投递到 chat.msg.incoming
type IncomingMessage struct {
    TempID         string `json:"temp_id"`         // 客户端生成的去重 ID
    SenderID       int64  `json:"sender_id"`
    ConversationID int64  `json:"conversation_id"`
    Content        string `json:"content"`
    SentAt         int64  `json:"sent_at"`          // Unix ms
}

// OutgoingMessage — Message Service 投递到 chat.msg.outgoing
type OutgoingMessage struct {
    MsgID          int64  `json:"msg_id"`           // Snowflake ID
    SenderID       int64  `json:"sender_id"`
    ConversationID int64  `json:"conversation_id"`
    Content        string `json:"content"`
    CreatedAt      int64  `json:"created_at"`
    IsAI           bool   `json:"is_ai"`            // true 表示来自 Agent
}
```

> **Partition 策略**：生产者按 `conversation_id % partitionCount` 路由，保证同一会话消息有序。

---

## 六、数据库表结构 (MySQL)

```sql
-- 用户表
CREATE TABLE tb_users (
    id          BIGINT PRIMARY KEY,           -- Snowflake ID
    username    VARCHAR(64) UNIQUE NOT NULL,
    password    VARCHAR(128) NOT NULL,         -- bcrypt hash
    nickname    VARCHAR(64),
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 好友关系（小 ID 在前，避免重复）
CREATE TABLE tb_friendships (
    id          BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_id_a   BIGINT NOT NULL,
    user_id_b   BIGINT NOT NULL,
    UNIQUE KEY uk_friendship (user_id_a, user_id_b)
);

-- 会话表（单聊 type=0，群聊 type=1）
CREATE TABLE tb_conversations (
    id          BIGINT PRIMARY KEY,
    type        TINYINT NOT NULL DEFAULT 0,
    name        VARCHAR(128),
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 会话成员
CREATE TABLE tb_conversation_members (
    conversation_id BIGINT NOT NULL,
    user_id         BIGINT NOT NULL,
    joined_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (conversation_id, user_id)
);

-- 消息表（核心）
CREATE TABLE tb_messages (
    id              BIGINT PRIMARY KEY,       -- Snowflake ID，全局有序
    conversation_id BIGINT NOT NULL,
    sender_id       BIGINT NOT NULL,
    content         TEXT NOT NULL,
    is_ai           TINYINT DEFAULT 0,
    created_at      BIGINT NOT NULL,          -- Unix ms，与 Snowflake 对齐
    INDEX idx_conv_id (conversation_id, id)   -- 分页查询核心索引
);
```

---

## 七、Redis 键规范

| Key 模式 | 类型 | TTL | 用途 |
|---|---|---|---|
| `online:{user_id}` | String | 90s（心跳续期） | 存储该用户连接的 Gateway 节点 IP:Port |
| `chat:msgs:{conv_id}` | List | 永不过期 | LPUSH 最新消息，LTRIM 保留最近 100 条 |
| `token:blacklist:{jti}` | String | 与 JWT 剩余有效期相同 | JWT 主动吊销 |

---

## 八、各服务核心逻辑

### 8.1 Gateway 服务

**HTTP 路由**（Gin）:
```
POST /api/v1/auth/register   → User Service gRPC
POST /api/v1/auth/login       → User Service gRPC，返回 JWT
GET  /api/v1/ws               → WebSocket 升级（需 JWT query param 或 header）
GET  /api/v1/messages/:conv_id → 读取 Redis List，不足则查 MySQL（Message Service gRPC）
```

**ConnHub 核心结构**:
```go
type ConnHub struct {
    mu    sync.RWMutex
    conns map[int64]*websocket.Conn  // userID -> WS 连接
}

// 注册连接：写 Redis online:{user_id} = selfAddr，启动心跳 goroutine
func (h *ConnHub) Register(userID int64, conn *websocket.Conn)
// 注销连接：删除 Redis key，关闭 conn
func (h *ConnHub) Unregister(userID int64)
// 本地推送：如果用户在本节点则直接写 conn
func (h *ConnHub) Push(userID int64, msg []byte) bool
```

**消息接收流程**:
1. 解析 WS 消息 → `IncomingMessage`
2. `kafka.Produce("chat.msg.incoming", conversationID, jsonBytes)`
3. 立即回复客户端 `{"status":"sending","temp_id":"..."}`

**消息推送（消费 chat.msg.outgoing）**:
```go
// 消费 outgoing topic
for msg := range consumer.Messages() {
    var out model.OutgoingMessage
    json.Unmarshal(msg.Value, &out)
    // 获取会话成员
    members := userClient.GetConversationMembers(out.ConversationID)
    for _, uid := range members {
        if !hub.Push(uid, msg.Value) {
            // 用户不在本节点，查 Redis online:{uid} 获取对端网关地址
            // 通过 gRPC 转发（跨节点推送，进阶实现）
        }
    }
}
```

### 8.2 User Service

- gRPC Server 实现 `UserServiceServer` 接口
- `Register`/`Login` 接口通过 Gin HTTP 暴露（也可合并到 Gateway）
- `Login` 成功后生成 JWT：`Claims{UserID, exp: now+7d}`
- bcrypt 做密码哈希，不存明文

### 8.3 Message Service

**消费 chat.msg.incoming 的核心流程**:
```go
func (s *Service) HandleIncoming(raw model.IncomingMessage) error {
    // 1. gRPC 调用 User Service 验证权限
    ok := s.userClient.CheckMembership(raw.SenderID, raw.ConversationID)
    if !ok { return ErrPermissionDenied }

    // 2. 生成 Snowflake ID
    msgID := s.snowflake.NextID()

    // 3. 写 MySQL
    s.repo.SaveMessage(msgID, raw)

    // 4. 写 Redis List（LPUSH + LTRIM 100）
    s.redis.LPush("chat:msgs:"+convID, jsonMsg)
    s.redis.LTrim("chat:msgs:"+convID, 0, 99)

    // 5. 投递 chat.msg.outgoing
    out := model.OutgoingMessage{MsgID: msgID, ...}
    s.kafka.Produce("chat.msg.outgoing", convID, out)
    return nil
}
```

### 8.4 Agent Worker

**触发条件**（消费 chat.msg.outgoing）:
```go
func isAITriggered(msg model.OutgoingMessage) bool {
    return strings.Contains(msg.Content, "@AI") || isDirectAIChat(msg.ConversationID)
}
```

**AI 回复链路**:
```go
func (w *Worker) HandleMessage(msg model.OutgoingMessage) {
    if !isAITriggered(msg) { return }

    // 1. 调用 MemoryOS 获取记忆上下文
    memory := w.memoryClient.Query(msg.SenderID, msg.Content)

    // 2. 构造 Prompt，调用 LLM（OpenAI/本地模型）
    reply := w.llm.Chat(memory, msg.Content)

    // 3. 构造 AI 消息，投递回 chat.msg.incoming（复用已有链路落库+推送）
    aiMsg := model.IncomingMessage{
        TempID:         uuid.New().String(),
        SenderID:       AgentBotUserID,  // 系统保留的 AI 用户 ID
        ConversationID: msg.ConversationID,
        Content:        reply,
    }
    w.kafka.Produce("chat.msg.incoming", msg.ConversationID, aiMsg)
}
```

---

## 九、Snowflake ID 实现规范 (`pkg/snowflake`)

```
| 1 bit 符号位 | 41 bit 毫秒时间戳 | 10 bit 机器ID | 12 bit 序列号 |
```

- **起始 Epoch**：`2024-01-01 00:00:00 UTC` 的 Unix 毫秒数
- **机器 ID**：从环境变量 `NODE_ID`（0-1023）读取，启动时校验
- **时钟回拨**：回拨 < 5ms 则等待，> 5ms 则 panic（强依赖 NTP）

---

## 十、Docker Compose 基础设施 (`deploy/docker-compose.yml`)

```yaml
version: "3.9"
services:
  mysql:
    image: mysql:8.0
    environment:
      MYSQL_ROOT_PASSWORD: nexuschat
      MYSQL_DATABASE: nexuschat
    ports: ["3306:3306"]
    volumes: ["mysql_data:/var/lib/mysql"]

  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]

  # Kafka KRaft 模式（无需 Zookeeper）
  kafka:
    image: bitnami/kafka:3.7
    ports: ["9092:9092"]
    environment:
      KAFKA_CFG_NODE_ID: "1"
      KAFKA_CFG_PROCESS_ROLES: "broker,controller"
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"
      KAFKA_CFG_LISTENERS: "PLAINTEXT://:9092,CONTROLLER://:9093"
      KAFKA_CFG_ADVERTISED_LISTENERS: "PLAINTEXT://localhost:9092"
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE: "true"
      ALLOW_PLAINTEXT_LISTENER: "yes"

volumes:
  mysql_data:
```

---

## 十一、实现顺序（严格遵循）

```
Week 1
  ✅ Step 1: 初始化 go.mod，确定 module 名，安装所有依赖
  ✅ Step 2: 编写并生成 .proto 文件（protoc）
  ✅ Step 3: 启动 docker-compose，验证中间件连通性
  ✅ Step 4: 实现 pkg/snowflake、pkg/kafka、pkg/redisclient、pkg/middleware

Week 2
  ✅ Step 5: User Service（gRPC server + MySQL CRUD + JWT login/register HTTP）
  ✅ Step 6: Message Service（Kafka consumer + Snowflake + MySQL + Redis 写入 + Kafka produce）
  ✅ Step 7: Gateway WebSocket（ConnHub + JWT 升级 + Kafka producer + 心跳）

Week 3
  ✅ Step 8: Gateway Kafka consumer（消费 outgoing + WS 推送）
  ✅ Step 9: 端到端联调（单聊消息全链路跑通）
  ✅ Step 10: Redis 在线状态逻辑 + 消息历史 API

Week 4（可选）
  ⬜ Step 11: Agent Worker（AI 触发检测 + MemoryOS + LLM）
  ⬜ Step 12: 流式回复（SSE 或 WS stream token-by-token）
  ⬜ Step 13: 跨网关节点推送（gRPC 转发）
```

---

## 十二、关键依赖包

```go
// go.mod 主要依赖
require (
    github.com/gin-gonic/gin           v1.10.0
    github.com/gorilla/websocket       v1.5.3
    google.golang.org/grpc             v1.64.0
    google.golang.org/protobuf         v1.34.2
    github.com/segmentio/kafka-go      v0.4.47
    github.com/redis/go-redis/v9       v9.5.4
    gorm.io/gorm                       v1.25.10
    gorm.io/driver/mysql               v1.5.7
    github.com/golang-jwt/jwt/v5       v5.2.1
    golang.org/x/crypto                v0.24.0   // bcrypt
    github.com/google/uuid             v1.6.0
)
```

---

## 附：环境变量规范

每个服务通过 `.env` 或系统环境变量读取配置，统一使用 `os.Getenv`：

```
# 公共
NODE_ID=1                          # Snowflake 机器号

# Gateway
GATEWAY_PORT=8080
KAFKA_BROKERS=localhost:9092
REDIS_ADDR=localhost:6379
USER_SERVICE_ADDR=localhost:50051
MESSAGE_SERVICE_ADDR=localhost:50052
JWT_SECRET=your-secret-key

# User Service
USER_GRPC_PORT=50051
MYSQL_DSN=root:nexuschat@tcp(localhost:3306)/nexuschat?parseTime=true

# Message Service
MESSAGE_GRPC_PORT=50052
MYSQL_DSN=root:nexuschat@tcp(localhost:3306)/nexuschat?parseTime=true
REDIS_ADDR=localhost:6379

# Agent Worker
MEMORYOS_API_URL=http://localhost:8000
LLM_API_KEY=sk-...
AGENT_BOT_USER_ID=1
```
