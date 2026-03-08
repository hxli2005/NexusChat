# NexusChat — 面试知识点手册

> 针对项目中每一项技术选型，整理高频面试问题与标准回答。建议结合项目实际代码细节作答。

---

## 一、系统设计（全局）

### Q1：介绍一下 NexusChat 的整体架构

**答：**
NexusChat 是一个基于微服务的分布式实时聊天平台，拆分为 4 个服务：
- **Gateway**：管理 WebSocket 长连接，负责消息收发
- **User Service**：用户注册/登录/权限验证，暴露 gRPC 接口
- **Message Service**：消息落库（Snowflake ID + MySQL），缓存（Redis），消费/生产 Kafka
- **Agent Worker**：订阅消息，触发 AI 回复，调用 MemoryOS

服务间同步调用用 **gRPC**，异步解耦用 **Kafka**。这样设计的核心价值是：WebSocket 收到消息后立即返回"发送中"，实际的权限验证、落库、推送全部异步完成，网关层的响应延迟不受数据库压力影响。

---

### Q2：为什么要用微服务？不用单体行吗？

**答：**
单体对于这个项目规模完全可行。选择微服务是为了刻意解决以下问题：
1. **扩展性**：WebSocket 连接数 vs 消息落库 QPS 增长曲线不同，Gateway 和 Message Service 可以独立扩容
2. **隔离性**：AI 调用可能很慢（秒级），Agent Worker Hang 住不会影响用户的正常聊天
3. **学习目标**：掌握 gRPC、Kafka 在真实场景下的应用

---

### Q3：你们消息流转的完整链路是什么？

**答：**
```
用户 WS 发消息
  → Gateway 投递 Kafka[chat.msg.incoming]（立即回包"发送中"）
  → Message Service 消费：gRPC 验权 → Snowflake 生成 ID → MySQL 落库 → Redis 缓存
  → Message Service 投递 Kafka[chat.msg.outgoing]
  → Gateway 消费：查 Redis 在线状态 → WS 推送给目标用户
  → Agent Worker 消费（如触发 @AI）：调 MemoryOS → LLM → 把 AI 回复重投 incoming
```

---

## 二、WebSocket

### Q4：WebSocket 和 HTTP 长轮询的区别？

**答：**
| 维度 | HTTP 长轮询 | WebSocket |
|---|---|---|
| 连接建立 | 每次响应后重新发起 HTTP 请求 | 一次握手，全双工复用 |
| 延迟 | 高（请求-响应往返） | 低（服务端随时 push） |
| 服务端资源 | 每个"等待中"请求占一个线程/协程 | 每个连接一个描述符 + goroutine |
| 实现复杂度 | 简单 | 稍高 |

WebSocket 本质是在 TCP 上通过 HTTP Upgrade 协议头协商后切换的二进制/文本帧协议。

---

### Q5：如何管理海量 WebSocket 连接？

**答：**
Go 的优势在这里非常明显。每条连接起两个 goroutine（readPump / writePump），Go runtime 的 M:N 调度让百万连接在几十个 OS 线程上运行成为可能。

在 NexusChat 中，`ConnHub` 用 `map[userID]*websocket.Conn` + `sync.RWMutex` 管理本节点上的所有连接：
- 读操作（推送）用 `RLock`，高并发下不阻塞
- 写操作（注册/注销）用 `Lock`

进阶优化：可以将 `map` 分成多个分片（sharding），减少锁竞争。

---

### Q6：心跳机制如何实现？

**答：**
WebSocket 协议本身有 Ping/Pong 帧。在 Go 的 `gorilla/websocket` 中：
```go
conn.SetReadDeadline(time.Now().Add(90 * time.Second))
conn.SetPongHandler(func(string) error {
    conn.SetReadDeadline(time.Now().Add(90 * time.Second))
    return nil
})
// 服务端每30s发一次Ping
go func() {
    ticker := time.NewTicker(30 * time.Second)
    for range ticker.C {
        conn.WriteMessage(websocket.PingMessage, nil)
    }
}()
```
心跳的另一个作用是同步刷新 Redis 中的在线状态（TTL 续期），防止节点宕机后在线状态残留。

---

### Q7：用户断线后如何清理在线状态？

**答：**
两层保障：
1. **主动断开**：`readPump` 读到 `io.EOF`，调用 `hub.Unregister(userID)` → 删除 Redis `online:{user_id}`
2. **被动超时**：Redis TTL 设为 90s，心跳每 30s 续期一次。节点宕机后最多 90s 内在线状态自动失效

---

## 三、Kafka

### Q8：为什么用 Kafka 而不是直接同步处理？

**答：**
直接同步处理的问题：Gateway 收到消息后需要等待「验权 gRPC + MySQL 写入 + 推送」全部完成才能响应，任何一步慢都直接影响用户体验。数据库故障会导致消息丢失或超时。

用 Kafka 后：
- **解耦**：Gateway 只负责收发连接，无需感知 MySQL 是否可用
- **削峰**：突发流量打到 Kafka，Message Service 按自己的节奏消费
- **容错**：Message Service 重启后从 offset 继续消费，不丢消息

---

### Q9：Kafka 如何保证同一会话的消息有序？

**答：**
Kafka 只在单个 Partition 内保证顺序。设计上，生产者在发布消息时将 `conversation_id` 作为 **Message Key**，Kafka 默认对 Key 做哈希取模，相同 `conversation_id` 的消息始终落到同一个 Partition，从而保证会话内有序。

---

### Q10：消费者 offset 提交策略？如何避免消息重复消费？

**答：**
使用 `segmentio/kafka-go` 的**手动提交**策略：
1. 消费消息
2. 业务处理成功（MySQL 写入成功）
3. 调用 `reader.CommitMessages(ctx, msg)`

这样即使 Step 2 和 3 之间宕机，重启后会重新消费，消息不丢失。

但会导致**重复消费**。解决方案：以 `msg_id`（Snowflake ID）为幂等键，MySQL 表对 `id` 设置主键，重复插入直接 `INSERT IGNORE` 或 `ON DUPLICATE KEY UPDATE`。

---

### Q11：Kafka 的 Producer 消息可靠性配置？

**答：**
- `acks=all`（或 `-1`）：等待所有 ISR 副本确认，防止 Leader 宕机丢失
- `retries=3`：网络抖动时自动重试
- `idempotence=true`（幂等生产）：配合序列号，防止重试导致重复写入

在 Gateway 这种高吞吐场景可适当降低到 `acks=1`（只等 Leader），根据业务容忍度权衡。

---

## 四、Redis

### Q12：在线状态怎么设计的？解决了什么问题？

**答：**
```
Key:   online:{user_id}
Value: "gateway-host:port"  （存该用户连接的网关节点地址）
TTL:   90s，心跳续期
```
解决了**水平扩展时的用户路由问题**：当 Gateway 节点有多台时，消息要推送给用户 B，需要先查 Redis 知道 B 连的是哪个节点，再通过 gRPC 将消息转发到对应节点推送。

---

### Q13：消息缓存为什么用 Redis List？怎么防止缓存穿透？

**答：**
```
Key:  chat:msgs:{conversation_id}
操作: LPUSH（新消息头插）+ LTRIM（只保留最近100条）
读取: LRANGE 0 -1 → O(100)，极快
```

**防止缓存穿透**：
- 用户进入聊天页面，先读 Redis（命中率极高）
- Redis 未命中（极偶发，如缓存刚启动）才查 MySQL，查完回填 Redis
- 历史消息翻页（第 101 条起）直接走 MySQL 游标分页，不走 Redis，不存在缓存击穿问题

---

### Q14：Redis 和 MySQL 的数据一致性怎么保证？

**答：**
采用 **Write-Through** 策略：Message Service 先写 MySQL，成功后再写 Redis。不使用 Cache-Aside（先写缓存再写库），因为缓存写失败时 MySQL 已落库，数据不会丢，只是缓存暂时失效，下次读会从 MySQL 回填，最终一致。

对强一致性要求极高的场景可以引入分布式事务（如 Saga），但聊天场景对最终一致性可以接受。

---

## 五、gRPC

### Q15：为什么服务间通信选 gRPC 而不是 REST HTTP/JSON？

**答：**
| 维度 | REST + JSON | gRPC + Protobuf |
|---|---|---|
| 序列化 | JSON 文本，较大 | Protobuf 二进制，小 3-10x |
| 接口契约 | 文档约定，弱类型 | .proto 强类型，编译期保障 |
| 代码生成 | 手写 client 代码 | protoc 自动生成 |
| 流式支持 | 需 SSE/WS 额外处理 | 原生支持 4 种流模式 |
| 性能 | 良好 | 更优（连接复用 + 二进制） |

内部服务通信频率高（验权、查成员），Protobuf 的低开销和强类型契约是关键优势。

---

### Q16：gRPC 的 4 种调用模式？NexusChat 用了哪种？

**答：**
1. **Unary**：普通请求-响应（最常见）→ NexusChat 目前使用此模式
2. **Server Streaming**：服务端流（1请求N响应）→ 可用于消息历史分批下发
3. **Client Streaming**：客户端流（N请求1响应）→ 批量写入场景
4. **Bidirectional Streaming**：双向流 → 可用于替代 WebSocket（复杂度更高）

---

### Q17：如何处理 gRPC 的超时和重试？

**答：**
```go
// 调用时设置 deadline
ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
defer cancel()
resp, err := userClient.CheckMembership(ctx, req)
```
若 User Service 超時，Message Service 应拒绝该消息并向 Kafka 发送一个"失败通知"，而非无限等待。可以配合 gRPC 的 `WaitForReady` + 重试策略（通过 `ServiceConfig` JSON 配置）实现自动重试。

---

## 六、Snowflake 分布式 ID

### Q18：为什么不用 MySQL 自增 ID？

**答：**
MySQL 自增 ID 的问题：
1. **单点瓶颈**：高并发写入时自增 ID 需要行锁
2. **不适合分布式分表**：跨分片时 ID 会冲突或需要额外协调
3. **可预测**：连续数字容易被爬取/遍历

Snowflake ID 的优势：
- **全局唯一**：机器ID + 时间戳 + 序列号保证不冲突
- **趋势递增**：时间戳在高位，按时间排序效率高，适合 B+ 树索引
- **不依赖中心**：每个节点独立生成，无需协调

---

### Q19：Snowflake 如何解决时钟回拨问题？

**答：**
时钟回拨指服务器系统时间被调回，可能导致生成相同 ID。

NexusChat 的处理策略：
- **轻微回拨（< 5ms）**：`time.Sleep` 等待时钟追上上次记录的时间戳
- **严重回拨（> 5ms）**：panic 并报警，由运维处理（NTP 配置问题）
- **根本防御**：服务器开启 NTP 同步，禁止手动调时

---

### Q20：Snowflake 的 QPS 上限是多少？

**答：**
12 bit 序列号 → 单节点每毫秒最多生成 $2^{12} = 4096$ 个 ID，即 **单节点 QPS = 4,096,000**。
10 bit 机器 ID → 支持最多 **1024** 个节点。
理论上整个集群每秒可生成约 42 亿个不重复 ID，远超业务需求。

---

## 七、MySQL 与数据库设计

### Q21：消息表为什么用 Snowflake ID 做主键而不是自增？

**答：**
前一题已覆盖 Snowflake 的优势。额外一点：Snowflake ID 趋势递增，对 InnoDB B+ 树索引非常友好，写入时不会产生大量页分裂（自增 ID 也不会，但 UUID 会）。

---

### Q22：消息表的分页查询如何设计？

**答：**
使用**游标分页**（Keyset Pagination）：
```sql
-- 传入 before_msg_id（上次最后一条的 ID），查更早的消息
SELECT * FROM tb_messages
WHERE conversation_id = ?
  AND id < ?           -- before_msg_id
ORDER BY id DESC
LIMIT 20;
```
相比 `OFFSET` 分页，游标分页在深翻时不会扫描大量无用行，`(conversation_id, id)` 联合索引使查询 O(log n)。

---

### Q23：如果消息量很大，如何做分表？

**答：**
按 `conversation_id` 哈希分表（水平分表）：
- 路由规则：`table_index = conversation_id % 16`，命中 `tb_messages_00` ~ `tb_messages_15`
- 优点：同一会话的消息在同一张表，查询不跨表
- 缺点：跨会话的全局查询需要 scatter-gather
- 实现：可用 GORM + 中间件层做路由，或引入 ShardingSphere

分库策略升级：`db_index = conversation_id / 16 % 数据库数`。

---

## 八、JWT 鉴权

### Q24：JWT 的结构和验证流程？

**答：**
JWT = `Base64(Header).Base64(Payload).HMAC_SHA256(Header+Payload, secret)`

验证流程：
1. 客户端携带 `Authorization: Bearer <token>` 请求
2. 服务端重新计算签名，与 token 中的签名比对
3. 检查 `exp` 字段是否过期
4. 解析 `user_id` 字段，无需查库

**无状态**是 JWT 的核心优势，也是弱点：token 签发后无法主动失效（除非维护黑名单）。

---

### Q25：JWT 如何实现主动登出（吊销）？

**答：**
JWT 本身是无状态的，主动吊销需要引入状态：

**方案：Redis 黑名单**
- 登出时，将 JWT 的 `jti`（唯一标识符）写入 Redis，TTL = token 剩余有效时间
- 每次请求验证时，额外检查 Redis 中是否存在该 `jti`：`EXISTS token:blacklist:{jti}`
- 代价：每次请求多一次 Redis 查询（微秒级，可接受）

---

## 九、并发与 Go 特性

### Q26：ConnHub 中为什么用 `sync.RWMutex` 而不是 `sync.Mutex`？

**答：**
消息推送（读 map，找 conn）远比注册/注销（写 map）频繁。`RWMutex` 允许多个 goroutine 同时持有读锁，只有写操作时才互斥。在推送密集的场景下，`RWMutex` 比 `Mutex` 吞吐量高得多。

进一步优化：分片锁（Shard Lock），将 map 分成 N 个分片，每个分片独立锁，将锁竞争降低 N 倍。

---

### Q27：writePump 为什么要单独起一个 goroutine？

**答：**
`websocket.Conn` 的写操作**不是并发安全的**（gorilla/websocket 文档明确声明）。

如果多个 goroutine 同时往同一个连接写（如心跳 goroutine 和推送 goroutine），会导致 data race。

解决方案：每个连接有一个专属的 `writePump` goroutine，外部通过 `chan []byte` 发送消息，`writePump` 是唯一的写入者。

```go
type Client struct {
    conn   *websocket.Conn
    send   chan []byte  // 其他 goroutine 往这里塞消息
}

func (c *Client) writePump() {
    for msg := range c.send {
        c.conn.WriteMessage(websocket.TextMessage, msg)
    }
}
```

---

## 十、AI Agent 集成（MemoryOS 联动）

### Q28：Agent Worker 的设计有什么挑战？

**答：**
1. **幂等性**：Agent Worker 崩溃重启后会重新消费消息，可能重复发 AI 回复。解决：AI 回复消息的 `temp_id` 字段用 `"ai-reply-{原始msg_id}"` 格式，Message Service 去重。

2. **超时控制**：LLM 调用可能耗时 5-30s，需设置合理 context timeout，避免 goroutine 泄漏。

3. **背压控制**：AI 回复投递到 `chat.msg.incoming` 后会触发新的消费，需要确保 AI 不会回复 AI 自己的消息（通过判断 `sender_id == AgentBotUserID` 跳过）。

4. **流式回复**：LLM 支持 SSE 流式输出，可以将 token 逐个投递 WebSocket，实现"打字机"效果，提升用户体验。

---

## 十一、经典场景题

### Q29：如果 Gateway 有多个节点，用户 A 和用户 B 连在不同节点，消息如何推送？

**答：**
```
1. Message Service 消费消息，发布到 chat.msg.outgoing
2. 所有 Gateway 节点都订阅 outgoing topic（同一 Consumer Group 的不同分区分配，或不同 Consumer Group）
3. 每个 Gateway 节点尝试在本地 ConnHub 中找目标用户
4. 如果本地没有该用户：
   - 查 Redis online:{user_id} → 得到目标节点地址
   - 通过 gRPC 调用目标节点的"内部推送接口"
   - 目标节点收到后从本地 ConnHub 推送
```

注意：所有节点要订阅**全量 outgoing 消息**，或者 Message Service 在发布时指定目标节点（更复杂）。前者简单但每个节点要过滤无关消息，后者精准但耦合度高。

---

### Q30：消息送达保证（At-least-once vs Exactly-once）？

**答：**
NexusChat 实现的是 **At-least-once delivery**：
- Kafka 手动提交 offset，处理成功后才 commit
- MySQL 以 Snowflake ID 为主键天然去重（重复消费时 INSERT IGNORE）
- 客户端用 `temp_id` 做前端去重展示

**Exactly-once** 代价极高（需要跨 Kafka + MySQL 的分布式事务），聊天场景"已读回执"可以弥补偶发的送达不确定性，业界普遍采用 at-least-once + 幂等。

---

## 附：项目亮点一句话总结（简历/自我介绍用）

> 设计并实现了基于 **Kafka + WebSocket + gRPC** 的分布式实时聊天平台：通过 Kafka 异步解耦消息接入与落库，单节点可承受 **万级并发** WebSocket 连接；使用 Snowflake 算法生成全局有序消息 ID，结合 Redis List 缓存热点会话，将消息加载 P99 延迟压缩至 **10ms 以内**；集成 AI Agent 微服务，通过 MemoryOS 为用户提供具备长期记忆的智能对话体验。
