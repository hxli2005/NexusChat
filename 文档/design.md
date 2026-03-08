NexusChat 分布式 AI 智能对话平台 - 设计与开发文档
1. 项目背景与目标
NexusChat 是一个支持实时群聊、私聊，并深度集成 AI Agent 的分布式聊天平台。它作为 MemoryOS 的直接应用层，允许用户在聊天室中直接与具备“长期记忆”的 AI 助手互动。

主要目标（也是简历的加分项）：

掌握 微服务架构 (Microservices) 与 gRPC 内部通信。
掌握 WebSocket 实时通信与海量连接管理。
引入 Kafka 解决高并发消息的异步落库与微服务解耦。
实现与 MemoryOS 的上下游联动，形成完整的 AI 应用生态。
2. 核心技术栈设计
领域	技术选型	选用原因（面试亮点）
接入层	Gin、WebSocket	处理 HTTP 请求，管理 WS 长连接，结合 JWT 鉴权
服务间通信	gRPC + Protobuf	强类型、高并发、低延迟的内部 RPC 调用
消息队列	Kafka	聊天消息广播解耦、AI 异步回复、削峰填谷
存储层	MySQL (GORM), Redis	MySQL 存持久化消息与账号；Redis 做在线状态维持与消息缓存
分布式支持	Snowflake (雪花算法)	生成全局唯一的有序 Message ID
可观测性	OpenTelemetry + Jaeger	（可选伸缩）追踪一条消息从 WS 进来到 AI 返回的全链路
3. 微服务架构划分
系统拆分为 4 个主要微服务（适中的复杂度）：

1. Gateway & WS Service (网关与连接层)
职责：维护客户端的 WebSocket 长连接，处理心跳保活。
流程：收到用户 WS 消息后，不直接查库，而是将其打包投递到 Kafka。并监听 Kafka 的业务广播，将消息推送到对应的 WS 连接。
2. User Service (用户服务)
职责：处理注册、登录（JWT）、好友关系、群组关系。
接口：暴露 gRPC 接口供其他服务查询验证“用户是否在群里”、“获取用户信息”等。
3. Message Service (消息处理服务)
职责：消费 Kafka 中的原始消息，生成全局唯一的 Snowflake ID，验证权限后进行落库存库（写 MySQL，同步缓存 Redis），然后再将处理好的消息发回 Kafka 的“广播 Topic”。
4. Agent Worker (AI 代理服务) -- 🌟 联动 MemoryOS
职责：监听 Kafka 消息。如果发现有人 @Agent 或者在和 Agent 私聊，触发 AI 逻辑。
联动：组装参数请求你的 MemoryOS (API) 获取上下文，调用大模型生成回复，最后将回复发送到 Kafka，由 WS Service 推给用户。
4. 关键业务流设计 (核心亮点)
场景 A：用户 A 给 用户 B 发送消息
A 通过 WebSocket 发送消息到 Gateway。
Gateway 将消息包装投递至 Kafka 的 chat.msg.incoming Topic，立即向 A 返回 "发送中"。
Message Service 消费该 Topic，使用 雪花算法 生成消息 ID。
Message Service 通过 gRPC 调用 User Service 检查 A 与 B 的好友关系。
校验通过，消息落库 MySQL，并写入 Redis 缓存。
Message Service 将最终消息投递至 Kafka 的 chat.msg.outgoing Topic。
Gateway 消费 outgoing Topic，根据 Redis 中 B 的在线状态。如果 B 在线，通过 WebSocket 推送给 B。
场景 B：用户在群聊中 @AI 助手 (联动 MemoryOS)
前面的流程同上，消息进入 Message Service 后落库。
Agent Worker 微服务同样订阅了 chat.msg.outgoing。
它发现这是一条 @AI 的消息，提取内容。
调用 MemoryOS 的接口，传入 user_id 和 query，获取记忆。
请求 LLM 获取回复。
Agent Worker 构造成一条系统发出的消息，投递回 Kafka，系统最终通过 WebSocket 广播到群聊，展现出 AI “秒回”或“打字机”效果。
5. 数据库与缓存设计关键
数据库设计 (MySQL)
tb_users: 用户基础表
tb_conversations: 会话表（单聊/群聊）
tb_messages: 核心消息表。分表策略建议按 conversation_id 的 hash 进行水平分表。
Redis 缓存策略 (面试核心点)
在线状态管理：使用 Hash 或 ZSet。网关在用户连上 WS 时写入 online_users:{user_id} = gateway_server_ip，心跳续期。断开时删除。解决如何定位用户连在哪个网关节点的问题。
最新消息缓存（读写分离）：针对群聊，使用 Redis List chat:messages:{conversation_id} 缓存最新 100 条消息。用户进入聊天页面时，直接读 Redis（极快），超过 100 条向上翻页时再触发 MySQL 查询（解决缓存穿透/击穿防线）。
6. 开发实施 Milestone (建议顺序)
阶段 1：基础设施建立 (Week 1)
搭建 monorepo 目录结构（api/ 放 protobuf 文件，cmd 放各个微服务，pkg 放公共雪花算法、Kafka 客户端）。
定义 .proto 文件并生成 Go 代码（如 user.proto, message.proto）。
使用 Docker Compose 搭建 MySQL, Redis, Kafka, Zookeeper/KRaft 环境。
阶段 2：用户与消息中枢 (Week 2)
实现 User Service，跑通 gRPC 调用验证逻辑。
实现 Gateway WebSocket 服务，完成 JWT 连接升级和心跳检测，打印用户上/下线日志。
实现 Message Service，集成 Snowflake ID 生成器，打通 MySQL 异步落库。
阶段 3：分布式消息流转 (Week 3)
引入 Kafka，实现 Gateway 端生产消息 -> Message Service 消费/落库 -> Gateway 消费推送的完整异步闭环。
增加 Redis 的在线状态（网关路由）逻辑。
阶段 4：AI 智能体大脑接入 (Week 4)
实现 Agent Worker 微服务。
使用 Go 的 http.Client 或 Resty 库，让 Agent Worker 请求你部署好的 MemoryOS API。
将 LLM 的回复重新注入聊天链路。