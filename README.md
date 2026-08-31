# acemcp-relay

LCE 的多租户远程 MCP 服务。支持 MCP 的编码 Agent 只需配置一个 Streamable HTTP MCP 地址，即可完成代码索引、语义检索、符号关系查询和索引统计。服务基于 Gin 构建，并使用 PostgreSQL 保存工作区索引快照、任务进度和请求记录。

## 功能特性

- **纯 MCP 首次与增量索引**：`codebase_index` 比较 Agent 提交的完整工作区 manifest 与服务端快照，首次返回全量文件，后续只返回新增或变更文件并检测删除文件
- **索引任务进度**：持久化任务阶段、文件数、chunk 数、分支、revision、心跳和完成状态
- **LCE MCP 对接**：对外通过 `codebase_index` 同步索引、通过 `codebase-retrieval` 执行向量召回和精排；底层索引原语不向客户端暴露
- **API Key 认证**：基于 Bearer Token 的认证中间件，API Key 存于 PostgreSQL（`api_keys.id` 为 key 的哈希 hex，兼容存量 MD5 与新的 SHA-256 双读）；relay 进程内缓存认证结果；正/负缓存时长和容量由运行策略配置，撤销延迟受正缓存时长约束
- **请求日志**：自动记录每个请求的状态、耗时、来源 IP 等信息到 PostgreSQL
- **错误追踪**：异步记录代理层和上游服务的错误详情
- **任务回收**：索引任务完成或失败后删除暂存文件；运行任务心跳超时后自动标记并回收
- **使用排行榜**：定时统计成功的 `codebase-retrieval` 与 `codebase_enhance_prompt` 调用量（更新周期由运行策略配置，不含索引、符号图和协议请求）
- **健康检查**：按运行策略周期调用 LCE MCP `tools/list`，将可用性和延迟写入 `health_checks`
- **请求/响应压缩**：对上游请求体使用 zstd 压缩（`SpeedFastest` 等级，小于 1024 字节的 payload 跳过压缩）；响应按客户端 `Accept-Encoding` 协商编码（`br` / `gzip` / `deflate` / `identity`，brotli 等级 4），压缩失败时回退到 identity
- **性能观测**：内置 pprof 服务（仅监听 `127.0.0.1:6060`），用于运行时 CPU / 内存 profiling

## 支持的 API 路径

| 路径 | 说明 |
|------|------|
| `POST /mcp` | MCP Streamable HTTP 入口，承载初始化、工具发现和全部工具调用 |
| `DELETE /mcp` | 关闭当前 MCP session |
| `GET /mcp` | 返回 405；服务不提供 SSE 推流 |
| `GET /mcp/tenant-stats` | 网页控制台内部索引统计接口；要求控制台内部凭据 |
| `GET /mcp/roots` | 网页控制台内部 root/分支索引状态接口；返回已发布快照、最近任务进度及结构化失败诊断 |
| `POST /mcp/dismiss-root-failure` | 清理指定 root 的 `failed` / `timed_out` 任务记录；保留已发布云端快照，仅组织 owner 可操作组织索引 |
| `POST /mcp/delete-root` | 删除指定 root 的 LCE 已发布快照和 Relay 状态；首次索引失败且尚无 workspace 的任务也可完整清理 |
| `POST /mcp/clear-index` | 网页控制台内部全量清理接口；要求控制台内部凭据及冷却校验 |

索引和检索均只走 MCP；不保留插件专用的 `/relay/*` REST 数据面，也不提供旧 Augment `/find-missing`、`/batch-upload`、`/checkpoint-blobs` 协议。

### MCP 工具边界

远程 MCP 对模型只暴露 5 个租户安全工具：

- `codebase-retrieval`：在租户服务端索引上执行向量召回与精排；向量空间由 LCE 固定，Relay 只按用户注入可选 rerank 配置。
- `codebase_symbol_graph`：查询指定 `root_id` 的服务端符号图。
- `codebase_enhance_prompt`：基于租户索引中检索到的代码上下文增强自然语言任务；管理员必须先配置并启用提示词增强模型。
- `codebase_index_status`：查询一个或全部项目根的索引可用性、同步阶段、文件进度与失败原因；已有快照可用时会同时标明后台更新状态。
- `codebase_index`：不依赖特定宿主插件的索引入口。Agent 使用自身文件读取能力提交完整 manifest，再按 `pending_files` 分批上传内容，最后完成任务并收敛符号图。

`codebase_tenant_stats` 是网页控制台的内部统计接口，不会出现在远程 MCP 的 `tools/list` 中。

`codebase_index` 的操作顺序为 `start -> upload -> complete`，可用 `status` 同步续期 Relay 与 LCE 的任务租约并查询进度，失败时调用 `fail` 回收任务。服务端负责注入 `tenant_id`，并强制执行 SHA-256 内容匹配、路径过滤、manifest/批次上限、每日索引字节配额、root 隔离和删除检测。Relay 使用同一个 job UUID 调用 LCE 的内部 `begin -> stage/renew -> publish/abort` 协议；首次 full job 以 `replace_root=true` 发布完整根快照，能够清除仅存在于云端的旧文件。PostgreSQL 中的词法、精确、向量和经编译器细化的符号图数据只在 publish 时原子可见。`codebase_remote_index` 和 `codebase_clear_index` 是内部控制面，不能由模型直接调用。

任务一旦进入 `failed` 或 `timed_out` 就是终态，Relay 不会在网络恢复后续跑同一个 job。重新建立索引必须由一个能够读取仓库文件并执行索引协议的客户端创建新任务；修复根因后，应让该客户端重新初始化索引能力或重新连接。刷新网页控制台、仅查询状态或再次发起普通自然语言检索都不是可靠的整任务重试触发器。`dismiss-root-failure` 只清理失败记录，不会触发重建，也不会删除仍可检索的已发布快照。

远端服务无法读取 MCP 宿主所在机器的文件系统或 `.git` 目录。Relay 的 Streamable HTTP `/mcp` 端点保留为本地客户端和服务内部调用的传输层；控制台不再把它作为普通用户的独立接入方式，因为直接配置它缺少本地 Git 工具、文件监听、自动增量同步和分支视图跟踪。

控制台按部署策略生成本地客户端启动配置，不把某个编辑器、包执行器或安装命令写成协议。该客户端向 Agent 暴露检索、符号图、提示词增强、索引状态以及两个本地 Git 工具，并负责文件监听、自动增量索引与分支视图跟踪；底层 `codebase_index` 同步协议由客户端自动调用，不作为第七个面向 Agent 的工具暴露。

提示词增强不会自动拦截用户的每条消息。用户需要明确要求 Agent 调用，例如：

> 先调用 `codebase_enhance_prompt`，展示增强结果后再开始实现；原始要求始终优先。任务：修复登录超时并补充回归测试。已知标识：`AuthService`、`sessionTTL`。

Agent 应把完整原始任务放入 `prompt`，把已知符号、文件名或错误码放入可选的 `technical_terms`。返回结果包含目标、实现要求、约束、已验证代码引用、验证步骤和待确认问题，只能作为原始请求的补充，不能删改用户要求。`root_id` 可限定索引分支视图，`output_language` 支持 `auto`、`zh-CN` 和 `en`。

## 技术栈

- **语言**：Go 1.25
- **Web 框架**：Gin
- **数据库**：PostgreSQL（请求日志、排行榜、API Key 存储）
- **缓存**：Redis（封禁与配额状态；API Key 认证缓存为进程内实现）
- **依赖管理**：Go Modules

## 前置要求

- Go 1.25+
- PostgreSQL
- Redis

## 快速开始

### 1. 克隆项目

```bash
git clone <repository-url>
cd acemcp-relay
```

### 2. 安装依赖

```bash
go mod download
```

### 3. 配置环境变量

复制示例配置文件并根据实际情况修改：

```bash
cp .env.example .env
```

### 4. 运行

```bash
go run .
```

### 5. 构建

```bash
go build -o acemcp-relay .
```

## 环境变量配置

通过 `.env` 文件或系统环境变量配置，所有变量均有默认值。

### 服务配置

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `SERVER_ADDR` | 服务监听地址 | `127.0.0.1:3009` |
| `PPROF_ADDR` | pprof 与内部 metrics 监听地址；除非明确配置网络隔离，否则保持 loopback | `127.0.0.1:6060` |
| `TRUSTED_PROXIES` | 可提供 `X-Forwarded-For` 的反向代理 IP/CIDR，逗号分隔；直连时留空 | （空） |

### LCE MCP 配置

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `LCE_MCP_URL` | LCE MCP HTTP endpoint | `http://127.0.0.1:3000/mcp` |
| `LCE_CLOUD_DATABASE_URL` | LCE cloud PostgreSQL URL; the database must have pgvector available | required for tenant indexing |
| `LCE_TENANT_ASSERTION_SECRET` | 与 LCE 共享的租户断言密钥（≥32 字符）。relay 用它为每次带 `tenant_id` 的调用签发短时效断言，LCE 校验通过才认这个租户 | 空（不签发） |

`tenant_id` 在 MCP 里只是普通工具入参，LCE 无法区分授权调用与伪造调用——不做校验时，任何能连到 LCE 端口的人都可以读、写、甚至用 `codebase_clear_index` 清空**任意**租户的索引。配置该密钥后，租户隔离不再只依赖"LCE 端口不对外暴露"这一条前提。

存活探测走 LCE 的 `GET {LCE_MCP_URL}/health`；部署编排的就绪探测走 `GET {LCE_MCP_URL}/ready`，后者还会确认 PostgreSQL/pgvector schema 与平台 embedding provider 可用。两者都不建立 MCP session。

升级顺序：先部署 relay（LCE 未配密钥时会忽略该请求头），再给 LCE 配上同一个密钥。反过来会在 relay 更新前中断租户调用。

### 运行时策略

以下值是部署策略而不是协议标识，均可通过环境变量覆盖。Relay 自身的无效运行策略会记录配置告警并回退到安全默认值；LCE 与前端进程会在启动或首次读取配置时拒绝无效值，避免带着歧义配置继续运行。索引文件上限高于批次上限时，Relay 会把文件上限收敛到批次上限。

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `INDEX_JOB_HEARTBEAT_TIMEOUT` | 运行中索引任务无心跳后的超时时间 | `10m` |
| `INDEX_JOB_SWEEP_INTERVAL` | 超时任务扫描周期 | `1m` |
| `INDEX_JOB_RENEW_TIMEOUT` | 单次索引租约续期调用超时 | `15s` |
| `INDEX_MAX_MANIFEST_FILES` | 单次 manifest 文件数上限 | `100000` |
| `INDEX_MAX_BATCH_FILES` | 单批上传文件数上限 | `50` |
| `INDEX_MAX_FILE_BYTES` | 单文件原始内容字节上限 | `524288` |
| `INDEX_MAX_BATCH_BYTES` | 单批原始内容总字节上限 | `524288` |
| `INDEX_MAX_PATH_BYTES` | 索引路径字节上限 | `4096` |
| `INDEX_MAX_FAILURE_BYTES` | 客户端上报索引失败详情的 UTF-8 字节上限；工具 schema 与服务端截断共用该值 | `2000` |
| `INDEX_START_MIN_INTERVAL` | 同一租户与 root 两次 start 的保护间隔 | `30s` |
| `INDEX_START_MEMORY_ENTRIES` | start 保护窗口的进程内记录上限 | `4096` |
| `MCP_CLIENT_REQUEST_BODY_LIMIT_BYTES` | 客户端 MCP 请求体上限 | `33554432` |
| `LCE_MCP_REQUEST_BODY_LIMIT_BYTES` | 转发到 LCE 的 MCP 请求体上限 | `4194304` |
| `MCP_CALL_TIMEOUT` | 常规 LCE MCP 调用超时 | `120s` |
| `INDEX_MCP_CALL_TIMEOUT` | 远程索引与清理调用超时 | `330s` |
| `MCP_INIT_SESSION_TIMEOUT` | LCE MCP session 初始化超时 | `10s` |
| `PLATFORM_MODEL_CONFIG_BODY_LIMIT_BYTES` | 模型配置响应体上限 | `65536` |
| `PLATFORM_MODEL_CONFIG_READ_TIMEOUT` | 读取平台模型配置超时 | `15s` |
| `PLATFORM_MODEL_DISCOVERY_TIMEOUT` | 获取供应商模型列表超时 | `30s` |
| `PLATFORM_MODEL_CONFIG_VALIDATION_TIMEOUT` | 模型连接验证超时 | `35s` |
| `PLATFORM_MODEL_CONFIG_BARRIER_WAIT` | embedding 切换等待索引屏障的时间 | `10s` |
| `PLATFORM_MODEL_CONFIG_CLEAR_TIMEOUT` | 清理 Relay 索引状态超时 | `30s` |
| `PLATFORM_MODEL_CONFIG_SAVE_TIMEOUT` | 不重建索引的配置保存超时 | `15s` |
| `PLATFORM_MODEL_CONFIG_RESET_SAVE_TIMEOUT` | 需要重建索引的配置保存超时 | `60s` |
| `PLATFORM_MODEL_CONFIG_LOCK_POLL_INTERVAL` | 写屏障轮询间隔 | `50ms` |
| `LCE_PLATFORM_MODEL_VALIDATION_TICKET_TTL_MS` | LCE 供应商验证票据有效期 | `600000` |
| `LCE_PLATFORM_MODEL_PROVIDER_RESPONSE_MAX_BYTES` | LCE 供应商模型列表响应上限 | `1048576` |
| `LCE_PLATFORM_MODEL_DISCOVERY_TIMEOUT_MS` | LCE 供应商模型列表请求超时 | `10000` |
| `LCE_PLATFORM_MODEL_DISCOVERY_RESULT_LIMIT` | LCE 单次保留的模型列表条目上限 | `500` |
| `LCE_CLOUD_INDEX_JOB_TTL_MINUTES` | LCE 暂存索引任务租期 | `15` |
| `LCE_CLOUD_COMPILER_SNAPSHOT_MAX_FILES` | LCE 云端编译器快照文件上限 | `2000` |
| `LCE_CLOUD_QUERY_EMBEDDING_CACHE_TTL_MS` | LCE 查询向量内存缓存有效期 | `30000` |
| `LCE_CLOUD_QUERY_EMBEDDING_CACHE_MAX_ENTRIES` | LCE 查询向量内存缓存容量 | `512` |
| `LCE_CLOUD_RERANK_CACHE_TTL_MS` | LCE rerank 内存缓存有效期 | `30000` |
| `LCE_CLOUD_RERANK_CACHE_MAX_ENTRIES` | LCE rerank 内存缓存容量 | `512` |
| `LCE_PROVIDER_AUTH_COOLDOWN_MS` | LCE 对鉴权失败 API Key 的冷却时间 | `300000` |
| `LCE_PROVIDER_BILLING_COOLDOWN_MS` | LCE 对余额/计费失败 API Key 的冷却时间 | `900000` |
| `LCE_PROVIDER_TRANSIENT_COOLDOWN_MS` | LCE 对网络与供应商临时故障 API Key 的冷却时间 | `5000` |
| `LCE_PROVIDER_RATE_LIMIT_MIN_COOLDOWN_MS` | LCE 对限流失败 API Key 的最短冷却时间 | `1000` |
| `EMBEDDINGS_NETWORK_MAX_RETRIES` | embedding 网络/超时/5xx 单次调用的额外重试次数，可设为 `0` | `3` |
| `EMBEDDINGS_RATE_LIMIT_MAX_RETRIES` | embedding 429 单次调用的额外重试次数，可设为 `0` | `5` |
| `EMBEDDINGS_RETRY_INITIAL_DELAY_MS` | embedding 网络类重试初始延迟 | `1000` |
| `EMBEDDINGS_RATE_LIMIT_MIN_BACKOFF_MS` | embedding 限流自适应退避下限 | `5000` |
| `EMBEDDINGS_RATE_LIMIT_MAX_BACKOFF_MS` | embedding 限流自适应退避上限 | `60000` |
| `EMBEDDINGS_RATE_LIMIT_SUCCESS_INCREASE_COUNT` | 连续成功多少次后提高 embedding 并发 | `3` |
| `EMBEDDINGS_RATE_LIMIT_BACKOFF_DECAY_SUCCESSES` | 连续成功多少次后衰减限流退避 | `10` |
| `EMBEDDINGS_RATE_LIMIT_SLOT_POLL_MS` | embedding 自适应并发槽等待轮询间隔 | `50` |
| `RERANK_MAX_ATTEMPTS` | rerank 单次调用最大尝试次数 | `3` |
| `RERANK_RETRY_MAX_DELAY_MS` | rerank 重试延迟上限 | `10000` |
| `RERANK_RATE_LIMIT_RETRY_BASE_MS` | rerank 限流重试基础延迟 | `1000` |
| `RERANK_TRANSIENT_RETRY_BASE_MS` | rerank 临时错误重试基础延迟 | `500` |
| `QUERY_REWRITE_MAX_QUERIES` | 单次查询改写保留的子查询上限 | `3` |
| `QUERY_REWRITE_TIMEOUT_MS` | 查询改写供应商调用超时 | `4000` |
| `QUERY_REWRITE_CACHE_MAX_ENTRIES` | 查询改写进程内缓存容量 | `256` |
| `PROMPT_ENHANCER_TIMEOUT_MS` | 提示增强供应商调用超时 | `30000` |
| `PROMPT_ENHANCER_MAX_CONCURRENT_REQUESTS` | 提示增强最大并发调用数 | `4` |
| `PROMPT_ENHANCER_MAX_RESPONSE_BYTES` | 提示增强响应体字节上限 | `524288` |
| `PROMPT_ENHANCER_MAX_OUTPUT_TOKENS` | 提示增强请求的最大输出 token 数 | `1200` |
| `PROMPT_ENHANCER_MAX_ATTEMPTS_PER_KEY` | 提示增强每个 API Key 的最大尝试次数 | `2` |
| `PROMPT_ENHANCER_RETRY_MIN_DELAY_MS` | 提示增强重试最短延迟 | `250` |
| `PROMPT_ENHANCER_RETRY_DEFAULT_DELAY_MS` | 提示增强无 Retry-After 时的默认延迟 | `500` |
| `PROMPT_ENHANCER_RETRY_MAX_DELAY_MS` | 提示增强重试最长延迟 | `2000` |
| `LCE_PLATFORM_MODEL_CONFIG_BODY_LIMIT_BYTES` | 前端模型配置请求体上限 | `65536` |
| `LCE_PLATFORM_MODEL_CONFIG_RESPONSE_LIMIT_BYTES` | 前端读取模型配置响应上限 | `65536` |
| `LCE_PROVIDER_MODEL_DISCOVERY_RESPONSE_LIMIT_BYTES` | 前端供应商模型列表响应上限 | `1048576` |
| `AUTH_MIN_PASSWORD_LENGTH` / `AUTH_MAX_PASSWORD_LENGTH` | 前端密码长度策略 | `8` / `128` |
| `AUTH_EMAIL_VERIFICATION_TTL_SECONDS` | 邮箱验证链接有效期 | `3600` |
| `AUTH_ORGANIZATION_INVITATION_TTL_SECONDS` | 组织邀请有效期 | `172800` |
| `AUTH_GITHUB_MIN_ACCOUNT_AGE_DAYS` | GitHub 登录允许的最小账号年龄；`0` 表示关闭该检查 | `365` |
| `SMTP_MAX_CONNECTIONS` / `SMTP_MAX_MESSAGES` | SMTP 连接池容量与单连接消息上限 | `3` / `100` |
| `SMTP_CONNECTION_TIMEOUT_MS` / `SMTP_GREETING_TIMEOUT_MS` / `SMTP_SOCKET_TIMEOUT_MS` | SMTP 连接、问候和套接字超时 | `10000` / `10000` / `20000` |
| `REDIS_URL` | 前端 Redis 完整连接 URL；设置后优先于 host/port | 空 |
| `PAYMENT_PROVIDER_REQUEST_TIMEOUT_MS` | 支付供应商请求超时 | `10000` |
| `PAYMENT_WEBHOOK_MAX_AGE_SECONDS` | 支付回调允许的最大时间偏差 | `300` |
| `PAYMENT_ORDER_TTL_MINUTES` | 待支付订单有效期 | `15` |
| `LEADERBOARD_UPDATE_INTERVAL` | 排行榜聚合周期 | `30m` |
| `LEADERBOARD_TOP_N` | 每日排行榜保留条目数 | `10` |
| `LEADERBOARD_TIMEZONE` | 排行榜和每日配额的自然日时区 | `Asia/Shanghai` |
| `HEALTH_CHECK_INTERVAL` | LCE 健康探测周期 | `2m` |
| `HEALTH_CHECK_TIMEOUT` | 单次健康探测超时 | `30s` |
| `MCP_HTTP_IDLE_CONN_TIMEOUT` | Relay 到 LCE 的 HTTP 空闲连接超时 | `90s` |
| `MCP_HTTP_MAX_IDLE_CONNS` | Relay 到 LCE 的最大空闲连接数 | `50` |
| `MCP_HTTP_MAX_IDLE_CONNS_PER_HOST` | 每个 LCE 主机的最大空闲连接数 | `50` |
| `MCP_SESSION_TTL` | Relay MCP session 空闲寿命 | `30m` |
| `MCP_SESSION_SWEEP_INTERVAL` | 过期 MCP session 扫描周期 | `1m` |
| `MCP_TOOLS_CACHE_TTL` | LCE 工具列表缓存时长 | `5m` |
| `MCP_MAX_SESSIONS` | Relay MCP session 总上限 | `1000` |
| `MCP_MAX_SESSIONS_PER_USER` | 单租户 MCP session 上限 | `16` |
| `DB_MAX_OPEN_CONNS` | PostgreSQL 最大打开连接数 | `40` |
| `DB_MAX_IDLE_CONNS` | PostgreSQL 最大空闲连接数 | `40` |
| `DB_CONN_MAX_LIFETIME` | PostgreSQL 连接最大寿命 | `30m` |
| `DB_CONN_MAX_IDLE_TIME` | PostgreSQL 连接最大空闲时间 | `5m` |
| `AUTH_CACHE_POSITIVE_TTL` | API Key 有效身份缓存时长 | `30s` |
| `AUTH_CACHE_NEGATIVE_TTL` | 无效 API Key 缓存时长 | `5s` |
| `AUTH_CACHE_MAX_ENTRIES` | 进程内 API Key 缓存容量 | `10000` |
| `MODEL_CONFIG_CACHE_TTL` | 用户模型配置缓存时长 | `5m` |
| `QUOTA_CACHE_TTL` | 配额上限缓存时长 | `5m` |
| `QUOTA_COUNTER_TTL` | 每日配额计数器兜底 TTL | `48h` |
| `REQUEST_LOG_STALE_AFTER` | pending 请求日志判定为遗留记录的阈值 | `15m` |
| `REQUEST_LOG_RECONCILE_INTERVAL` | 遗留 pending 日志回收周期 | `5m` |
| `CLEAR_INDEX_COOLDOWN` | 全量清空索引的租户级冷却时间 | `72h` |
| `DELETE_ROOT_MIN_INTERVAL` | 同一租户与 root 的删除保护间隔 | `1m` |
| `DELETE_ROOT_MEMORY_ENTRIES` | 删除保护窗口的进程内记录上限 | `4096` |
| `INDEX_OPERATION_LEASE_DURATION` | 索引破坏性操作租约时长 | `2m` |
| `INDEX_OPERATION_RENEW_INTERVAL` | 索引操作租约续期间隔，必须短于租约时长 | `30s` |
| `INDEX_OPERATION_ACQUIRE_POLL_INTERVAL` | 索引操作租约竞争轮询间隔 | `100ms` |
| `INDEX_OPERATION_DB_TIMEOUT` | 索引操作租约续期/释放的数据库超时 | `5s` |

供应商异常按可恢复性分类，而不是无限重试：网络错误、超时、HTTP 408/429/5xx 使用上表的有界预算；鉴权失败、余额/计费不足、非法输入、输出维度不兼容及其他确定性 4xx 会立即停止当前快照的重复尝试。修复配置、补充余额或更换供应商/模型后，保存配置会改变供应商指纹并使旧向量失效；客户端应重新建立索引。临时错误在预算耗尽后暂停，网络恢复、仓库快照变化或后续索引调用会再次触发同步，而不是在后台永久空转。

### PostgreSQL 配置

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `DB_HOST` | 数据库主机 | `localhost` |
| `DB_PORT` | 数据库端口 | `5432` |
| `DB_USER` | 数据库用户名 | `postgres` |
| `DB_PASSWORD` | 数据库密码 | （空） |
| `DB_NAME` | 数据库名称 | `postgres` |

### Redis 配置

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `REDIS_PORT` | Redis 端口 | `6379` |

### 访问控制与配额

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `CONSOLE_API_SECRET` | 网页控制台调用索引统计/清除接口的内部凭据，须与前端 `BETTER_AUTH_SECRET` 相同；Compose 部署已自动传入 | （空） |
| `BANNED_CACHE_TTL` | 账号封禁查询的 Redis 正/负缓存 TTL | `5m` |
| `DEFAULT_DAILY_REQUEST_LIMIT` | 每用户每日请求上限默认值（Asia/Shanghai 自然日），超限返回 429；`0` 不限。可在控制台「配额管理」按用户覆盖（`user_quotas` 表，改后即时生效） | `0` |
| `DAILY_INDEX_BYTES_LIMIT` | 每用户每日索引字节上限（Asia/Shanghai 自然日），`0` 表示不限。每次索引上传同时计入常规请求数配额和实际上传内容的字节配额 | `2147483648`（2 GiB） |
| `MODEL_CONFIG_SECRET` | 按用户 rerank 配置的加密密钥，须与前端设置相同；未设置时关闭。用户密钥以 AES-256-GCM 加密存于 `user_model_configs`，Relay 解密后仅按检索请求注入 rerank。云端 embedding 和向量空间始终由 LCE 服务端控制，修改 rerank 不会清空或重建索引 | （空） |

封禁：管理员在控制台「用户管理」封禁账号后写入 `banned_users` 表，relay 对该用户
所有请求返回 403（缓存 `banned:{user}`，封禁/解封即时生效）。API Key 删除或重置后，
Relay 的进程内认证缓存撤销延迟最多为 `AUTH_CACHE_POSITIVE_TTL`；客户端身份只由 API Key 决定。

## 数据库表结构

服务启动时会自动迁移创建以下表：

- **`request_logs`**：请求日志，记录每个请求的用户、路径、状态码、耗时等；日志 INSERT 为异步写入（channel 协调，确保后续 UPDATE / 外键操作等待 INSERT 完成），并在 `(user_id, request_timestamp)` 上建有复合索引
- **`error_details`**：错误详情，关联到 request_logs，区分代理层（proxy）和上游（upstream）错误
- **`leaderboard`**：每日用户代码检索与提示词增强调用量排行榜快照
- **`health_checks`**：LCE MCP 健康检查历史，记录状态、延迟、错误信息及下次检查时间
- **`index_workspaces`**：用户工作区最近一次完成索引的分支、代码 revision、LCE cloud revision 和时间
- **`index_jobs`**：索引任务状态、固定 `root_id`、阶段、文件/chunk 进度、心跳和错误；完成状态只在符号图收敛成功后提交
- **`index_job_files`**：运行中任务的 manifest 和批次提交状态，任务完成、失败或超时后回收
- **`indexed_files`**：服务端已完成索引的工作区文件快照，用于后续增量比较和删除检测
- **`index_operation_leases`**：串行化同一工作区的发布、清理等破坏性操作
- **`banned_users`**：账号封禁状态
- **`user_quotas` / `org_quotas` / `org_member_quotas`**：个人、组织及组织成员配额覆盖
- **`user_model_configs`**：按用户加密保存的 rerank 配置

> 数据库连接池容量、连接寿命和空闲时间由 `DB_MAX_*` / `DB_CONN_*` 运行策略控制。

## 部署形态

**relay 为单实例设计**：认证缓存、`codebase_index start` 频率限制、MCP session 均为进程内状态；各自的 TTL、容量和保护窗口由运行策略配置。横向扩展（多副本）前需将这些状态外置到 Redis，否则各副本状态不一致（缓存各自失效、限流被放大 N 倍、session 无法跨副本路由）。

API key 撤销语义：前端封禁账号、删除或重置密钥后，旧 key 仍可能在 `AUTH_CACHE_POSITIVE_TTL` 窗口内通过进程内缓存认证；不提供跨进程即时失效机制。

### 部署配置与主机策略

部署脚本不再假设三个仓库位于固定的兄弟目录；可通过 `DEPLOY_LCE_DIR`、`DEPLOY_RELAY_DIR`、`DEPLOY_FRONTEND_DIR` 指向实际检出位置，分支、版本与 Docker 清理策略也都由 `DEPLOY_*` 变量控制。`deploy/.env.example` 列出了 Compose 使用的全部应用运行策略，避免必须修改编排文件才能调整地址、限额、重试或客户端节奏。

Nginx 配置使用 `deploy/nginx.conf.template`，域名、证书、上游、上传大小和代理超时由专用 `deploy/nginx.env` 提供。先复制 `deploy/nginx.env.example` 并按目标主机修改，再运行 `deploy/render-nginx-config.sh` 生成忽略提交的 `deploy/nginx.conf.rendered`。渲染器只替换明确列出的部署变量，不会误替换 Nginx 自身的 `$host`、`$request_uri` 等变量。

`deploy/tune-host.sh` 的 sysctl 文件位置、Nginx/PostgreSQL 服务身份以及容量参数同样由 `DEPLOY_*` 变量覆盖。脚本会在写入主机配置前验证正整数和端口/连接数关系；这些默认值是可审查的运维基线，不是散落在业务代码里的固定策略。

## 已知限制

- chunk 数优先采用 LCE 返回值；LCE 未返回精确数量时，Relay 会使用 Agent 在 manifest 中提交的估算值并将 `chunk_count_estimated` 标记为 `true`。

## 日志

服务日志同时输出到控制台和 `gin.log` 文件。
