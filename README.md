# acemcp-relay

LCE 的多租户远程 MCP 服务。IDE Agent 只需配置一个 Streamable HTTP MCP 地址，即可完成代码索引、语义检索、符号关系查询和索引统计。服务基于 Gin 构建，并使用 PostgreSQL 保存工作区索引快照、任务进度和请求记录。

## 功能特性

- **纯 MCP 首次与增量索引**：`codebase_index` 比较 Agent 提交的完整工作区 manifest 与服务端快照，首次返回全量文件，后续只返回新增或变更文件并检测删除文件
- **索引任务进度**：持久化任务阶段、文件数、chunk 数、分支、revision、心跳和完成状态
- **LCE MCP 对接**：对外通过 `codebase_index` 同步索引、通过 `codebase-retrieval` 执行向量召回和精排；底层索引原语不向客户端暴露
- **API Key 认证**：基于 Bearer Token 的认证中间件，API Key 存于 PostgreSQL（`api_keys.id` 为 key 的哈希 hex，兼容存量 MD5 与新的 SHA-256 双读）；relay 进程内缓存认证结果（正缓存 30s / 负缓存 5s），封禁、删除、重置 key 的撤销延迟最多 30 秒
- **请求日志**：自动记录每个请求的状态、耗时、来源 IP 等信息到 PostgreSQL
- **错误追踪**：异步记录代理层和上游服务的错误详情
- **任务回收**：索引任务完成或失败后删除暂存文件；运行任务心跳超时后自动标记并回收
- **使用排行榜**：定时统计成功的 `codebase-retrieval` 与 `codebase_enhance_prompt` 调用量（每 30 分钟更新，不含索引、符号图和协议请求）
- **健康检查**：每 2 分钟调用 LCE MCP `tools/list`，将可用性和延迟写入 `health_checks`
- **请求/响应压缩**：对上游请求体使用 zstd 压缩（`SpeedFastest` 等级，小于 1024 字节的 payload 跳过压缩）；响应按客户端 `Accept-Encoding` 协商编码（`br` / `gzip` / `deflate` / `identity`，brotli 等级 4），压缩失败时回退到 identity
- **性能观测**：内置 pprof 服务（仅监听 `127.0.0.1:6060`），用于运行时 CPU / 内存 profiling

## 支持的 API 路径

| 路径 | 说明 |
|------|------|
| `POST /mcp` | MCP Streamable HTTP 入口，承载初始化、工具发现和全部工具调用 |
| `DELETE /mcp` | 关闭当前 MCP session |
| `GET /mcp` | 返回 405；服务不提供 SSE 推流 |
| `GET /mcp/tenant-stats` | 网页控制台内部索引统计接口；要求控制台内部凭据 |
| `POST /mcp/clear-index` | 网页控制台内部清理接口；要求控制台内部凭据及冷却校验 |

索引和检索均只走 MCP；不保留插件专用的 `/relay/*` REST 数据面，也不提供旧 Augment `/find-missing`、`/batch-upload`、`/checkpoint-blobs` 协议。

### MCP 工具边界

远程 MCP 对模型只暴露 5 个租户安全工具：

- `codebase-retrieval`：在租户服务端索引上执行向量召回与精排；向量空间由 LCE 固定，Relay 只按用户注入可选 rerank 配置。
- `codebase_symbol_graph`：查询指定 `root_id` 的服务端符号图。
- `codebase_enhance_prompt`：基于租户索引中检索到的代码上下文增强自然语言任务；管理员必须先配置并启用提示词增强模型。
- `codebase_index_status`：查询一个或全部项目根的索引可用性、同步阶段、文件进度与失败原因；已有快照可用时会同时标明后台更新状态。
- `codebase_index`：不依赖 IDE 插件的索引入口。Agent 使用自身文件读取能力提交完整 manifest，再按 `pending_files` 分批上传内容，最后完成任务并收敛符号图。

`codebase_tenant_stats` 是网页控制台的内部统计接口，不会出现在远程 MCP 的 `tools/list` 中。

`codebase_index` 的操作顺序为 `start -> upload -> complete`，可用 `status` 同步续期 Relay 与 LCE 的任务租约并查询进度，失败时调用 `fail` 回收任务。服务端负责注入 `tenant_id`，并强制执行 SHA-256 内容匹配、路径过滤、manifest/批次上限、每日索引字节配额、root 隔离和删除检测。Relay 使用同一个 job UUID 调用 LCE 的内部 `begin -> stage/renew -> publish/abort` 协议；首次 full job 以 `replace_root=true` 发布完整根快照，能够清除仅存在于云端的旧文件。PostgreSQL 中的词法、精确、向量和经编译器细化的符号图数据只在 publish 时原子可见。`codebase_remote_index` 和 `codebase_clear_index` 是内部控制面，不能由模型直接调用。

远端服务无法读取 IDE 所在机器的文件系统或 `.git` 目录。Relay 的 Streamable HTTP `/mcp` 端点保留为 npm 客户端和服务内部调用的传输层；控制台不再把它作为普通用户的独立接入方式，因为直接配置它缺少 npm 客户端的本地 Git 工具、文件监听、自动增量同步和分支视图跟踪。

使用 `npx -y @anmezing/lce-cloud@latest` 时，npx 会自动下载并运行 npm 客户端，无需全局安装。npm 客户端向 Agent 暴露检索、符号图、提示词增强、索引状态以及两个本地 Git 工具，并负责文件监听、自动增量索引与分支视图跟踪。控制台生成的 npx 配置会提供完整的 6 个工具；底层 `codebase_index` 同步协议由客户端自动调用，不作为第七个面向 Agent 的工具暴露。

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
| `SERVER_ADDR` | 服务监听地址 | `127.0.0.1:8080` |
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
| `DAILY_INDEX_BYTES_LIMIT` | 每用户每日索引字节上限（Asia/Shanghai 自然日），`0` 表示不限。每次 `codebase_index upload` 同时计入常规 MCP 请求配额和实际上传内容的字节配额；单文件与单批原始内容上限均为 512 KiB，完整 manifest 所在的客户端 MCP 请求体上限为 32 MiB，转发给 LCE 的单次 JSON-RPC 请求体上限为 4 MiB | `2147483648`（2 GiB） |
| `MODEL_CONFIG_SECRET` | 按用户 rerank 配置的加密密钥，须与前端设置相同；未设置时关闭。用户密钥以 AES-256-GCM 加密存于 `user_model_configs`，Relay 解密后仅按检索请求注入 rerank。云端 embedding 和向量空间始终由 LCE 服务端控制，修改 rerank 不会清空或重建索引 | （空） |

封禁：管理员在控制台「用户管理」封禁账号后写入 `banned_users` 表，relay 对该用户
所有请求返回 403（缓存 `banned:{user}`，封禁/解封即时生效）。API Key 删除或重置后，
Relay 的进程内认证缓存最多保留 30 秒；客户端身份只由 API Key 决定。

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

> 数据库连接池配置为最多 25 个打开/空闲连接，连接生命周期 30 分钟，以减少 SCRAM-SHA-256 认证带来的 CPU 开销。

## 部署形态

**relay 为单实例设计**：认证缓存（api key 30s 正缓存 / 5s 负缓存）、`codebase_index start` 频率限制、MCP session 均为进程内状态。横向扩展（多副本）前需将这些状态外置到 Redis，否则各副本状态不一致（缓存各自失效、限流被放大 N 倍、session 无法跨副本路由）。

API key 撤销语义：前端封禁账号、删除或重置密钥后，旧 key 仍可能通过 relay 进程内缓存认证最多 30 秒；这是已确认可接受的延迟，不提供跨进程即时失效机制。

## 已知限制

- chunk 数优先采用 LCE 返回值；LCE 未返回精确数量时，Relay 会使用 Agent 在 manifest 中提交的估算值并将 `chunk_count_estimated` 标记为 `true`。

## 日志

服务日志同时输出到控制台和 `gin.log` 文件。
