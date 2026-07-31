# acemcp-relay

Go HTTP relay 服务，用于在 LCE Coding Agent 插件与 LCE MCP 服务之间协调代码索引和检索。基于 Gin 构建，并使用 PostgreSQL 保存工作区索引快照、任务进度和请求记录。

## 功能特性

- **首次与增量索引**：比较客户端工作区 manifest 与服务端快照，首次返回全量文件，后续只返回新增或变更文件并检测删除文件
- **索引任务进度**：持久化任务阶段、文件数、chunk 数、分支、revision、心跳和完成状态
- **LCE MCP 对接**：通过 `codebase_remote_index` 执行索引，通过 `codebase-retrieval` 执行向量召回和精排
- **API Key 认证**：基于 Bearer Token 的认证中间件，通过 PostgreSQL 存储 API Key，Redis 缓存加速验证
- **请求日志**：自动记录每个请求的状态、耗时、来源 IP 等信息到 PostgreSQL
- **错误追踪**：异步记录代理层和上游服务的错误详情
- **任务回收**：索引任务完成或失败后删除暂存文件；运行任务心跳超时后自动标记并回收
- **使用排行榜**：定时统计 LCE `codebase-retrieval` 工具请求量（每 30 分钟更新）
- **健康检查**：每 2 分钟调用 LCE MCP `tools/list`，将可用性和延迟写入 `health_checks`
- **请求/响应压缩**：对上游请求体使用 zstd 压缩（`SpeedFastest` 等级，小于 1024 字节的 payload 跳过压缩）；响应按客户端 `Accept-Encoding` 协商编码（`br` / `gzip` / `deflate` / `identity`，brotli 等级 4），压缩失败时回退到 identity
- **性能观测**：内置 pprof 服务（仅监听 `127.0.0.1:6060`），用于运行时 CPU / 内存 profiling

## 支持的 API 路径

### Relay 数据面

| 路径 | 说明 |
|------|------|
| `GET /relay/capabilities` | 返回版本化索引协议与服务端 manifest、batch、单文件体积限制；客户端按服务端与自身上限的交集规划上传 |
| `POST /relay/index-jobs` | 提交带 `protocolVersion: 1` 与非空 `rootId` 的工作区 manifest，创建全量或增量索引任务并返回待索引、待删除文件；检测到变化的 workspace-root 绑定时，仅清理 LCE 中的旧 root，并失效 Relay 中绑定该旧 root 的快照，其他仓库不受影响 |
| `GET /relay/index-jobs/:id` | 查询任务阶段、文件进度、chunk 进度和状态，同时刷新任务心跳 |
| `POST /relay/remote-index` | 上传一个任务批次并调用 LCE `codebase_remote_index` |
| `POST /relay/index-jobs/:id/complete` | 完成任务：先让 LCE 修复并收敛该 root 的服务端符号图，再提交工作区快照并回收暂存文件 |
| `POST /relay/index-jobs/:id/fail` | 标记任务失败并回收任务暂存文件 |

Relay 不提供旧 Augment `/find-missing`、`/batch-upload`、`/checkpoint-blobs` 或 REST 检索协议；索引统一走版本化 index-job 链路，检索统一走 MCP。

### MCP 工具边界

Relay 的远程 MCP 入口只向模型暴露 3 个租户安全工具：

- `codebase-retrieval`：在租户服务端索引上执行向量召回与精排；Relay 按用户注入 embedding/rerank 模型配置。
- `codebase_symbol_graph`：查询指定 `root_id` 的服务端符号图。
- `codebase_tenant_stats`：查询当前租户的聚合索引统计。

`codebase_remote_index`、`codebase_find_missing` 和 `codebase_clear_index` 属于受保护的索引/控制面，不通过模型可调用的 MCP 入口暴露。LCE Code 插件会另外启动一个本地 stdio MCP，提供 `codebase_git_context` 与 `codebase_review_changes`；因此插件内看到的是 3 个远程工具加 2 个本地工具。Git 证据直接作为本地 MCP 的工具结果返回给调用方，不会被 Review 工具转发给 Relay；Review 只把检索请求交给 Relay，向量检索和精排仍在服务端执行。

## 技术栈

- **语言**：Go 1.25
- **Web 框架**：Gin
- **数据库**：PostgreSQL（请求日志、排行榜、API Key 存储）
- **缓存**：Redis（API Key 缓存）
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

### LCE MCP 配置

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `LCE_MCP_URL` | LCE MCP HTTP endpoint | `http://127.0.0.1:3000/mcp` |
| `LCE_TENANT_ASSERTION_SECRET` | 与 LCE 共享的租户断言密钥（≥32 字符）。relay 用它为每次带 `tenant_id` 的调用签发短时效断言，LCE 校验通过才认这个租户 | 空（不签发） |

`tenant_id` 在 MCP 里只是普通工具入参，LCE 无法区分授权调用与伪造调用——不做校验时，任何能连到 LCE 端口的人都可以读、写、甚至用 `codebase_clear_index` 清空**任意**租户的索引。配置该密钥后，租户隔离不再只依赖"LCE 端口不对外暴露"这一条前提。

健康探测走 LCE 的 `GET {LCE_MCP_URL}/health`：不建立 session、不占用 LCE 的请求并发额度。不要用完整的 `initialize` 当探针——每次探测都会占掉一个 session 名额，且只在空闲 TTL 后释放，稳态下会把自己挡在 503 外面。

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
### 会话配置

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `SESSION_TTL` | 模拟 CLI/插件 session 的 Redis TTL（Go duration 格式） | `5m` |

### 设备绑定（防账号共用）

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `DEVICE_BINDING_MODE` | `off` 不校验；`log` 只记告警不拦截；`enforce` 未注册设备返回 401 | `log` |
| `DEVICE_CACHE_TTL` | 设备注册状态的 Redis 缓存 TTL | `5m` |
| `DEVICE_IP_WINDOW` | 同设备多 IP 检测的滑动窗口 | `10m` |
| `DEVICE_MAX_IPS` | 窗口内允许的最大来源 IP 数，超过写 `device_alerts` 告警；`0` 关闭检测 | `3` |
| `CONSOLE_API_SECRET` | 网页控制台调用索引统计/清除接口的内部凭据，须与前端 `BETTER_AUTH_SECRET` 相同；Compose 部署已自动传入 | （空） |
| `DEFAULT_DAILY_REQUEST_LIMIT` | 每用户每日请求上限默认值（Asia/Shanghai 自然日），超限返回 429；`0` 不限。可在控制台「配额管理」按用户覆盖（`user_quotas` 表，改后即时生效） | `0` |
| `DAILY_INDEX_BYTES_LIMIT` | 每用户每日索引字节上限（Asia/Shanghai 自然日），超限返回 429；`0` 不限。索引通道**不能**靠请求数配额约束：创建一次 job 只计 1 次请求，之后的 `/relay/remote-index` 批次全部豁免（一次扫描上千批，按请求计费会误伤正常用户），而 embedding 成本全在那些批次上——无此上限时 1 次请求配额可驱动上百 GB 的 embedding 调用。单文件上限 1 MiB、单批上限 8 MiB 为硬编码常量 | `2147483648`（2 GiB） |
| `MODEL_CONFIG_SECRET` | 按用户模型配置（BYO 模型）的加密密钥，须与前端设置相同的值；未设置时该特性关闭。用户在控制台「模型设置」配置自己的 embedding/rerank（含自己的 API key，AES-256-GCM 加密存于 `user_model_configs`），relay 解密后按请求注入 LCE；配置变化时 relay 自动清空该租户索引，插件下次扫描全量重建 | （空） |

设备在前端 `/api/auth/device` 登录时注册（插件上报 `vscode.env.machineId`）。
活跃设备数由前端 `MAX_DEVICES_PER_USER` 控制，默认 `1`，即**单设备互踢**：
新设备登录立即踢掉旧设备，被踢设备在 `enforce` 模式下收到 401，必须重新走网页
登录才能使用 —— 换设备无感，共用账号则互相踢下线且每次都要重新 OAuth。
单设备模式下，**换设备（发生互踢）时还会轮换 API token**：被踢机器上的旧 token
（包括被复制走的副本）立即失效；此防线不依赖 `DEVICE_BINDING_MODE`，部署前端后
立即生效。同一台机器的重复登录（多个 IDE / 多窗口 / 重装，machineId 相同）不触发
互踢也不轮换 token，同机多 IDE 各自登录一次即可共用当前 token。
每次踢出都会写一条 `device_evicted` 告警，频繁互踢即共用信号：

```sql
-- 最近 24h 换设备（互踢）次数排行，次数高的基本就是共用账号
SELECT user_id, COUNT(*) AS evictions
FROM device_alerts
WHERE kind = 'device_evicted' AND created_at > NOW() - INTERVAL '1 day'
GROUP BY user_id ORDER BY evictions DESC LIMIT 20;
```

灰度路径：先发插件新版本（默认 `log` 模式观察告警），确认老客户端换代完成后切 `enforce`。
封禁：管理员在控制台「用户管理」封禁账号后写入 `banned_users` 表，relay 对该用户
所有请求返回 403（缓存 `banned:{user}`，封禁/解封即时生效）；设备登录同样被拒。
排查：`SELECT * FROM device_alerts ORDER BY created_at DESC LIMIT 50;`
（`missing_client_id` = 老版本插件或非插件流量；`unregistered_device` = 未登录注册或已被踢的设备；`multi_ip` = 疑似 token 被复制共用；`device_evicted` = 新设备登录踢出旧设备。）

## 数据库表结构

服务启动时会自动迁移创建以下表：

- **`request_logs`**：请求日志，记录每个请求的用户、路径、状态码、耗时等；日志 INSERT 为异步写入（channel 协调，确保后续 UPDATE / 外键操作等待 INSERT 完成），并在 `(user_id, request_timestamp)` 上建有复合索引
- **`error_details`**：错误详情，关联到 request_logs，区分代理层（proxy）和上游（upstream）错误
- **`leaderboard`**：每日用户请求量排行榜
- **`health_checks`**：LCE MCP 健康检查历史，记录状态、延迟、错误信息及下次检查时间
- **`index_workspaces`**：用户工作区最近一次完成索引的分支、revision 和时间
- **`index_jobs`**：索引任务状态、固定 `root_id`、阶段、文件/chunk 进度、心跳和错误；完成状态只在符号图收敛成功后提交
- **`index_job_files`**：运行中任务的 manifest 和批次提交状态，任务完成、失败或超时后回收
- **`indexed_files`**：服务端已完成索引的工作区文件快照，用于后续增量比较和删除检测

> 数据库连接池配置为最多 25 个打开/空闲连接，连接生命周期 30 分钟，以减少 SCRAM-SHA-256 认证带来的 CPU 开销。

## 已知限制

- chunk 数优先采用 LCE 返回值；LCE 未返回精确数量时，relay 会使用客户端估算值并将 `chunkCountEstimated` 标记为 `true`。

## 日志

服务日志同时输出到控制台和 `gin.log` 文件。
