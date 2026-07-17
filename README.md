# 🦀 Crab.rs

基于 QUIC 的点对点远程管理工具。

## 它能做什么

Crab 是一个点对点的 QUIC 通信工具。你把两个实例连起来，然后就可以：

- 在对方机器上**读写文件**（流式传输，支持大文件）
- 在对方机器上**删除文件或目录**
- 把对方的 HTTP 请求**代理**到本地网络
- 把本地端口**转发**到对方内网的服务（TCP 隧道）

所有通信走 QUIC + TLS 1.3，加密和连接复用开箱即用。

## 架构

实例之间对等连接，谁都可以向谁发命令。你只需决定自己是**监听模式**（等人连）还是**连接模式**（主动去连别人）：

```
实例 A（监听模式）                 实例 B（连接模式）
listen = true                      listen = false
                                    remote_addr = ["A:443"]
     │                                  │
     └────────── QUIC 连接 ─────────────┘
                        │
                TCP 转发 / 文件读写 / HTTP 代理
```

一个实际场景（端口转发）：

```
你的浏览器 → 本地 :8080 → [Crab A]
                           QUIC 加密隧道
                          [Crab B] → 内网服务 :80
```

## 命令

| 命令 | 作用 |
|------|------|
| `Ping` | 测试连通性 |
| `Delete` | 删除文件或目录 |
| `ReadFile` | 读取文件内容（流式） |
| `WriteFile` | 写入文件内容（流式） |
| `HttpProxy` | 让对端代发 HTTP 请求 |
| `TCPForward` | 本地端口转发到对端内网 |

## 文件读写

读文件分两阶段：先取元数据（大小、类型、修改时间），再决定是否拉数据流。

写文件分两阶段：先确认可写，再传数据流。支持自动创建目录、覆盖保护，写入先到临时文件，完成后原子重命名。

## TCP 端口转发

配置 TCP 转发规则后，Crab 会在本地监听一个端口，进来的连接通过 QUIC 隧道送到对端，由对端连接到目标地址：

```toml
[[tcp]]
listen = "127.0.0.1:8080"      # 本机监听的端口
target = "b-host"              # 对端实例的 node_id
[tcp.params]
target_address = "10.0.0.1:80"  # 对端要连接的内网地址
connect_timeout = 5
keepalive_timeout = 60
keepalive_interval = 15
keepalive_retries = 3
```

访问 `http://127.0.0.1:8080`，流量路径：

```
你 → TCP :8080 → [Crab A] → QUIC 隧道 → [Crab B] → TCP :80 → 内网服务
```

多规则示例：

```toml
[[tcp]]
listen = "127.0.0.1:2222"
target = "jump-box"
[tcp.params]
target_address = "192.168.1.1:22"

[[tcp]]
listen = "127.0.0.1:3306"
target = "db-node"
[tcp.params]
target_address = "10.0.0.5:3306"
```

参数说明：

| 参数 | 默认 | 说明 |
|------|------|------|
| `target_address` | — | 对端要连接的 TCP 目标 |
| `connect_timeout` | 5 | 对端连接目标的超时（秒） |
| `keepalive_timeout` | 60 | TCP keepalive 空闲超时 |
| `keepalive_interval` | 15 | TCP keepalive 探测间隔 |
| `keepalive_retries` | 3 | TCP keepalive 重试次数 |

## HTTP 代理

让对端代发 HTTP 请求，适用于跨网络访问资源（需要启用 `api` feature）：

```bash
# 让节点 node-b 代为请求 http://example.com/api
curl -x http://127.0.0.1:3000/ctrl/node-b/proxy \
  -H "x-target-host: http://example.com" \
  http://example.com/api
```

所有标准 HTTP 方法都支持（GET、POST、PUT、DELETE 等），请求体和响应体都会流式透传。

## HTTP API（需要 `api` feature）

启用 HTTP API 后，可以通过 curl 直接向节点发命令。API 地址默认由 `endpoint.bind_address` 指定，路由前缀为 `/ctrl/{node_id}/`。

所有接口的响应格式：

```json
{
  "err_no": 0,
  "msg": "success",
  "data": null
}
```

### 查看节点信息

```bash
curl http://127.0.0.1:3000/ctrl/node-b
```

返回握手时提交的设备信息（主机名、系统版本、磁盘等）。

### Ping

```bash
curl http://127.0.0.1:3000/ctrl/node-b/ping
```

### 删除文件或目录

```bash
# 删除文件
curl -X DELETE "http://127.0.0.1:3000/ctrl/node-b/dir?path=/tmp/foo"

# 删除目录
curl -X DELETE "http://127.0.0.1:3000/ctrl/node-b/dir?path=/tmp/dir&dir=true"
```

### 读取文件

```bash
curl -o output.bin "http://127.0.0.1:3000/ctrl/node-b/file?path=/data/dump.bin"
```

返回文件内容（流式），先返回 JSON 头（含 `Content-Length`），后跟文件二进制流。

### 写入文件

```bash
# 简单写入
echo "hello" | curl -X POST \
  "http://127.0.0.1:3000/ctrl/node-b/file?path=/tmp/hello.txt&overwrite=true" \
  --data-binary @-

# 自动创建目录
curl -X POST \
  "http://127.0.0.1:3000/ctrl/node-b/file?path=/tmp/new/log.txt&mkdir=true" \
  --data-binary @log.txt
```

### HTTP 代理

```bash
# 让 node-b 代为请求一个网页
curl http://127.0.0.1:3000/ctrl/node-b/proxy \
  -H "x-target-host: https://httpbin.org" \
  http://httpbin.org/get

# 带路径的写法
curl http://127.0.0.1:3000/ctrl/node-b/proxy/httpbin.org/get \
  -H "x-target-host: https://httpbin.org"

# POST 请求也透传
curl -X POST http://127.0.0.1:3000/ctrl/node-b/proxy \
  -H "x-target-host: https://httpbin.org" \
  http://httpbin.org/post \
  -H "Content-Type: application/json" \
  -d '{"hello": "world"}'
```

`x-target-host` 头指定目标 HTTP 服务的地址（必填），请求的 URL 路径和查询参数会拼接到目标地址后面。

## 快速开始

### 1. 准备证书

```bash
openssl req -x509 -newkey rsa:4096 \
  -keyout private.key -out cert.pem -days 365 -nodes
```

### 2. 启动 A（监听模式）

```toml
# a.toml
node_id = "host-a"

[endpoint]
bind_address = "0.0.0.0:443"
listen = true

[tls]
priv_key = "private.key"
cert = "cert.pem"
```

```bash
cargo run --features bin
```

### 3. 启动 B（连接模式）

```toml
# b.toml
node_id = "host-b"

[endpoint]
bind_address = "0.0.0.0:0"
listen = false
remote_addr = ["a.example.com:443"]

[tls]
priv_key = "private.key"
cert = "cert.pem"
```

```bash
cargo run --features bin -- --config b.toml
```

B 启动后会自动连到 A，握手完成双方状态变为 `Running`。

### 4. TCP 转发

在 A 的配置里追加：

```toml
[[tcp]]
listen = "127.0.0.1:8080"
target = "host-b"
[tcp.params]
target_address = "10.0.0.1:80"
connect_timeout = 5
keepalive_timeout = 60
keepalive_interval = 15
keepalive_retries = 3
```

重启 A，访问 `http://127.0.0.1:8080` 即可穿透到 B 的内网。

## 配置参考

```toml
node_id = "my-node"

[endpoint]
bind_address = "0.0.0.0:443"
listen = true
remote_addr = ["peer-a:443", "peer-b:443"]

[endpoint.options]
connect_timeout = 10
handshake_timeout = 10
first_heartbeat = 3
heartbeat_interval = 15
heartbeat_timeout = 30

[tls]
use_system_ca = true
ca_path = "ca.pem"
priv_key = "private.key"
cert = "cert.pem"
verify_client = false
```

| 配置项 | 说明 |
|--------|------|
| `endpoint.listen` | `true` = 监听模式（等人连），`false` = 连接模式（主动去连） |
| `endpoint.remote_addr` | 连接模式下的目标地址列表，任一可用即可 |
| `endpoint.options` | 超时参数，不填则使用默认值 |
| `tls.use_system_ca` | 是否加载系统根证书 |
| `tls.ca_path` | 额外 CA 证书 |
| `tls.verify_client` | 是否开启双向 TLS 验证 |
| `tcp` | TCP 转发规则数组，每一条定义一个本地端口到对端目标的映射 |

## 构建

```bash
cargo build                          # 仅库
cargo build --features bin           # 完整二进制（TCP 转发、HTTP 代理）
cargo build --features api           # 额外开启 HTTP REST API
```

feature 说明：

| Feature | 额外依赖 | 包含功能 |
|---------|----------|----------|
| (默认) | 无 | 仅框架库 |
| `bin` | hyper, hyper-rustls, sysinfo, dashmap, socket2 等 | TCP 转发、HTTP 代理、文件管理 |
| `api` | axum | HTTP API 管理接口 |

## 依赖栈

| 依赖 | 用途 |
|------|------|
| quinn | QUIC 传输层 |
| rustls | TLS 1.3 加密 |
| tokio | 异步运行时 |
| bincode | 消息序列化 |
| binrw | 二进制协议头编解码 |
| hyper | HTTP 代理客户端 |

## 许可证

MIT
