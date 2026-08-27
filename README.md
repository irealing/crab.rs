# 🦀 Crab.rs

基于 QUIC 的点对点远程管理工具。

## 它能做什么

Crab 是一个点对点的 QUIC 通信工具。你把两个实例连起来，然后就可以：

- 在对方机器上**读写文件**（流式传输，支持大文件）
- 在对方机器上**删除文件或目录**
- 把对方的 HTTP 请求**代理**到本地网络
- 把本地端口**转发**到对方内网的服务（TCP 隧道）
- 在本地起一个 **SOCKS5 代理**，把流量导到对方内网

所有通信走 QUIC + TLS 1.3，加密和连接复用开箱即用。

## 架构

实例之间对等连接，谁都可以向谁发命令。`listen` 和 `remote_addr` 是**两个独立的开关**，不是二选一：

- `listen = true` → 开启监听，接受别人连进来
- `remote_addr` 配置了 → 主动连出去

两者可以同时开启，也可以只开其中一个。同一个实例既可以是服务端又可以是客户端：

```
实例 A                         实例 B
listen = true                  listen = true
remote_addr = ["B:443"]        remote_addr = ["A:443"]
     │                              │
     └──────────── QUIC ────────────┘
                 双向连接

实例 C（纯客户端，NAT 后面）
listen = false                 ← 不监听
remote_addr = ["A:443"]        ← 只主动连 A
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
| `ListDir` | 列举目录内容 |
| `Delete` | 删除文件或目录 |
| `ReadFile` | 读取文件内容（流式） |
| `WriteFile` | 写入文件内容（流式） |
| `HttpProxy` | 让对端代发 HTTP 请求 |
| `TCPForward` | 本地端口转发到对端内网 |
| `Socks5` | 本地 SOCKS5 代理，流量走对端 |

## 文件读写

读文件分两阶段：先取元数据（大小、类型、修改时间），再决定是否拉数据流。

写文件分两阶段：先确认可写，再传数据流。支持自动创建目录、覆盖保护，写入先到临时文件，完成后原子重命名。

## TCP 端口转发

配置 TCP 转发规则后，Crab 会在本地监听一个端口，进来的连接通过 QUIC 隧道送到对端，由对端连接到目标地址：

```toml
[[tcp_forward]]
listen = "127.0.0.1:8080"      # 本机监听的端口
target = "b-host"              # 对端实例的 node_id
[tcp_forward.params]
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
[[tcp_forward]]
listen = "127.0.0.1:2222"
target = "jump-box"
[tcp_forward.params]
target_address = "192.168.1.1:22"

[[tcp_forward]]
listen = "127.0.0.1:3306"
target = "db-node"
[tcp_forward.params]
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

## SOCKS5 代理（需要 `socks5` feature）

在本地起一个 SOCKS5 代理服务器，所有代理请求通过 QUIC 隧道转发到对端，由对端访问目标。支持 TCP CONNECT 和 UDP ASSOCIATE（含用户名密码认证）：

```toml
[[socks5_proxy]]
listen = "127.0.0.1:1080"   # 本地 SOCKS5 监听地址
target = "host-b"            # 对端实例的 node_id

# 可选：用户名密码认证，不写这段则无需认证
[socks5_proxy.auth]
username = "user"
password = "pass"
```

然后配置浏览器或 curl 使用该代理：

```bash
curl --socks5 127.0.0.1:1080 http://example.com
```

流量路径：

```
你 → SOCKS5 :1080 → [Crab A] → QUIC 隧道 → [Crab B] → 目标网站
```

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

启用 HTTP API 后，可以通过 curl 直接向节点发命令。API 监听地址由配置中的 `http_api` 字段独立指定（与 `endpoint.bind_address` 无关），路由前缀为 `/ctrl/{node_id}/`：

```toml
http_api = "0.0.0.0:3000"   # 不写则不启动 HTTP API
```

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

### 列出目录

```bash
curl "http://127.0.0.1:3000/ctrl/node-b/dir?path=/data"
```

返回目录下的文件列表：

```json
{
  "err_no": 0,
  "msg": "success",
  "data": [
    { "name": "dump.bin", "dir": false },
    { "name": "logs", "dir": true }
  ]
}
```

`name` 为条目名称，`dir` 表示是否为目录（`true` 为目录）。注意：当**目标节点**是 Windows 时，`path` 传 `/` 或留空会返回磁盘列表：

```bash
curl "http://127.0.0.1:3000/ctrl/node-b/dir?path=/"
```

```json
{
  "err_no": 0,
  "msg": "success",
  "data": [
    { "name": "C:\\", "dir": false },
    { "name": "D:\\", "dir": false }
  ]
}
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

http_api = "0.0.0.0:3000"

[endpoint]
bind_address = "0.0.0.0:443"
listen = true

[tls]
priv_key = "private.key"
cert = "cert.pem"
```

```bash
cargo run --features full
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

### 4. TCP 端口转发

在 A 的配置里追加：

```toml
[[tcp_forward]]
listen = "127.0.0.1:8080"
target = "host-b"
[tcp_forward.params]
target_address = "10.0.0.1:80"
connect_timeout = 5
keepalive_timeout = 60
keepalive_interval = 15
keepalive_retries = 3
```

重启 A，访问 `http://127.0.0.1:8080` 即可穿透到 B 的内网。

### 5. SOCKS5 代理

在 A 的配置里追加（需要 `socks5` feature）：

```toml
[[socks5_proxy]]
listen = "127.0.0.1:1080"
target = "host-b"
```

重启 A，然后：

```bash
curl --socks5 127.0.0.1:1080 http://10.0.0.1
```

通过代理访问 B 内网的任何地址。

## 配置参考

```toml
node_id = "my-node"

http_api = "0.0.0.0:3000"   # HTTP API 监听地址（可选，需要 api feature）

[endpoint]
bind_address = "0.0.0.0:443"   # QUIC 监听/绑定地址
listen = true                   # 是否开启监听，接受别人连接
remote_addr = ["peer-a:443"]   # 要主动连接的节点（可选，可多个）

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
| `http_api` | HTTP API 监听地址，不写则不启动（需要 `api` feature） |
| `endpoint.listen` | 是否开启监听接受连接，`true`/`false` 均可与 `remote_addr` 同时使用 |
| `endpoint.remote_addr` | 要主动连接的节点地址列表，可多个；与 `listen` 相互独立 |
| `endpoint.options` | 超时参数，不填则使用默认值 |
| `tls.use_system_ca` | 是否加载系统根证书 |
| `tls.ca_path` | 额外 CA 证书 |
| `tls.verify_client` | 是否开启双向 TLS 验证 |
| `tcp_forward` | TCP 转发规则数组，每一条定义一个本地端口到对端目标的映射（需要 `tcp_forward` feature） |
| `socks5_proxy` | SOCKS5 代理规则数组（需要 `socks5` feature） |

## 构建

Crab 的 feature 决定了二进制运行时的角色：**被控端**（听命令）和**控制端**（发命令）可以分离打包，也可以合并。

```bash
cargo build                                            # 仅库（供第三方集成）
cargo build --features bin                             # 二进制：被控端（接受文件读写、代理等命令）
cargo build --features bin,tcp_forward                 # 被控端 + TCP 转发控制端
cargo build --features bin,socks5                      # 被控端 + SOCKS5 代理控制端
cargo build --features bin,api                         # 被控端 + HTTP API 控制端
cargo build --features full                            # 被控端 + 完整控制端能力
```

feature 说明：

| Feature | 角色 | 包含功能 |
|---------|------|----------|
| (默认) | — | 仅框架库，供第三方程序集成 |
| `bin` | 被控端 | 接受并执行命令（文件管理、HTTP 代理） |
| `tcp_forward` | 控制端 | 本地监听端口，转发到被控端内网 |
| `socks5` | 控制端 | 本地 SOCKS5 代理，流量走被控端（支持 TCP/UDP） |
| `api` | 控制端 | HTTP API，通过 curl 向被控端发命令 |
| `full` | 两者 | `bin` + `tcp_forward` + `socks5` + `api`，完整的收发能力 |

典型部署场景：

- **内网机器**（被控端）：`cargo build --features bin`，最小化依赖
- **公网跳板**（控制端）：`cargo build --features full`，统一管理所有被控端

## 依赖栈

| 依赖 | 用途 |
|------|------|
| quinn | QUIC 传输层 |
| rustls | TLS 1.3 加密 |
| tokio | 异步运行时 |
| bincode | 消息序列化 |
| binrw | 二进制协议头编解码 |
| hyper | HTTP 代理客户端 |
| socks5-server | SOCKS5 代理协议实现 |

## 许可证

MIT
