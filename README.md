# Crab.rs – 基于Rust和QUIC开发的轻量级的异步节点管理框架

## 项目概述

Crab.rs 是一个用 Rust 编写的高效且轻量级的异步节点管理框架。

## 主要特点

- **异步架构**：异步优先，核心部分使用纯异步实现，基于[Tokio](https://tokio.rs/)。
- **QUIC协议**：使用[quinn](https://github.com/quinn-rs/quinn)充分利用QUIC特性。
- **模块化设计**：核心功能由`src/lib.rs`导出，允许开发者轻松地集成和扩展。
- **应用层**：`src/main.rs`提供了一个基于该框架搭建的产级服务实例，也可用于演示如何集成并使用框架的核心功能。

## 技术实现

### 概念解释

- **Endpoint**：代表网络中的端点，可以作为服务端监听其他Endpoint连接，也可以作为客户端连接到其他Endpoint。
- **Node**：是远程Endpoint的一个抽象表示。
- **Handle**: Crab.rs使用Actor模式构建，`Handle`用于与Node进行通信。


### 连接过程

1. **三次握手**：当两个Endpoint尝试建立连接时，它们之间首先通过一个双向流进行三次握手。这是通过交换握手信息来完成的，确保了双方身份的验证。
2. **心跳保持**：一旦握手成功，就会启动另一个双向流专门用于维持心跳。心跳的存在表明连接处于活动状态；若心跳中断，则认为相应的Node已退出。
3. **命令交互**：连接建立后，任意一方都可以向对方发送指令。每条指令及其响应都将在一个新的双向流中独立处理。

### 自定义协议

`crab`框架允许用户通过实现`crab::proto::Protocol` trait来定义自己的节点管理协议。`Protocol` trait定义了一系列方法，这些方法涵盖了从握手到心跳再到命令处理等关键操作。通过这种方式，开发者能够轻松地为他们的应用定制合适的协议逻辑。

**以下是`Protocol`的定义**

```rust
#[async_trait::async_trait]
pub trait Protocol: Send + Sync {
    type Handshake: HandshakePacket + 'static;
    type Heartbeat: DeserializeOwned + Serialize + Send + Sync + 'static;
    type Command: DeserializeOwned + Serialize + Send + Sync + 'static;
    fn make_handshake(&self) -> Result<Self::Handshake, CrabError>;
    fn make_heartbeat(&self) -> Result<Self::Heartbeat, CrabError>;
    async fn on_handshake(
        &self,
        _: &NodeMetadata,
        _: &Self::Handshake,
    ) -> Result<Self::Handshake, CrabError> {
        self.make_handshake()
    }
    async fn on_heartbeat(
        &self,
        _: &NodeMetadata,
        _: &Self::Heartbeat,
    ) -> Result<Self::Heartbeat, CrabError> {
        self.make_heartbeat()
    }
    async fn on_node_accepted(
        &self,
        _: &NodeMetadata,
        _: Handle,
        _: Self::Handshake,
    ) -> Result<(), CrabError> {
        Ok(())
    }
    async fn on_node_exited(&self, _: &NodeMetadata) {}
    async fn handle_command(
        &self,
        _: CancellationToken,
        _: &NodeMetadata,
        _: (MessageHeader, Self::Command),
        _: Stream,
    ) -> Result<(), CrabError> {
        Err(CrabError::ErrorCode(CrabError::UNKNOWN_ERROR))
    }
}
```


## 基于Crab的应用

`Crab.rs`自带一个基于crab构建的节点管理应用，程序入口为`src/main.rs`

### 如何构建

- **构建项目**：
  ```shell
  cargo build --features 'bin' --bin crab --release
  ```
### 功能清单

* 远程文件读取：允许从远程节点读取指定路径下的文件内容。
* 远程文件写入：支持将本地文件上传至远程节点的指定目录。
* HTTP代理请求：转发节点发送的HTTP请求到目标服务器，并返回响应。
* 节点文件（夹）删除：通过API接口删除远程节点上的特定文件或整个文件夹。
#### 未来规划
* TCP/UDP端口转发：计划实现TCP和UDP协议的端口转发功能。

## 许可证

本项目采用MIT许可证。详见[LICENSE](LICENSE)文件获取更多信息。
