# 🦀 Crab.rs

> **Build your own remote management platform with QUIC.**

Crab.rs is a lightweight asynchronous framework for building **long-lived remote nodes**, **distributed agents**, and **network services**.

Instead of dealing with connections, heartbeats, reconnections and stream management, you only need to implement your own protocol.

---

## Why Crab?

Most networking frameworks focus on **request-response** communication.

Crab focuses on **long-lived remote nodes**.

It provides everything needed to build reliable distributed systems:

- 🚀 QUIC transport
- ❤️ Automatic heartbeat
- 🌊 Independent bidirectional streams
- 🔄 Node lifecycle management
- 📡 Bidirectional communication
- ⚡ Fully asynchronous API
- 🦀 Native Tokio integration

You write your business logic.

Crab manages everything else.

---

## What can you build?

Crab is designed for systems where many remote nodes remain online for long periods.

Typical use cases include:

- Remote Agent
- Remote File Manager
- HTTP Proxy
- TCP Tunnel
- UDP Tunnel
- Remote Command Execution
- Distributed Worker
- Edge Computing
- IoT Platform
- Internal Service Mesh

---

## Architecture

```text
                 +----------------------+
                 |      Endpoint A      |
                 +----------------------+
                           │
                    QUIC Connection
                           │
       ┌───────────┬─────────────┬─────────────┐
       │           │             │             │
 Handshake     Heartbeat      Command      Command
   Stream         Stream        Stream       Stream
                                      │
                                      ▼
                                 Protocol
                                      │
                                      ▼
                               Your Business
```

Crab fully utilizes QUIC multiplexing.

Each command runs inside its own independent bidirectional stream.

That means:

- No Head-of-Line Blocking
- Parallel command execution
- Independent timeout handling
- Independent cancellation

---

## Features

- QUIC transport
- Long-lived node management
- Independent command streams
- Automatic heartbeat
- Actor-style architecture
- Extensible protocol layer
- Bidirectional communication
- Graceful shutdown
- Tokio ecosystem

---

## Protocol

Users only need to implement the protocol layer.

```rust
#[async_trait::async_trait]
pub trait Protocol {
    type Command;
    type Handshake;

    async fn make_handshake(...);

    async fn on_handshake(...);

    async fn on_node_accepted(...);

    async fn handle_command(...);

    async fn on_node_exited(...);
}
```

Crab manages:

- Connection establishment
- Handshake
- Heartbeat
- Stream lifecycle
- Node lifecycle
- Command routing

Your protocol is responsible for:

- Authentication
- Authorization
- Business commands
- Application logic

---

## Lifecycle

```text
Connect
    │
    ▼
Handshake
    │
    ▼
Node Accepted
    │
    ▼
Heartbeat
    │
    ▼
Command...
    │
    ▼
Disconnect
```

Protocol callbacks:

```text
make_handshake()

↓

on_handshake()

↓

on_node_accepted()

↓

handle_command()

↓

on_node_exited()
```

---

## Built-in Applications

Current examples include:

- Remote File Read
- Remote File Write
- File Delete
- HTTP Proxy

More applications can be built without changing the framework itself.

---

## Roadmap

- [ ] TCP Port Forward
- [ ] UDP Port Forward
- [ ] Remote Shell
- [ ] Service Discovery
- [ ] Cluster Management
- [ ] Plugin System

---

## Philosophy

Crab is **not** another RPC framework.

Instead, Crab provides the infrastructure required to build distributed systems around **long-lived remote nodes**.

You can think of it as the networking runtime behind your own:

- Remote Management Platform
- Agent Framework
- Edge Computing Platform
- Service Mesh
- Internal Infrastructure

---

## License

MIT