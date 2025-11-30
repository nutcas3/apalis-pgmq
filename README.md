# apalis-pgmq

[![Crates.io](https://img.shields.io/crates/v/apalis-pgmq.svg)](https://crates.io/crates/apalis-pgmq)
[![Documentation](https://docs.rs/apalis-pgmq/badge.svg)](https://docs.rs/apalis-pgmq)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

`pgmq` backend for [Apalis](https://github.com/apalis-dev/apalis), providing a PostgreSQL-based message queue for distributed job processing.

## Overview

This crate provides a message queue implementation for Apalis using [PGMQ](https://github.com/tembo-io/pgmq) (PostgreSQL Message Queue). PGMQ is a lightweight, Postgres-based message queue with guaranteed delivery and visibility timeouts, similar to AWS SQS.

## Features

- **Persistent storage** – Messages stored in PostgreSQL survive restarts
- **Visibility timeout** – Messages become invisible during processing and reappear if not acknowledged
- **Guaranteed delivery** – Messages are delivered to exactly one consumer within the visibility timeout
- **Distributed processing** – Multiple workers can process messages concurrently
- **Immutable messages** – Once enqueued, messages cannot be modified (message queue semantics)
- **Archive support** – Messages can be archived for long-term retention instead of deletion

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
apalis = "0.6"
apalis-pgmq = "0.1"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["full"] }
```

## Prerequisites

You need PostgreSQL 13+ with the PGMQ extension installed. Follow the [PGMQ installation guide](https://github.com/tembo-io/pgmq#installation).

For development, you can use Docker:

```bash
docker run -d --name postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgres:15
```

Then install the PGMQ extension in your database:

```sql
CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;
```

## Quick Start

```rust
use apalis::prelude::*;
use apalis_pgmq::PgMq;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Email {
    to: String,
    subject: String,
    body: String,
}

async fn send_email(email: Email, ctx: JobContext) {
    println!("Sending email to: {}", email.to);
    // Your email sending logic here
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a PGMQ backend
    let mq = PgMq::<Email>::new(
        "postgres://postgres:postgres@localhost:5432/postgres",
        "emails"
    )
    .await?
    .with_visibility_timeout(60); // 60 seconds visibility timeout

    // Build a worker
    let worker = WorkerBuilder::new("email-worker")
        .backend(mq)
        .build_fn(send_email);

    // Run the worker
    Monitor::new()
        .register(worker)
        .run()
        .await?;

    Ok(())
}
```

## Enqueuing Messages

```rust
use apalis_pgmq::PgMq;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Job {
    id: u64,
    data: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut mq = PgMq::<Job>::new(
        "postgres://postgres:postgres@localhost:5432",
        "jobs"
    ).await?;

    // Enqueue a single message
    mq.enqueue(Job {
        id: 1,
        data: "process this".to_string(),
    }).await?;

    Ok(())
}
```

## Message Acknowledgment

Messages must be acknowledged after successful processing. With PGMQ, messages have a visibility timeout - if not acknowledged within this time, they become visible again for reprocessing.

The `PgMqContext` provides the message ID needed for acknowledgment:

```rust
use apalis::prelude::*;
use apalis_pgmq::{PgMq, PgMqContext};

async fn process_job(job: Job, ctx: JobContext) -> Result<JobResult, JobError> {
    // Process the job
    println!("Processing job: {:?}", job);
    
    // The message is automatically acknowledged by Apalis on success
    // If this function returns an error, the message will become visible again
    
    Ok(JobResult::Success)
}
```

## Key Differences from Storage Backends

Unlike storage backends (like `apalis-sql`), message queues:

1. **Are immutable** – Messages cannot be updated after being enqueued
2. **Have visibility timeouts** – Messages become temporarily invisible when read
3. **Auto-requeue on failure** – Unacknowledged messages automatically return to the queue
4. **No job state tracking** – Messages are either in the queue or processed (no "running" state)

## Configuration

### Visibility Timeout

Set how long messages stay invisible after being read:

```rust
let mq = PgMq::<Job>::new(url, "queue")
    .await?
    .with_visibility_timeout(120); // 2 minutes
```

### Archive Instead of Delete

Archive processed messages for audit trails:

```rust
let mq = PgMq::<Job>::new(url, "queue").await?;

// After processing, archive instead of delete
if let Some(msg_id) = context.msg_id {
    mq.archive(msg_id).await?;
}
```

## Examples

See the [`examples/`](examples/) directory for complete working examples:

- [`basic.rs`](examples/basic.rs) - Simple message enqueuing and processing
- [`worker.rs`](examples/worker.rs) - Full worker setup with Apalis

Run examples:

```bash
# Start PostgreSQL
docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:15

# Run an example
cargo run --example basic
```

## Comparison with apalis-sql

| Feature | apalis-pgmq | apalis-sql |
|---------|-------------|------------|
| Backend | PGMQ | Direct SQL |
| Message mutability | Immutable | Mutable |
| Visibility timeout | Yes | No |
| Auto-requeue | Yes | Manual |
| Job state tracking | No | Yes (pending/running/done) |
| Archive support | Yes | No |
| Use case | Message queues | Job queues with state |

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Apalis](https://github.com/apalis-dev/apalis) - Simple, extensible multithreaded background job processing library for Rust
- [PGMQ](https://github.com/tembo-io/pgmq) - PostgreSQL Message Queue
