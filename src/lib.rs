#![warn(
    clippy::await_holding_lock,
    clippy::cargo_common_metadata,
    clippy::dbg_macro,
    clippy::empty_enum,
    clippy::enum_glob_use,
    clippy::inefficient_to_string,
    clippy::mem_forget,
    clippy::mutex_integer,
    clippy::needless_continue,
    clippy::todo,
    clippy::unimplemented,
    clippy::wildcard_imports,
    future_incompatible,
    missing_docs,
    missing_debug_implementations,
    unreachable_pub
)]
#![doc = include_str!("../README.md")]

mod ack;
mod backend;
mod errors;
mod sink;

use apalis_core::task::{task_id::TaskId, Task};

pub use backend::{PgMq, PgMqContext};
pub use errors::PgMqError;

/// Type alias for a PGMQ task with context and i64 as the task ID type.
pub type PgMqTask<T> = Task<T, PgMqContext, i64>;

/// Type alias for a PGMQ task ID with i64 as the ID type.
pub type PgMqTaskId = TaskId<i64>;
