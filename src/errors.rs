use thiserror::Error;

/// Errors that can occur when using the PGMQ backend.
#[derive(Debug, Error)]
pub enum PgMqError {
    /// An error from the underlying PGMQ library.
    #[error("PGMQ error: {0}")]
    Pgmq(#[from] pgmq::errors::PgmqError),
    
    /// A serialization/deserialization error.
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    
    /// An error that occurs during a queue operation.
    #[error("Queue operation error: {0}")]
    QueueOperation(String),
}