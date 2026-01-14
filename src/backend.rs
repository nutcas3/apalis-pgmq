use apalis_core::backend::Backend;
use apalis_core::task::Task;
use apalis_core::timer::sleep;
use apalis_core::worker::context::WorkerContext;
use futures::stream::{Stream, unfold};
use pgmq::{Message as PgmqMessage, PGMQueue};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::pin::Pin;

use crate::errors::PgMqError;
use crate::sink::PgMqSink;

/// Context information for a PGMQ message.
///
/// This struct contains metadata about a message retrieved from the queue,
/// including its ID, read count, enqueue time, and visibility timeout.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PgMqContext {
    /// The message ID assigned by PGMQ.
    pub msg_id: Option<i64>,
    /// The number of times this message has been read.
    pub read_ct: Option<i32>,
    /// The timestamp when the message was enqueued.
    pub enqueued_at: Option<chrono::DateTime<chrono::Utc>>, 
    /// The visibility timeout timestamp - when the message will become visible again.
    pub vt: Option<chrono::DateTime<chrono::Utc>>,
}

/// A PGMQ backend for Apalis.
///
/// This backend uses PostgreSQL Message Queue (PGMQ) to provide a message queue
/// with guaranteed delivery, visibility timeouts, and automatic retry support.
///
/// # Example
///
/// ```no_run
/// use apalis_pgmq::PgMq;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// struct Job {
///     id: u64,
///     data: String,
/// }
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let backend = PgMq::<Job>::new(
///     "postgres://postgres:postgres@localhost:5432/postgres",
///     "jobs"
/// )
/// .await?
/// .with_visibility_timeout(std::time::Duration::from_secs(60))
/// .with_poll_interval(std::time::Duration::from_millis(500))
/// .with_max_retries(5);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct PgMq<T> {
    pub(crate) queue: PGMQueue,
    pub(crate) queue_name: String,
    visibility_timeout: std::time::Duration,
    poll_interval: std::time::Duration,
    pub(crate) max_retries: i32,
    sink: PgMqSink<T>,
    _phantom: PhantomData<T>,
}

impl<T> PgMq<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    /// Create a new PGMQ backend.
    ///
    /// This will connect to PostgreSQL and create the queue if it doesn't exist.
    ///
    /// # Arguments
    ///
    /// * `connection_url` - PostgreSQL connection URL
    /// * `queue_name` - Name of the queue to use
    ///
    /// # Errors
    ///
    /// Returns an error if the connection fails or the queue cannot be created.
    pub async fn new(connection_url: &str, queue_name: &str) -> Result<Self, PgMqError> {
        let queue = PGMQueue::new(connection_url.to_owned())
            .await
            .map_err(PgMqError::Pgmq)?;
        
        queue.create(queue_name)
            .await
            .map_err(PgMqError::Pgmq)?;
        
        Ok(Self {
            queue,
            queue_name: queue_name.to_owned(),
            visibility_timeout: std::time::Duration::from_secs(30),
            poll_interval: std::time::Duration::from_millis(100),
            max_retries: 5,
            sink: PgMqSink::new(),
            _phantom: PhantomData,
        })
    }

    /// Set the visibility timeout for messages.
    ///
    /// Messages will remain invisible for this duration after being read.
    /// If not acknowledged within this time, they will become visible again.
    ///
    /// Default: 30 seconds
    pub fn with_visibility_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.visibility_timeout = timeout;
        self
    }

    /// Set the poll interval for checking new messages.
    ///
    /// This controls how frequently the backend polls for new messages when
    /// the queue is empty.
    ///
    /// Default: 100 milliseconds
    pub fn with_poll_interval(mut self, interval: std::time::Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Set the maximum number of retry attempts.
    ///
    /// Messages that fail more than this number of times will be archived
    /// instead of being retried again.
    ///
    /// Default: 5
    pub fn with_max_retries(mut self, max_retries: i32) -> Self {
        self.max_retries = max_retries;
        self
    }
    
    /// Acknowledge (delete) a message from the queue.
    ///
    /// This permanently removes the message from the queue.
    ///
    /// # Arguments
    ///
    /// * `msg_id` - The message ID to acknowledge
    pub async fn ack(&self, msg_id: i64) -> Result<(), PgMqError> {
        self.queue
            .delete(&self.queue_name, msg_id)
            .await
            .map_err(PgMqError::Pgmq)?;
        Ok(())
    }
    
    /// Archive a message.
    ///
    /// Archived messages are moved to a separate archive table for long-term
    /// retention instead of being deleted.
    ///
    /// # Arguments
    ///
    /// * `msg_id` - The message ID to archive
    pub async fn archive(&self, msg_id: i64) -> Result<(), PgMqError> {
        self.queue
            .archive(&self.queue_name, msg_id)
            .await
            .map_err(PgMqError::Pgmq)?;
        Ok(())
    }

    /// Enqueue a message to the queue.
    ///
    /// # Arguments
    ///
    /// * `job` - The message to enqueue
    ///
    /// # Returns
    ///
    /// The message ID assigned by PGMQ
    pub async fn enqueue(&self, job: T) -> Result<i64, PgMqError> {
        let msg_id = self.queue
            .send(&self.queue_name, &job)
            .await
            .map_err(PgMqError::Pgmq)?;
        Ok(msg_id)
    }

    /// Handle retry logic for a failed message.
    ///
    /// If the read count exceeds max retries, the message will be archived.
    /// Otherwise, it will become visible again after the visibility timeout.
    ///
    /// # Arguments
    ///
    /// * `msg_id` - The message ID to retry
    /// * `read_count` - The current read count for the message
    pub async fn retry(&self, msg_id: i64, read_count: i32) -> Result<(), PgMqError> {
        if read_count >= self.max_retries {
            self.archive(msg_id).await?;
        }
        Ok(())
    }

    pub(crate) fn sink_mut(&mut self) -> &mut PgMqSink<T> {
        &mut self.sink
    }

    /// Get a reference to the inner `PGMQueue`.
    pub fn queue(&self) -> &PGMQueue {
        &self.queue
    }

    /// Get a reference to the queue name.
    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    /// Get the visibility timeout duration.
    ///
    /// Returns the configured visibility timeout for messages in the queue.
    pub fn visibility_timeout(&self) -> std::time::Duration {
        self.visibility_timeout
    }
    
    /// Get the poll interval duration.
    ///
    /// Returns the configured poll interval for checking new messages.
    pub fn poll_interval(&self) -> std::time::Duration {
        self.poll_interval
    }

    /// Get the maximum number of retries.
    ///
    /// Returns the configured maximum retry attempts before archiving.
    pub fn max_retries(&self) -> i32 {
        self.max_retries
    }
}

impl<T> Backend for PgMq<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + Unpin + 'static,
{
    type Args = T;
    type IdType = i64;
    type Context = PgMqContext;
    type Error = PgMqError;
    type Stream = Pin<Box<dyn Stream<Item = Result<Option<Task<Self::Args, Self::Context, Self::IdType>>, Self::Error>> + Send>>;
    type Beat = Pin<Box<dyn Stream<Item = Result<(), Self::Error>> + Send>>;
    type Layer = tower::layer::util::Identity;

    fn poll(self, _worker: &WorkerContext) -> Self::Stream {
        let queue = self.queue.clone();
        let queue_name = self.queue_name.clone();
        let visibility_timeout_secs = self.visibility_timeout.as_secs() as i32;
        let poll_interval = self.poll_interval;

        let state = (queue, queue_name, visibility_timeout_secs, poll_interval);

        Box::pin(unfold(state, |(queue, queue_name, visibility_timeout_secs, poll_interval)| async move {
            let result: Result<Option<PgmqMessage<T>>, _> = queue
                .read(&queue_name, Some(visibility_timeout_secs))
                .await;

            let next_state = (queue, queue_name, visibility_timeout_secs, poll_interval);

            match result {
                Ok(Some(msg)) => {
                    let context = PgMqContext {
                        msg_id: Some(msg.msg_id),
                        read_ct: Some(msg.read_ct),
                        enqueued_at: Some(msg.enqueued_at),
                        vt: Some(msg.vt),
                    };

                    let mut task = Task::new(msg.message);
                    task.parts.ctx = context;
                    task.parts.task_id = Some(apalis_core::task::task_id::TaskId::new(msg.msg_id));
                    
                    Some((Ok(Some(task)), next_state))
                }
                Ok(None) => {
                    sleep(poll_interval).await;
                    Some((Ok(None), next_state))
                }
                Err(e) => {
                    Some((Err(PgMqError::Pgmq(e)), next_state))
                }
            }
        }))
    }

    fn heartbeat(&self, _worker: &WorkerContext) -> Self::Beat {
        Box::pin(unfold((), |_| async {
            sleep(std::time::Duration::from_secs(30)).await;
            Some((Ok(()), ()))
        }))
    }

    fn middleware(&self) -> Self::Layer {
        tower::layer::util::Identity::new()
    }
}