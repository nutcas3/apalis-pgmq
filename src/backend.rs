use apalis_codec::json::JsonCodec;
use apalis_core::{
    backend::{codec::Codec, Backend},
    task::Task,
    worker::context::WorkerContext,
};
use futures::stream::{self, Stream};
use pgmq::PGMQueue;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

use crate::{config::Config, errors::PgMqError};

/// A PGMQ message queue backend for Apalis.
///
/// This backend uses PostgreSQL Message Queue (PGMQ) to provide a message queue
/// with guaranteed delivery, visibility timeouts, and automatic retry support.
#[derive(Clone, Debug)]
pub struct PgMqBackend<T> {
    queue: PGMQueue,
    config: Config,
    _phantom: PhantomData<T>,
}

impl<T> PgMqBackend<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    /// Create a new PGMQ backend.
    ///
    /// # Arguments
    ///
    /// * `connection_url` - PostgreSQL connection URL
    /// * `queue_name` - Name of the queue to use
    pub async fn new(connection_url: &str, queue_name: &str) -> Result<Self, PgMqError> {
        let queue = PGMQueue::new(connection_url.to_owned())
            .await
            .map_err(PgMqError::Pgmq)?;

        queue
            .create(queue_name)
            .await
            .map_err(PgMqError::Pgmq)?;

        Ok(Self {
            queue,
            config: Config::new(queue_name),
            _phantom: PhantomData,
        })
    }

    /// Create a new PGMQ backend with custom configuration.
    pub async fn new_with_config(
        connection_url: &str,
        config: Config,
    ) -> Result<Self, PgMqError> {
        let queue = PGMQueue::new(connection_url.to_owned())
            .await
            .map_err(PgMqError::Pgmq)?;

        queue
            .create(config.namespace())
            .await
            .map_err(PgMqError::Pgmq)?;

        Ok(Self {
            queue,
            config,
            _phantom: PhantomData,
        })
    }

    /// Get a reference to the inner `PGMQueue`.
    pub fn queue(&self) -> &PGMQueue {
        &self.queue
    }

    /// Get a reference to the configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }
}

impl<T> Backend for PgMqBackend<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + Unpin + 'static,
{
    type Args = T;
    type IdType = i64;
    type Context = ();
    type Error = PgMqError;
    type Stream = Box<dyn Stream<Item = Result<Option<Task<Self::Args, Self::Context, Self::IdType>>, Self::Error>> + Send + Unpin>;
    type Beat = Box<dyn Stream<Item = Result<(), Self::Error>> + Send + Unpin>;
    type Layer = tower::layer::util::Identity;

    fn heartbeat(&self, _worker: &WorkerContext) -> Self::Beat {
        // Simple heartbeat that never fails
        Box::new(stream::pending())
    }

    fn middleware(&self) -> Self::Layer {
        tower::layer::util::Identity::new()
    }

    fn poll(self, _worker: &WorkerContext) -> Self::Stream {
        let poll_interval = self.config.poll_interval();
        let vt_secs = self.config.visibility_timeout().as_secs() as i32;
        let namespace = self.config.namespace().to_string();
        
        let stream = stream::unfold(self, move |backend| {
            let namespace = namespace.clone();
            async move {
                let msg_result = backend
                    .queue
                    .read::<Vec<u8>>(&namespace, Some(vt_secs))
                    .await;

                match msg_result {
                    Ok(Some(pgmq_msg)) => {
                        let message: T = match JsonCodec::<Vec<u8>>::decode(&pgmq_msg.message) {
                            Ok(m) => m,
                            Err(e) => {
                                tracing::error!("Failed to decode message: {}", e);
                                return Some((Ok(None), backend));
                            }
                        };

                        let task = Task::new(message);
                        Some((Ok(Some(task)), backend))
                    }
                    Ok(None) => {
                        apalis_core::timer::sleep(poll_interval).await;
                        Some((Ok(None), backend))
                    }
                    Err(e) => Some((Err(PgMqError::Pgmq(e)), backend)),
                }
            }
        });

        Box::new(Box::pin(stream))
    }
}
