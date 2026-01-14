use apalis_core::{error::BoxDynError, task::Parts, worker::ext::ack::Acknowledge};
use futures::{future::BoxFuture, FutureExt};

use crate::{PgMq, PgMqContext, PgMqError};

impl<M: Send + 'static, Res: Send + Sync + 'static> Acknowledge<Res, PgMqContext, i64>
    for PgMq<M>
{
    type Error = PgMqError;
    type Future = BoxFuture<'static, Result<(), PgMqError>>;
    fn ack(
        &mut self,
        res: &Result<Res, BoxDynError>,
        parts: &Parts<PgMqContext, i64>,
    ) -> Self::Future {
        let queue = self.queue.clone();
        let queue_name = self.queue_name.clone();
        let msg_id = parts.ctx.msg_id.unwrap_or(0);
        let read_ct = parts.ctx.read_ct.unwrap_or(0);
        let max_retries = self.max_retries;
        let is_ok = res.is_ok();
        
        async move {
            match is_ok {
                true => {
                    queue
                        .delete(&queue_name, msg_id)
                        .await
                        .map_err(PgMqError::Pgmq)?;
                    Ok(())
                }
                false => {
                    if read_ct >= max_retries {
                        queue
                            .archive(&queue_name, msg_id)
                            .await
                            .map_err(PgMqError::Pgmq)?;
                    }
                    Ok(())
                }
            }
        }
        .boxed()
    }
}