//! Abort a spawned query task when the caller is dropped (client disconnect).

use tokio::task::{JoinError, JoinHandle};

/// Wraps a [`JoinHandle`] and aborts the task if dropped before [`join`](Self::join).
pub struct AbortOnDrop<T> {
    handle: Option<JoinHandle<T>>,
}

impl<T> AbortOnDrop<T> {
    pub fn new(handle: JoinHandle<T>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    /// Await the task and disarm the abort-on-drop.
    pub async fn join(mut self) -> std::result::Result<T, JoinError> {
        self.handle.take().expect("AbortOnDrop joined twice").await
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AbortOnDrop;

    #[tokio::test]
    async fn drop_aborts_the_task() {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let handle = AbortOnDrop::new(tokio::spawn(async move {
            struct SendOnDrop(Option<tokio::sync::oneshot::Sender<()>>);
            impl Drop for SendOnDrop {
                fn drop(&mut self) {
                    if let Some(tx) = self.0.take() {
                        let _ = tx.send(());
                    }
                }
            }
            let _guard = SendOnDrop(Some(tx));
            std::future::pending::<()>().await;
        }));
        drop(handle);
        // Ok(()) = Drop ran and signalled; RecvError = the future was dropped
        // before first poll (sender dropped with the task). Both mean abort worked.
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), rx)
            .await
            .expect("aborted task should be dropped");
    }

    #[tokio::test]
    async fn join_does_not_abort() {
        let handle = AbortOnDrop::new(tokio::spawn(async { 7u8 }));
        assert_eq!(handle.join().await.unwrap(), 7);
    }
}
