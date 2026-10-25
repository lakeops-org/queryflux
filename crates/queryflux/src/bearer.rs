//! What a worker or hub presents upstream as `Authorization: Bearer`.

use std::sync::Arc;

/// Read on every request: a fixed secret, or a hub's short-lived signed token, which
/// changes as it is renewed.
#[derive(Clone)]
pub(crate) struct Bearer(Arc<dyn Fn() -> String + Send + Sync>);

impl Bearer {
    pub(crate) fn secret(secret: impl Into<String>) -> Self {
        let secret: Arc<str> = secret.into().into();
        Self(Arc::new(move || secret.to_string()))
    }

    pub(crate) fn get(&self) -> String {
        (self.0)()
    }
}
