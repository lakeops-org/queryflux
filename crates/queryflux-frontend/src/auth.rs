use queryflux_auth::{AuthContext, AuthProvider, Credentials};
use queryflux_core::error::Result;

/// Authenticate a bearer-only HTTP client using the caller's captured provider.
/// Protocol wrappers retain responsibility for error responses and metrics.
pub(crate) async fn authenticate_bearer(
    auth_provider: &dyn AuthProvider,
    authorization: Option<&str>,
) -> Result<AuthContext> {
    auth_provider
        .authenticate(&Credentials {
            username: None,
            password: None,
            bearer_token: authorization
                .and_then(crate::strip_bearer_prefix)
                .map(str::to_owned),
        })
        .await
}
