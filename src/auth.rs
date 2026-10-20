//! Bearer-token validation for the API, with the same crate HFS uses: a JWT
//! signed by the identity provider's JWKS, with the expected issuer and audience.

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use axum::Json;
use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use helios_auth::{AuthConfig, AuthProvider, JwksBearerAuthProvider, JwksCache};
use serde_json::json;

/// `None` when authentication is off: every request passes.
pub type Auth = Option<Arc<dyn AuthProvider>>;

#[derive(Debug, Clone)]
pub struct AuthSettings {
    pub enabled: bool,
    pub jwks_url: Option<String>,
    pub issuer: Option<String>,
    pub audience: Option<String>,
    pub algorithms: Vec<String>,
    pub jwks_min_refresh_interval: u64,
}

impl AuthSettings {
    /// `HIE_AUTH_ENABLED`, `HIE_AUTH_JWKS_URL`, `HIE_AUTH_ISSUER`, `HIE_AUTH_AUDIENCE`,
    /// `HIE_AUTH_ALGORITHMS` and `HIE_AUTH_JWKS_MIN_REFRESH_INTERVAL`, named as HFS names
    /// its own.
    pub fn from_env() -> Self {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
        Self {
            enabled: var("HIE_AUTH_ENABLED")
                .is_some_and(|v| v.eq_ignore_ascii_case("true") || v == "1"),
            jwks_url: var("HIE_AUTH_JWKS_URL"),
            issuer: var("HIE_AUTH_ISSUER"),
            audience: var("HIE_AUTH_AUDIENCE"),
            algorithms: var("HIE_AUTH_ALGORITHMS")
                .unwrap_or_else(|| "RS256,RS384,ES256,ES384".into())
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            jwks_min_refresh_interval: var("HIE_AUTH_JWKS_MIN_REFRESH_INTERVAL")
                .and_then(|v| v.parse().ok())
                .unwrap_or(10),
        }
    }
}

/// Builds the validator, fetching the JWKS once so a wrong URL fails at startup.
pub async fn build(settings: &AuthSettings) -> Result<Auth> {
    if !settings.enabled {
        return Ok(None);
    }
    let config = AuthConfig {
        enabled: true,
        jwks_url: settings.jwks_url.clone(),
        expected_issuer: settings.issuer.clone(),
        expected_audience: settings.audience.clone(),
        allowed_algorithms: settings.algorithms.clone(),
        jwks_min_refresh_interval: settings.jwks_min_refresh_interval,
        ..AuthConfig::default()
    };
    if let Err(problems) = config.validate() {
        bail!("HIE_AUTH_* configuration: {}", problems.join("; "));
    }
    let jwks_url = config
        .jwks_url
        .as_deref()
        .context("HIE_AUTH_JWKS_URL is required when HIE_AUTH_ENABLED=true")?;
    let cache = Arc::new(JwksCache::new(jwks_url, config.jwks_min_refresh_interval));
    cache
        .initial_fetch()
        .await
        .map_err(|e| anyhow::anyhow!("fetching the JWKS at {jwks_url}: {e}"))?;
    Ok(Some(Arc::new(JwksBearerAuthProvider::new(cache, &config))))
}

/// Requires a valid bearer token on every request it wraps, when auth is on. The
/// authenticated principal is left in the request's extensions for the handlers.
pub async fn require_bearer(
    State(auth): State<Auth>,
    mut request: Request,
    next: Next,
) -> Response {
    let Some(provider) = auth else {
        return next.run(request).await;
    };
    let authorization = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let Some(authorization) = authorization else {
        return unauthorized("missing bearer token");
    };
    match provider.authenticate(&authorization).await {
        Ok(principal) => {
            request.extensions_mut().insert(Arc::new(principal));
            next.run(request).await
        }
        Err(e) => unauthorized(&e.to_string()),
    }
}

fn unauthorized(reason: &str) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Bearer")],
        Json(json!({"error": reason})),
    )
        .into_response()
}
