use axum::{
    http::{Request, Response},
    middleware::Next,
};

/// Security headers middleware
///
/// Adds standard headers to all responses:
/// - `Cache-Control: no-store` (or `no-cache` for SSE responses)
/// - `X-Content-Type-Options: nosniff` - Prevents MIME type sniffing
/// - `Cross-Origin-Resource-Policy: cross-origin` - Allows cross-origin access
///
/// # Panics
///
/// Panics if the hardcoded header values fail to parse, which should never happen.
pub async fn add_security_headers(
    request: Request<axum::body::Body>,
    next: Next,
) -> Response<axum::body::Body> {
    let mut response = next.run(request).await;

    let headers = response.headers_mut();

    // SSE responses use no-cache (streaming); all others use no-store
    let is_sse = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("text/event-stream"));

    let cache_control = if is_sse { "no-cache" } else { "no-store" };
    headers.insert("cache-control", cache_control.parse().unwrap());
    headers.insert("X-Content-Type-Options", "nosniff".parse().unwrap());
    headers.insert(
        "Cross-Origin-Resource-Policy",
        "cross-origin".parse().unwrap(),
    );

    response
}
