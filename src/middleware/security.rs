use axum::{
    http::{Request, Response},
    middleware::Next,
};

/// Security headers middleware
///
/// Adds security headers to all responses:
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
    headers.insert("X-Content-Type-Options", "nosniff".parse().unwrap());
    headers.insert(
        "Cross-Origin-Resource-Policy",
        "cross-origin".parse().unwrap(),
    );

    response
}
