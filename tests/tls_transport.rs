mod common;

use axum_server::{Handle, tls_rustls::RustlsConfig};
use common::test_client;
use durable_streams_server::{config::Config, router, storage::memory::InMemoryStorage};
use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};
use rustls_pemfile::certs;
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

const CERT_PATH: &str = "e2e/fixtures/ds-server-cert.pem";
const KEY_PATH: &str = "e2e/fixtures/ds-server-key.pem";
const CA_CERT_PATH: &str = "e2e/fixtures/ds-ca-cert.pem";

/// Spawn a TLS test server on a random port, returning the port number.
async fn spawn_tls_server() -> u16 {
    let storage = Arc::new(InMemoryStorage::new(100 * 1024 * 1024, 10 * 1024 * 1024));
    let app = router::build_router(storage, &Config::default());
    let tls = RustlsConfig::from_pem_file(CERT_PATH, KEY_PATH)
        .await
        .expect("failed to build rustls config");

    let handle = Handle::new();
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));

    let server_handle = handle.clone();
    tokio::spawn(async move {
        axum_server::bind_rustls(addr, tls)
            .handle(server_handle)
            .serve(app.into_make_service())
            .await
            .expect("TLS test server failed");
    });

    // Wait for the server to bind and report its listening address.
    let listening = tokio::time::timeout(Duration::from_secs(5), handle.listening())
        .await
        .expect("TLS server did not start listening within 5s")
        .expect("server never reported listening address");
    listening.port()
}

async fn tls_raw_get_health(port: u16) -> std::io::Result<String> {
    let cert_pem = std::fs::read(CA_CERT_PATH)?;
    let mut reader = BufReader::new(cert_pem.as_slice());
    let cert_chain: Vec<_> = certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(std::io::Error::other)?;

    let mut roots = RootCertStore::empty();
    for cert in cert_chain {
        roots.add(cert).map_err(std::io::Error::other)?;
    }

    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));

    let tcp = TcpStream::connect(("127.0.0.1", port)).await?;
    let server_name = ServerName::try_from("localhost")
        .map_err(|e| std::io::Error::other(format!("invalid server name: {e}")))?;
    let mut tls = connector
        .connect(server_name, tcp)
        .await
        .map_err(std::io::Error::other)?;

    tls.write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await?;
    tls.flush().await?;

    let mut bytes = Vec::new();
    tls.read_to_end(&mut bytes).await?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

#[tokio::test]
async fn test_https_health_check_with_custom_ca() {
    let port = spawn_tls_server().await;

    let raw = tls_raw_get_health(port)
        .await
        .expect("HTTPS health check request failed");
    assert!(raw.starts_with("HTTP/1.1 200 OK"));
    assert!(raw.ends_with("\r\n\r\nok") || raw.ends_with("\n\nok"));
}

#[test]
fn test_tls_config_validation_requires_pair() {
    let mut config = Config {
        tls_cert_path: Some(CERT_PATH.to_string()),
        ..Config::default()
    };
    assert!(config.validate().is_err());

    config.tls_cert_path = None;
    config.tls_key_path = Some(KEY_PATH.to_string());
    assert!(config.validate().is_err());
}

#[tokio::test]
async fn test_http_client_fails_against_https_endpoint() {
    let port = spawn_tls_server().await;

    let err = test_client()
        .get(format!("http://127.0.0.1:{port}/healthz"))
        .send()
        .await
        .expect_err("plaintext client unexpectedly succeeded against tls endpoint");

    // reqwest wraps the TLS/HTTP protocol mismatch as a request error;
    // verify it is indeed a request-level failure (not e.g. a timeout or decode error)
    assert!(
        err.is_request(),
        "expected request error from protocol mismatch, got: {err:#}"
    );
}
