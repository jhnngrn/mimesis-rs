//! mimesis-rs server binary.

use mimesis::server::{Config, Server};
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let cfg = config_from_env();
    let verbose = cfg.verbose;
    let listen = cfg.listen.clone();

    let server = Server::new(cfg).expect("server init failed");
    let server = Arc::new(server);

    if let Err(e) = server.init() {
        eprintln!("init failed: {}", e);
        std::process::exit(1);
    }

    // Flush all nodes at boot to resume pending work (matches Go kickAll).
    server.flush_all();

    let addr = parse_listen(&listen);
    let listener = TcpListener::bind(addr).await.expect("bind failed");

    if verbose {
        eprintln!("[mimesis] listening on {}", addr);
    } else {
        eprintln!("listening on {}", addr);
    }

    loop {
        let (stream, _remote) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                eprintln!("accept error: {}", e);
                continue;
            }
        };
        let srv = Arc::clone(&server);
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = service_fn(move |req| {
                let s = Arc::clone(&srv);
                async move { s.handle_request(req).await }
            });
            if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
                if !e.to_string().contains("connection closed") {
                    eprintln!("http error: {}", e);
                }
            }
        });
    }
}

/// Parse listen address. Supports ":8471" (all interfaces) or "127.0.0.1:8471".
fn parse_listen(s: &str) -> SocketAddr {
    if s.starts_with(':') {
        format!("0.0.0.0{}", s).parse().expect("bad listen address")
    } else {
        s.parse().expect("bad listen address")
    }
}

/// Build Config from environment variables.
fn config_from_env() -> Config {
    let mut cfg = Config::default();

    if let Ok(v) = env::var("MIMESIS_DOMAIN") {
        cfg.domain = v;
    }
    if let Ok(v) = env::var("MIMESIS_LISTEN") {
        cfg.listen = v;
    }
    if let Ok(v) = env::var("MIMESIS_DATA_DIR") {
        cfg.data_dir = v;
    }
    if let Ok(v) = env::var("MIMESIS_ADMIN_USER") {
        cfg.admin_user = v;
    }
    if let Ok(v) = env::var("MIMESIS_ADMIN_PASS") {
        cfg.admin_pass = v;
    }
    if let Ok(v) = env::var("MIMESIS_PEER_SCHEME") {
        cfg.peer_scheme = v;
    }
    if env::var("MIMESIS_VERBOSE").map(|v| v == "1" || v == "true").unwrap_or(false) {
        cfg.verbose = true;
    }

    if cfg.domain.is_empty() {
        eprintln!("MIMESIS_DOMAIN is required");
        std::process::exit(1);
    }

    cfg
}
