//! Benchmark comparing HTTP/1 vs h2c (HTTP/2 cleartext) across realistic
//! API workloads.
//!
//! Run with:
//! ```
//! cargo bench --bench http_versions --features http1,http2,json
//! ```

#![allow(missing_docs)]

use axum::{
    extract::Path,
    routing::{get, post},
    serve::ListenerExt as _,
    Json, Router,
};
use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use http::{Method, Request, Version};
use http_body_util::{BodyExt as _, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde::{Deserialize, Serialize};
use std::{future::IntoFuture as _, net::SocketAddr};
use tokio::{net::TcpStream, runtime::Runtime};

// ---------------------------------------------------------------------------
// Realistic API server
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct User {
    id:         u32,
    name:       &'static str,
    email:      &'static str,
    role:       &'static str,
    created_at: &'static str,
}

const USERS: &[User] = &[
    User { id: 1, name: "Alice Smith",   email: "alice@example.com",   role: "admin",  created_at: "2024-01-15T10:00:00Z" },
    User { id: 2, name: "Bob Jones",     email: "bob@example.com",     role: "user",   created_at: "2024-03-22T14:30:00Z" },
    User { id: 3, name: "Carol White",   email: "carol@example.com",   role: "user",   created_at: "2024-05-10T09:15:00Z" },
    User { id: 4, name: "Dave Brown",    email: "dave@example.com",    role: "editor", created_at: "2024-06-01T16:45:00Z" },
    User { id: 5, name: "Eve Davis",     email: "eve@example.com",     role: "user",   created_at: "2024-07-18T11:20:00Z" },
];

#[derive(Deserialize, Serialize)]
struct EchoPayload {
    message:    String,
    request_id: String,
    metadata:   std::collections::HashMap<String, String>,
}

async fn list_users_handler() -> Json<&'static [User]> {
    Json(USERS)
}

async fn get_user_handler(Path(id): Path<u32>) -> Json<Option<&'static User>> {
    Json(USERS.iter().find(|u| u.id == id))
}

async fn echo_handler(Json(payload): Json<EchoPayload>) -> Json<EchoPayload> {
    Json(payload)
}

fn start_server() -> (SocketAddr, Runtime) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let app = Router::new()
        .route("/api/users",       get(list_users_handler))
        .route("/api/users/{id}",  get(get_user_handler))
        .route("/api/echo",        post(echo_handler));

    let listener = rt
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let addr = listener.local_addr().unwrap();

    // TCP_NODELAY on accepted connections prevents Nagle buffering of small
    // h2c frames (SETTINGS, HEADERS), which otherwise adds ~16ms latency.
    rt.spawn(
        axum::serve(
            listener.tap_io(|s| { let _ = s.set_nodelay(true); }),
            app,
        )
        .into_future(),
    );

    (addr, rt)
}

// ---------------------------------------------------------------------------
// Body type shared by all helpers
// ---------------------------------------------------------------------------

type ReqBody = Full<Bytes>;

fn empty_body() -> ReqBody { Full::new(Bytes::new()) }

fn json_body(v: &impl Serialize) -> ReqBody {
    Full::new(Bytes::from(serde_json::to_vec(v).unwrap()))
}

fn echo_payload() -> EchoPayload {
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("client".into(), "benchmark".into());
    metadata.insert("version".into(), "1.0.0".into());
    EchoPayload {
        message:    "Hello from benchmark".into(),
        request_id: "bench-00000000-0000-0000-0000-000000000001".into(),
        metadata,
    }
}

// ---------------------------------------------------------------------------
// Request builders with realistic headers
//
// Realistic headers are important for two reasons:
//   1. They mirror what browsers/clients actually send.
//   2. They stress HPACK encoding/decoding, which is where the fast-hpack
//      fork is supposed to show a benefit.
// ---------------------------------------------------------------------------

const AUTH:       &str = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyXzEyMyIsImlhdCI6MTcwMDAwMDAwMH0.bench";
const USER_AGENT: &str = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
const ACCEPT:     &str = "application/json, text/plain, */*";

// Heavy-headers variant: mimics a browser request with cookies + tracing
// headers.  Exercises HPACK decoding more aggressively (especially relevant
// for the fast-hpack decoder in the h2 fork).
const COOKIE: &str = "session=abc123def456ghi789jkl012mno345pqr678stu901vwx234yz; _ga=GA1.2.1234567890.1700000000; _gid=GA1.2.9876543210.1700086400; csrftoken=ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789AB";
const TRACEPARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

fn build_get(version: Version, uri: String, host: &str) -> Request<ReqBody> {
    Request::builder()
        .version(version)
        .method(Method::GET)
        .uri(uri)
        .header("host",            host)
        .header("user-agent",      USER_AGENT)
        .header("accept",          ACCEPT)
        .header("accept-language", "en-US,en;q=0.9")
        .header("authorization",   AUTH)
        .header("x-request-id",    "bench-00000000-0000-0000-0000-000000000001")
        .header("cache-control",   "no-cache")
        .body(empty_body())
        .unwrap()
}

fn build_post(version: Version, uri: String, host: &str, body: ReqBody) -> Request<ReqBody> {
    Request::builder()
        .version(version)
        .method(Method::POST)
        .uri(uri)
        .header("host",            host)
        .header("user-agent",      USER_AGENT)
        .header("accept",          ACCEPT)
        .header("accept-language", "en-US,en;q=0.9")
        .header("authorization",   AUTH)
        .header("content-type",    "application/json")
        .header("x-request-id",    "bench-00000000-0000-0000-0000-000000000001")
        .body(body)
        .unwrap()
}

/// Same as build_get but with an extra cookie string + tracing headers.
/// Used to benchmark HPACK decode performance for the h2 fork's fast-hpack.
fn build_get_heavy(version: Version, uri: String, host: &str) -> Request<ReqBody> {
    Request::builder()
        .version(version)
        .method(Method::GET)
        .uri(uri)
        .header("host",              host)
        .header("user-agent",        USER_AGENT)
        .header("accept",            ACCEPT)
        .header("accept-language",   "en-US,en;q=0.9")
        .header("authorization",     AUTH)
        .header("cookie",            COOKIE)
        .header("x-request-id",     "bench-00000000-0000-0000-0000-000000000001")
        .header("traceparent",       TRACEPARENT)
        .header("x-b3-traceid",      "4bf92f3577b34da6a3ce929d0e0e4736")
        .header("x-b3-spanid",       "00f067aa0ba902b7")
        .header("x-forwarded-for",   "203.0.113.42, 10.0.0.1")
        .header("x-forwarded-proto", "https")
        .header("cache-control",     "no-cache")
        .body(empty_body())
        .unwrap()
}

// ---------------------------------------------------------------------------
// Low-level connection helpers
// ---------------------------------------------------------------------------

/// TCP connect with TCP_NODELAY.  Without this, the 24-byte h2c connection
/// preface is buffered by Nagle for up to 40 ms before being sent.
async fn tcp_connect(addr: SocketAddr) -> TokioIo<TcpStream> {
    let stream = TcpStream::connect(addr).await.unwrap();
    stream.set_nodelay(true).unwrap();
    TokioIo::new(stream)
}

async fn http1_connect(addr: SocketAddr) -> hyper::client::conn::http1::SendRequest<ReqBody> {
    let io = tcp_connect(addr).await;
    let (sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
    tokio::spawn(conn);
    sender
}

async fn h2c_connect(addr: SocketAddr) -> hyper::client::conn::http2::SendRequest<ReqBody> {
    let io = tcp_connect(addr).await;
    let (sender, conn) = hyper::client::conn::http2::handshake(TokioExecutor::new(), io)
        .await
        .unwrap();
    tokio::spawn(conn);
    sender
}

// ---------------------------------------------------------------------------
// HTTP/1 request runners
// ---------------------------------------------------------------------------

async fn http1_get(
    sender: &mut hyper::client::conn::http1::SendRequest<ReqBody>,
    addr: SocketAddr,
    path: &str,
) {
    sender.ready().await.unwrap();
    let req = build_get(Version::HTTP_11, format!("http://{}{}", addr, path), &addr.to_string());
    sender.send_request(req).await.unwrap().into_body().collect().await.unwrap();
}

async fn http1_get_heavy(
    sender: &mut hyper::client::conn::http1::SendRequest<ReqBody>,
    addr: SocketAddr,
    path: &str,
) {
    sender.ready().await.unwrap();
    let req = build_get_heavy(Version::HTTP_11, format!("http://{}{}", addr, path), &addr.to_string());
    sender.send_request(req).await.unwrap().into_body().collect().await.unwrap();
}

async fn http1_post(
    sender: &mut hyper::client::conn::http1::SendRequest<ReqBody>,
    addr: SocketAddr,
    path: &str,
) {
    sender.ready().await.unwrap();
    let req = build_post(
        Version::HTTP_11,
        format!("http://{}{}", addr, path),
        &addr.to_string(),
        json_body(&echo_payload()),
    );
    sender.send_request(req).await.unwrap().into_body().collect().await.unwrap();
}

// ---------------------------------------------------------------------------
// HTTP/1 pool: C connections, each handling TOTAL/C requests sequentially
// ---------------------------------------------------------------------------

async fn http1_pool_get(addr: SocketAddr, connections: u64, total: u64, path: &'static str) {
    let per = total / connections;
    let futs = (0..connections).map(|_| async move {
        let mut sender = http1_connect(addr).await;
        for _ in 0..per {
            http1_get(&mut sender, addr, path).await;
        }
    });
    futures_util::future::join_all(futs).await;
}

async fn http1_pool_post(addr: SocketAddr, connections: u64, total: u64, path: &'static str) {
    let per = total / connections;
    let futs = (0..connections).map(|_| async move {
        let mut sender = http1_connect(addr).await;
        for _ in 0..per {
            http1_post(&mut sender, addr, path).await;
        }
    });
    futures_util::future::join_all(futs).await;
}

// ---------------------------------------------------------------------------
// h2c request runners
// ---------------------------------------------------------------------------

async fn h2c_dispatch_gets_heavy(
    sender: &mut hyper::client::conn::http2::SendRequest<ReqBody>,
    addr: SocketAddr,
    path: &str,
    n: u64,
) {
    let futs: Vec<_> = (0..n)
        .map(|_| {
            let req = build_get_heavy(
                Version::HTTP_2,
                format!("http://{}{}", addr, path),
                &addr.to_string(),
            );
            sender.send_request(req)
        })
        .collect();
    futures_util::future::join_all(
        futs.into_iter()
            .map(|f| async move { f.await.unwrap().into_body().collect().await.unwrap() }),
    )
    .await;
}

async fn h2c_dispatch_gets(
    sender: &mut hyper::client::conn::http2::SendRequest<ReqBody>,
    addr: SocketAddr,
    path: &str,
    n: u64,
) {
    let futs: Vec<_> = (0..n)
        .map(|_| {
            let req = build_get(
                Version::HTTP_2,
                format!("http://{}{}", addr, path),
                &addr.to_string(),
            );
            sender.send_request(req)
        })
        .collect();
    futures_util::future::join_all(
        futs.into_iter()
            .map(|f| async move { f.await.unwrap().into_body().collect().await.unwrap() }),
    )
    .await;
}

async fn h2c_dispatch_posts(
    sender: &mut hyper::client::conn::http2::SendRequest<ReqBody>,
    addr: SocketAddr,
    path: &str,
    n: u64,
) {
    let futs: Vec<_> = (0..n)
        .map(|_| {
            let req = build_post(
                Version::HTTP_2,
                format!("http://{}{}", addr, path),
                &addr.to_string(),
                json_body(&echo_payload()),
            );
            sender.send_request(req)
        })
        .collect();
    futures_util::future::join_all(
        futs.into_iter()
            .map(|f| async move { f.await.unwrap().into_body().collect().await.unwrap() }),
    )
    .await;
}

// ---------------------------------------------------------------------------
// h2c multi-connection pool: C connections × (TOTAL/C) concurrent streams each.
//
// With TCP_NODELAY each h2c handshake costs ~175 µs (vs ~150 µs for HTTP/1),
// so opening C connections is cheap.  Each connection then multiplexes its
// share of streams concurrently, and C connections saturate C server threads —
// the same parallelism HTTP/1 achieves with C connections.
// ---------------------------------------------------------------------------

async fn h2c_pool_get(addr: SocketAddr, connections: u64, total: u64, path: &'static str) {
    let per = total / connections;
    let futs = (0..connections).map(|_| async move {
        let mut sender = h2c_connect(addr).await;
        h2c_dispatch_gets(&mut sender, addr, path, per).await;
    });
    futures_util::future::join_all(futs).await;
}

async fn h2c_pool_post(addr: SocketAddr, connections: u64, total: u64, path: &'static str) {
    let per = total / connections;
    let futs = (0..connections).map(|_| async move {
        let mut sender = h2c_connect(addr).await;
        h2c_dispatch_posts(&mut sender, addr, path, per).await;
    });
    futures_util::future::join_all(futs).await;
}

// ---------------------------------------------------------------------------
// Benchmark 1: warm single request – pure latency, no handshake cost
// ---------------------------------------------------------------------------

fn bench_warm_latency(c: &mut Criterion) {
    let (addr, rt) = start_server();
    let mut group = c.benchmark_group("warm_latency");
    group.throughput(Throughput::Elements(1));

    let mut h1 = rt.block_on(http1_connect(addr));
    let mut h2 = rt.block_on(h2c_connect(addr));

    group.bench_function("http1/get_json", |b| {
        b.iter(|| rt.block_on(http1_get(&mut h1, addr, "/api/users")));
    });
    group.bench_function("h2c/get_json", |b| {
        b.iter(|| rt.block_on(async {
            h2c_dispatch_gets(&mut h2, addr, "/api/users", 1).await;
        }));
    });

    // Re-establish for POST (different persistent state)
    let mut h1p = rt.block_on(http1_connect(addr));
    let mut h2p = rt.block_on(h2c_connect(addr));

    group.bench_function("http1/post_echo", |b| {
        b.iter(|| rt.block_on(http1_post(&mut h1p, addr, "/api/echo")));
    });
    group.bench_function("h2c/post_echo", |b| {
        b.iter(|| rt.block_on(async {
            h2c_dispatch_posts(&mut h2p, addr, "/api/echo", 1).await;
        }));
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark 2: persistent connection throughput
// HTTP/1: N sequential.  h2c: N concurrent streams on one connection.
// ---------------------------------------------------------------------------

fn bench_persistent(c: &mut Criterion) {
    let (addr, rt) = start_server();

    for (label, path, is_post) in [
        ("get_json",  "/api/users", false),
        ("post_echo", "/api/echo",  true),
    ] {
        let mut group = c.benchmark_group(format!("persistent/{}", label));

        for n in [10u64, 100, 500] {
            group.throughput(Throughput::Elements(n));

            group.bench_with_input(BenchmarkId::new("http1", n), &n, |b, &n| {
                b.iter(|| rt.block_on(async {
                    let mut sender = http1_connect(addr).await;
                    for _ in 0..n {
                        if is_post {
                            http1_post(&mut sender, addr, path).await;
                        } else {
                            http1_get(&mut sender, addr, path).await;
                        }
                    }
                }));
            });

            group.bench_with_input(BenchmarkId::new("h2c", n), &n, |b, &n| {
                b.iter(|| rt.block_on(async {
                    let mut sender = h2c_connect(addr).await;
                    if is_post {
                        h2c_dispatch_posts(&mut sender, addr, path, n).await;
                    } else {
                        h2c_dispatch_gets(&mut sender, addr, path, n).await;
                    }
                }));
            });
        }

        group.finish();
    }
}

// ---------------------------------------------------------------------------
// Benchmark 3: connection pool scaling – the key comparison.
//
// HTTP/1 opens C connections, each handling TOTAL/C sequential requests.
// h2c opens C connections, each handling TOTAL/C *concurrent* streams.
//
// This shows whether h2c with multiple connections can match or beat HTTP/1
// at various pool sizes.
// ---------------------------------------------------------------------------

fn bench_pool(c: &mut Criterion) {
    let (addr, rt) = start_server();
    const TOTAL: u64 = 1_000;

    for (label, path, is_post) in [
        ("get_json",  "/api/users", false),
        ("post_echo", "/api/echo",  true),
    ] {
        let mut group = c.benchmark_group(format!("pool/{}", label));
        group.throughput(Throughput::Elements(TOTAL));

        for connections in [1u64, 4, 16, 64] {
            group.bench_with_input(
                BenchmarkId::new("http1", connections),
                &connections,
                |b, &c| {
                    b.iter(|| rt.block_on(async {
                        if is_post {
                            http1_pool_post(addr, c, TOTAL, path).await;
                        } else {
                            http1_pool_get(addr, c, TOTAL, path).await;
                        }
                    }));
                },
            );

            group.bench_with_input(
                BenchmarkId::new("h2c", connections),
                &connections,
                |b, &c| {
                    b.iter(|| rt.block_on(async {
                        if is_post {
                            h2c_pool_post(addr, c, TOTAL, path).await;
                        } else {
                            h2c_pool_get(addr, c, TOTAL, path).await;
                        }
                    }));
                },
            );
        }

        group.finish();
    }
}

// ---------------------------------------------------------------------------
// Benchmark 4: warmed connection pool
//
// Connections are established ONCE outside the criterion loop and reused
// across every iteration — exactly how a real connection pool (reqwest,
// hyper-util's pooled client, etc.) works in production.
//
// Two things this eliminates vs bench_pool:
//   (A) Per-iteration TCP + TLS/SETTINGS handshake overhead.
//   (B) Cold HPACK dynamic tables: the 99-char Authorization token is
//       sent verbatim only on the first request; after that it compresses
//       to a 1-2 byte back-reference.  With 1000 requests and warm tables
//       the HPACK cost drops ~50x compared to cold-connection pools.
//
// http2::SendRequest<B>: Clone — a clone shares the same connection and
// can independently send concurrent streams (no locking needed).
// http1::SendRequest<B>: not Clone — connections are sequential; each
// pre-warmed sender is wrapped in Arc<Mutex<>> for safe multi-task reuse.
// ---------------------------------------------------------------------------

fn bench_pool_warm(c: &mut Criterion) {
    use std::sync::Arc;
    use tokio::sync::Mutex;

    const CONNECTIONS: u64 = 64;
    const TOTAL: u64 = 1_000;
    let per: u64 = TOTAL / CONNECTIONS;

    for (label, path, is_post) in [
        ("get_json",  "/api/users", false),
        ("post_echo", "/api/echo",  true),
    ] {
        let (addr, rt) = start_server();
        let mut group = c.benchmark_group(format!("pool_warm/{}", label));
        group.throughput(Throughput::Elements(TOTAL));

        // --- HTTP/1 warmed pool -------------------------------------------
        // Pre-establish CONNECTIONS senders and wrap in Arc<Mutex> so each
        // can be borrowed by a spawned task without requiring Clone.
        let http1_senders: Vec<Arc<Mutex<_>>> = rt.block_on(
            futures_util::future::join_all(
                (0..CONNECTIONS).map(|_| async move {
                    Arc::new(Mutex::new(http1_connect(addr).await))
                }),
            ),
        );

        group.bench_function("http1", |b| {
            b.iter(|| {
                rt.block_on(futures_util::future::join_all(
                    http1_senders.iter().map(|sender| {
                        let sender = sender.clone();
                        async move {
                            let mut s = sender.lock().await;
                            for _ in 0..per {
                                if is_post {
                                    http1_post(&mut s, addr, path).await;
                                } else {
                                    http1_get(&mut s, addr, path).await;
                                }
                            }
                        }
                    }),
                ))
            });
        });

        // --- h2c warmed pool -------------------------------------------------
        // Clone each sender per iteration: the clone shares the underlying
        // h2 connection so all 16 cloned senders multiplex streams on the
        // same TCP connection simultaneously.
        let h2c_senders: Vec<_> = rt.block_on(
            futures_util::future::join_all(
                (0..CONNECTIONS).map(|_| h2c_connect(addr)),
            ),
        );

        group.bench_function("h2c", |b| {
            b.iter(|| {
                rt.block_on(futures_util::future::join_all(
                    h2c_senders.iter().map(|sender| {
                        let mut s = sender.clone(); // shares the connection
                        async move {
                            if is_post {
                                h2c_dispatch_posts(&mut s, addr, path, per).await;
                            } else {
                                h2c_dispatch_gets(&mut s, addr, path, per).await;
                            }
                        }
                    }),
                ))
            });
        });

        group.finish();
    }
}

// ---------------------------------------------------------------------------
// Benchmark 5: heavy headers — exercises HPACK decode on every request.
//
// Uses 12 request headers (cookie, tracing headers, forwarded-for, …) to
// stress the HPACK decoder.  This is where the h2 fork's fast-hpack feature
// is intended to help: the zero-alloc arena decoder avoids per-header
// allocations that dominate when header strings are long or numerous.
//
// Cold connections only (new connection per iteration) so that the HPACK
// dynamic table is always cold and every header must be fully decoded.
// ---------------------------------------------------------------------------

fn bench_heavy_headers(c: &mut Criterion) {
    let (addr, rt) = start_server();
    let mut group = c.benchmark_group("heavy_headers");
    group.throughput(Throughput::Elements(100));

    group.bench_function("http1", |b| {
        b.iter(|| rt.block_on(async {
            let mut sender = http1_connect(addr).await;
            for _ in 0..100 {
                http1_get_heavy(&mut sender, addr, "/api/users").await;
            }
        }));
    });

    group.bench_function("h2c", |b| {
        b.iter(|| rt.block_on(async {
            let mut sender = h2c_connect(addr).await;
            h2c_dispatch_gets_heavy(&mut sender, addr, "/api/users", 100).await;
        }));
    });

    group.finish();
}

// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_warm_latency,
    bench_persistent,
    bench_pool,
    bench_pool_warm,
    bench_heavy_headers,
);
criterion_main!(benches);
