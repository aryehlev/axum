//! Benchmark comparing HTTP/1 vs h2c (HTTP/2 cleartext) throughput on the
//! same axum router.
//!
//! Run with:
//! ```
//! cargo bench --bench http_versions --features http1,http2
//! ```

#![allow(missing_docs)]

use axum::{routing::get, serve::ListenerExt as _, Router};
use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use http::{Request, Version};
use http_body_util::{BodyExt as _, Empty};
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::{future::IntoFuture as _, net::SocketAddr};
use tokio::{net::TcpStream, runtime::Runtime};

// ---------------------------------------------------------------------------
// Shared server setup
// ---------------------------------------------------------------------------

fn start_server() -> (SocketAddr, Runtime) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let app = Router::new().route("/", get(|| async { "Hello, World!" }));

    let listener = rt
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let addr = listener.local_addr().unwrap();

    // Set TCP_NODELAY on every accepted connection so small h2c frames
    // (SETTINGS, HEADERS) are not held by Nagle's algorithm on the server side.
    rt.spawn(
        axum::serve(
            listener.tap_io(|stream| {
                let _ = stream.set_nodelay(true);
            }),
            app,
        )
        .into_future(),
    );

    (addr, rt)
}

// ---------------------------------------------------------------------------
// TCP helpers: always set TCP_NODELAY to avoid Nagle-induced frame delays.
// Without it, the tiny h2c connection preface (24 bytes) and SETTINGS frame
// are buffered for up to 40 ms before the OS flushes them.
// ---------------------------------------------------------------------------

async fn tcp_connect(addr: SocketAddr) -> TokioIo<TcpStream> {
    let stream = TcpStream::connect(addr).await.unwrap();
    stream.set_nodelay(true).unwrap();
    TokioIo::new(stream)
}

// ---------------------------------------------------------------------------
// HTTP/1 helpers
// ---------------------------------------------------------------------------

async fn http1_send(
    sender: &mut hyper::client::conn::http1::SendRequest<Empty<Bytes>>,
    addr: SocketAddr,
) {
    sender.ready().await.unwrap();
    let req = Request::builder()
        .version(Version::HTTP_11)
        .uri("/")
        .header("host", addr.to_string())
        .body(Empty::<Bytes>::new())
        .unwrap();
    let resp = sender.send_request(req).await.unwrap();
    resp.into_body().collect().await.unwrap();
}

/// One TCP connect + one request.  Measures cold-start cost.
async fn http1_cold(addr: SocketAddr) {
    let io = tcp_connect(addr).await;
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
    tokio::spawn(conn);
    http1_send(&mut sender, addr).await;
}

/// One request on a pre-established connection.  Measures pure request cost.
async fn http1_warm(
    sender: &mut hyper::client::conn::http1::SendRequest<Empty<Bytes>>,
    addr: SocketAddr,
) {
    http1_send(sender, addr).await;
}

/// N sequential requests on one persistent connection.
async fn http1_sequential(addr: SocketAddr, n: u64) {
    let io = tcp_connect(addr).await;
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
    tokio::spawn(conn);
    for _ in 0..n {
        http1_send(&mut sender, addr).await;
    }
}

/// 1000 requests across `concurrency` connections (simulates a pool).
async fn http1_pool(addr: SocketAddr, concurrency: u64) {
    const TOTAL: u64 = 1_000;
    let futs = (0..concurrency).map(|_| http1_sequential(addr, TOTAL / concurrency));
    futures_util::future::join_all(futs).await;
}

// ---------------------------------------------------------------------------
// h2c helpers
// ---------------------------------------------------------------------------

async fn h2c_connect(
    addr: SocketAddr,
) -> hyper::client::conn::http2::SendRequest<Empty<Bytes>> {
    let io = tcp_connect(addr).await;
    let (sender, conn) = hyper::client::conn::http2::handshake(TokioExecutor::new(), io)
        .await
        .unwrap();
    tokio::spawn(conn);
    sender
}

async fn h2c_send(
    sender: &mut hyper::client::conn::http2::SendRequest<Empty<Bytes>>,
    addr: SocketAddr,
) {
    let req = Request::builder()
        .version(Version::HTTP_2)
        .uri(format!("http://{}/", addr))
        .body(Empty::<Bytes>::new())
        .unwrap();
    let resp = sender.send_request(req).await.unwrap();
    resp.into_body().collect().await.unwrap();
}

/// One TCP connect + one request over h2c.  Measures cold-start cost.
async fn h2c_cold(addr: SocketAddr) {
    let mut sender = h2c_connect(addr).await;
    h2c_send(&mut sender, addr).await;
}

/// One request on a pre-established h2c connection.
async fn h2c_warm(
    sender: &mut hyper::client::conn::http2::SendRequest<Empty<Bytes>>,
    addr: SocketAddr,
) {
    h2c_send(sender, addr).await;
}

/// N concurrent streams on one h2c connection.
/// Requests are dispatched all at once, then collected concurrently via
/// join_all so that a slow stream doesn't block faster ones.
async fn h2c_streams(addr: SocketAddr, n: u64) {
    let mut sender = h2c_connect(addr).await;

    let response_futs: Vec<_> = (0..n)
        .map(|_| {
            let req = Request::builder()
                .version(Version::HTTP_2)
                .uri(format!("http://{}/", addr))
                .body(Empty::<Bytes>::new())
                .unwrap();
            sender.send_request(req)
        })
        .collect();

    // Collect all responses concurrently – not sequentially.
    futures_util::future::join_all(
        response_futs
            .into_iter()
            .map(|f| async move { f.await.unwrap().into_body().collect().await.unwrap() }),
    )
    .await;
}

/// 1000 requests all dispatched concurrently on one h2c connection.
/// All streams are in-flight simultaneously; responses are joined together.
/// This is the natural HTTP/2 usage pattern — no artificial batching.
async fn h2c_pool(addr: SocketAddr) {
    const TOTAL: u64 = 1_000;
    let mut sender = h2c_connect(addr).await;

    let response_futs: Vec<_> = (0..TOTAL)
        .map(|_| {
            let req = Request::builder()
                .version(Version::HTTP_2)
                .uri(format!("http://{}/", addr))
                .body(Empty::<Bytes>::new())
                .unwrap();
            sender.send_request(req)
        })
        .collect();

    futures_util::future::join_all(
        response_futs
            .into_iter()
            .map(|f| async move { f.await.unwrap().into_body().collect().await.unwrap() }),
    )
    .await;
}

// ---------------------------------------------------------------------------
// Benchmark 1: cold start (new connection every iteration)
// Shows handshake overhead for each protocol.
// ---------------------------------------------------------------------------

fn bench_cold_start(c: &mut Criterion) {
    let (addr, rt) = start_server();
    let mut group = c.benchmark_group("cold_start");
    group.throughput(Throughput::Elements(1));

    group.bench_function("http1", |b| {
        b.iter(|| rt.block_on(http1_cold(addr)));
    });
    group.bench_function("h2c", |b| {
        b.iter(|| rt.block_on(h2c_cold(addr)));
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark 2: warm request (connection established outside criterion loop)
// Shows pure request/response latency with no handshake cost.
// ---------------------------------------------------------------------------

fn bench_warm_request(c: &mut Criterion) {
    let (addr, rt) = start_server();
    let mut group = c.benchmark_group("warm_request");
    group.throughput(Throughput::Elements(1));

    // Pre-establish connections outside the measurement loop.
    let mut http1_sender = rt.block_on(async {
        let io = tcp_connect(addr).await;
        let (sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
        tokio::spawn(conn);
        sender
    });
    let mut h2c_sender = rt.block_on(h2c_connect(addr));

    group.bench_function("http1", |b| {
        b.iter(|| rt.block_on(http1_warm(&mut http1_sender, addr)));
    });
    group.bench_function("h2c", |b| {
        b.iter(|| rt.block_on(h2c_warm(&mut h2c_sender, addr)));
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark 3: throughput on one persistent connection
// HTTP/1: N sequential requests.  h2c: N concurrent streams, joined together.
// ---------------------------------------------------------------------------

fn bench_persistent(c: &mut Criterion) {
    let (addr, rt) = start_server();
    let mut group = c.benchmark_group("persistent_connection");

    for n in [10u64, 100, 1_000] {
        group.throughput(Throughput::Elements(n));

        group.bench_with_input(BenchmarkId::new("http1", n), &n, |b, &n| {
            b.iter(|| rt.block_on(http1_sequential(addr, n)));
        });
        group.bench_with_input(BenchmarkId::new("h2c", n), &n, |b, &n| {
            b.iter(|| rt.block_on(h2c_streams(addr, n)));
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark 4: concurrency scaling
// HTTP/1 scales by adding connections (1 → 256); h2c uses 1 connection with
// all 1_000 streams dispatched concurrently.  Total requests fixed at 1_000.
// ---------------------------------------------------------------------------

fn bench_concurrency(c: &mut Criterion) {
    let (addr, rt) = start_server();
    let mut group = c.benchmark_group("concurrency_scaling");
    group.throughput(Throughput::Elements(1_000));

    for concurrency in [1u64, 4, 16, 64, 256] {
        // HTTP/1: `concurrency` connections, each handling 1000/concurrency
        // requests sequentially.
        group.bench_with_input(
            BenchmarkId::new("http1_connections", concurrency),
            &concurrency,
            |b, &c| {
                b.iter(|| rt.block_on(http1_pool(addr, c)));
            },
        );
    }

    // h2c: 1 connection, all 1_000 streams in-flight simultaneously.
    // HTTP/2 multiplexing means adding connections is unnecessary; the single
    // connection line shows the ceiling h2c can achieve.
    group.bench_function("h2c_1conn_1000streams", |b| {
        b.iter(|| rt.block_on(h2c_pool(addr)));
    });

    group.finish();
}

// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_cold_start,
    bench_warm_request,
    bench_persistent,
    bench_concurrency,
);
criterion_main!(benches);
