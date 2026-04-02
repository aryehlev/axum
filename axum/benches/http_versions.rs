//! Benchmark comparing HTTP/1 vs h2c (HTTP/2 cleartext) throughput on the
//! same axum router.
//!
//! Run with:
//! ```
//! cargo bench --bench http_versions --features http1,http2
//! ```

#![allow(missing_docs)]

use axum::{routing::get, Router};
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

    rt.spawn(axum::serve(listener, app).into_future());

    (addr, rt)
}

// ---------------------------------------------------------------------------
// HTTP/1 helpers
// ---------------------------------------------------------------------------

/// One TCP connect + one request. Measures per-connection overhead.
async fn http1_single(addr: SocketAddr) {
    let stream = TcpStream::connect(addr).await.unwrap();
    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
    tokio::spawn(conn);

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

/// N sequential requests on one persistent connection.
async fn http1_sequential(addr: SocketAddr, n: u64) {
    let stream = TcpStream::connect(addr).await.unwrap();
    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
    tokio::spawn(conn);

    for _ in 0..n {
        // Wait until the connection is ready for the next request.
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
}

/// N requests across C concurrent connections (simulates a connection pool).
async fn http1_concurrent(addr: SocketAddr, concurrency: u64, requests: u64) {
    let futs = (0..concurrency).map(|_| {
        let per = requests / concurrency;
        async move { http1_sequential(addr, per).await }
    });
    futures_util::future::join_all(futs).await;
}

// ---------------------------------------------------------------------------
// h2c helpers
// ---------------------------------------------------------------------------

/// One TCP connect + one request over h2c.
async fn h2c_single(addr: SocketAddr) {
    let stream = TcpStream::connect(addr).await.unwrap();
    let io = TokioIo::new(stream);
    let (mut sender, conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), io)
            .await
            .unwrap();
    tokio::spawn(conn);

    let req = Request::builder()
        .version(Version::HTTP_2)
        .uri(format!("http://{}/", addr))
        .body(Empty::<Bytes>::new())
        .unwrap();
    let resp = sender.send_request(req).await.unwrap();
    resp.into_body().collect().await.unwrap();
}

/// N concurrent streams on one h2c connection (full multiplexing).
async fn h2c_concurrent_streams(addr: SocketAddr, n: u64) {
    let stream = TcpStream::connect(addr).await.unwrap();
    let io = TokioIo::new(stream);
    let (mut sender, conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), io)
            .await
            .unwrap();
    tokio::spawn(conn);

    let futs: Vec<_> = (0..n)
        .map(|_| {
            let req = Request::builder()
                .version(Version::HTTP_2)
                .uri(format!("http://{}/", addr))
                .body(Empty::<Bytes>::new())
                .unwrap();
            sender.send_request(req)
        })
        .collect();

    for fut in futs {
        fut.await.unwrap().into_body().collect().await.unwrap();
    }
}

/// N requests across C h2c connections, each carrying N/C concurrent streams.
async fn h2c_concurrent_connections(addr: SocketAddr, concurrency: u64, requests: u64) {
    let futs = (0..concurrency).map(|_| {
        let per = requests / concurrency;
        async move { h2c_concurrent_streams(addr, per).await }
    });
    futures_util::future::join_all(futs).await;
}

// ---------------------------------------------------------------------------
// Benchmark: single request per connection (handshake + request cost)
// ---------------------------------------------------------------------------

fn bench_single_request(c: &mut Criterion) {
    let (addr, rt) = start_server();

    let mut group = c.benchmark_group("single_request");
    group.throughput(Throughput::Elements(1));

    group.bench_function("http1", |b| {
        b.iter(|| rt.block_on(http1_single(addr)));
    });
    group.bench_function("h2c", |b| {
        b.iter(|| rt.block_on(h2c_single(addr)));
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: N sequential requests on one persistent connection
// ---------------------------------------------------------------------------

fn bench_sequential(c: &mut Criterion) {
    let (addr, rt) = start_server();

    let mut group = c.benchmark_group("sequential_persistent");

    for n in [10u64, 100, 1_000] {
        group.throughput(Throughput::Elements(n));

        group.bench_with_input(BenchmarkId::new("http1", n), &n, |b, &n| {
            b.iter(|| rt.block_on(http1_sequential(addr, n)));
        });
        group.bench_with_input(BenchmarkId::new("h2c", n), &n, |b, &n| {
            // h2c comparison: sequential streams (not multiplexed) to match HTTP/1
            b.iter(|| rt.block_on(h2c_concurrent_streams(addr, n)));
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: concurrent requests (HTTP/1 via parallel connections,
// h2c via parallel streams on one connection)
// ---------------------------------------------------------------------------

fn bench_concurrent(c: &mut Criterion) {
    let (addr, rt) = start_server();

    // Total requests fixed at 1_000; we vary concurrency.
    const TOTAL: u64 = 1_000;

    let mut group = c.benchmark_group("concurrent");
    group.throughput(Throughput::Elements(TOTAL));

    for concurrency in [1u64, 4, 16, 64] {
        // HTTP/1: open `concurrency` parallel connections, each handling
        // TOTAL/concurrency sequential requests.
        group.bench_with_input(
            BenchmarkId::new("http1_connections", concurrency),
            &concurrency,
            |b, &c| {
                b.iter(|| rt.block_on(http1_concurrent(addr, c, TOTAL)));
            },
        );

        // h2c: open `concurrency` connections each multiplexing
        // TOTAL/concurrency concurrent streams.
        group.bench_with_input(
            BenchmarkId::new("h2c_connections", concurrency),
            &concurrency,
            |b, &c| {
                b.iter(|| rt.block_on(h2c_concurrent_connections(addr, c, TOTAL)));
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_single_request,
    bench_sequential,
    bench_concurrent
);
criterion_main!(benches);
