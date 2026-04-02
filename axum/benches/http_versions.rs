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

    // Drive the server on the same multi-thread runtime so HTTP/2 WINDOW_UPDATE
    // frames are processed concurrently with client sends.
    rt.spawn(axum::serve(listener, app).into_future());

    (addr, rt)
}

// ---------------------------------------------------------------------------
// HTTP/1 helpers
// ---------------------------------------------------------------------------

async fn http1_request(addr: SocketAddr) {
    let stream = TcpStream::connect(addr).await.unwrap();
    let io = TokioIo::new(stream);

    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
    tokio::spawn(conn);

    let req = Request::builder()
        .version(Version::HTTP_11)
        .uri("/")
        .header("host", addr.to_string())
        .body(Empty::<Bytes>::new())
        .unwrap();

    let resp = sender.send_request(req).await.unwrap();
    // consume the body so the connection is truly done
    resp.into_body().collect().await.unwrap();
}

// ---------------------------------------------------------------------------
// h2c helpers
// ---------------------------------------------------------------------------

async fn h2c_request(addr: SocketAddr) {
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

// ---------------------------------------------------------------------------
// Pipelined / multiplexed variants: reuse a single connection for N requests
// ---------------------------------------------------------------------------

async fn http1_pipelined(addr: SocketAddr, n: u64) {
    let stream = TcpStream::connect(addr).await.unwrap();
    let io = TokioIo::new(stream);

    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
    tokio::spawn(conn);

    for _ in 0..n {
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

async fn h2c_multiplexed(addr: SocketAddr, n: u64) {
    let stream = TcpStream::connect(addr).await.unwrap();
    let io = TokioIo::new(stream);

    let (mut sender, conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), io)
            .await
            .unwrap();
    tokio::spawn(conn);

    // HTTP/2 streams are independent; fire them all concurrently and join.
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

// ---------------------------------------------------------------------------
// Benchmark: single request per connection (measures connection + handshake)
// ---------------------------------------------------------------------------

fn bench_single_request(c: &mut Criterion) {
    let (addr, rt) = start_server();

    let mut group = c.benchmark_group("single_request");
    group.throughput(Throughput::Elements(1));

    group.bench_function("http1", |b| {
        b.iter(|| rt.block_on(http1_request(addr)));
    });

    group.bench_function("h2c", |b| {
        b.iter(|| rt.block_on(h2c_request(addr)));
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: N sequential requests over one persistent connection
// ---------------------------------------------------------------------------

fn bench_persistent_connection(c: &mut Criterion) {
    let (addr, rt) = start_server();

    let mut group = c.benchmark_group("persistent_connection");

    for n in [10u64, 100, 1_000] {
        group.throughput(Throughput::Elements(n));

        group.bench_with_input(BenchmarkId::new("http1", n), &n, |b, &n| {
            b.iter(|| rt.block_on(http1_pipelined(addr, n)));
        });

        group.bench_with_input(BenchmarkId::new("h2c", n), &n, |b, &n| {
            b.iter(|| rt.block_on(h2c_multiplexed(addr, n)));
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------

criterion_group!(benches, bench_single_request, bench_persistent_connection);
criterion_main!(benches);
