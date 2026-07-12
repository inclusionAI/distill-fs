// Copyright (c) 2026 Ant Group Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::backend::chunkdb::{CheckSum, CheckSumOnDisk, ChunkDB, ChunkIndexControl};
use crate::backend::CHUNK_SIZE;
use crate::utils::now_epoch_secs;
use anyhow::{bail, Context};
use async_trait::async_trait;
use opentelemetry::global;
use opentelemetry::metrics::{CallbackRegistration, Counter, Histogram, Meter, UpDownCounter};
use opentelemetry::KeyValue;
use rand::{seq::SliceRandom, Rng};
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::future::Future;
#[cfg(test)]
use std::io::Read;
use std::io::{self, ErrorKind};
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::os::unix::net::UnixListener as StdUnixListener;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::OnceLock;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
use tokio::runtime::{Builder, Handle, Runtime};
use tokio::sync::{mpsc, oneshot, Mutex, Notify, Semaphore};
use tokio::task::{block_in_place, JoinHandle};
use tokio::time::{interval, timeout};
use tracing::{debug, info, warn};

#[cfg(all(test, target_os = "linux"))]
use tokio::io::split;

const REQUEST_LEN: usize = 50;
const RESPONSE_HEADER_LEN: usize = 13;
const STATUS_HIT: u8 = 0;
const STATUS_MISS: u8 = 1;
const STATUS_ERROR: u8 = 2;
const DEFAULT_TIMEOUT_MS: u64 = 1000;
const DEFAULT_MAX_QUERY_PEERS: usize = 3;
const HINT_MAX_RECENT: usize = 256;
const HINT_TTL_SECS: u64 = 300;
const HINT_GC_INTERVAL_SECS: u64 = 30;
const MAX_CHUNK_PAYLOAD_SIZE: usize = CHUNK_SIZE;
const DEFAULT_MAX_CONNECTIONS: usize = 8;
const DISCOVERY_TTL_SECS: u64 = 60;
const DISCOVERY_REFRESH_SECS: u64 = 20;
const REDIS_REGISTER_BATCH_SIZE: usize = 1000;
const INDEX_SYNC_BATCH_SIZE: usize = 1000;
const INDEX_REPAIR_BATCH_SIZE: usize = 256;
const MAX_CHUNK_OWNERS: usize = 3;
const CONNECTION_POOL_MIN_IDLE: usize = 1;
const CONNECTION_POOL_MAX_SIZE: usize = 8;
const CONNECTION_POOL_IDLE_TTL_SECS: u64 = 30;
const SERVER_KEEPALIVE_IDLE_TIMEOUT_SECS: u64 = 30;
const SESSION_MAX_INFLIGHT: usize = 32;

fn session_clock_base() -> Instant {
    static BASE: OnceLock<Instant> = OnceLock::new();
    *BASE.get_or_init(Instant::now)
}

fn monotonic_now_micros() -> u64 {
    session_clock_base().elapsed().as_micros() as u64
}

fn instant_from_micros(micros: u64) -> Instant {
    session_clock_base() + Duration::from_micros(micros)
}

#[derive(Debug, Clone, Copy)]
struct ConnectionPoolConfig {
    min_idle: usize,
    max_size: usize,
    idle_ttl: Duration,
}

impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        Self {
            min_idle: CONNECTION_POOL_MIN_IDLE,
            max_size: CONNECTION_POOL_MAX_SIZE,
            idle_ttl: Duration::from_secs(CONNECTION_POOL_IDLE_TTL_SECS),
        }
    }
}

#[derive(Debug)]
struct SessionPoolState<S> {
    sessions: Vec<Arc<S>>,
}

impl<S> Default for SessionPoolState<S> {
    fn default() -> Self {
        Self {
            sessions: Vec::new(),
        }
    }
}

trait PoolSession: Send + Sync + 'static {
    fn is_closed(&self) -> bool;
    fn inflight(&self) -> usize;
    fn last_used(&self) -> Instant;
    fn touch(&self);
}

#[derive(Debug)]
struct SessionPool<S> {
    config: ConnectionPoolConfig,
    state: Mutex<SessionPoolState<S>>,
    notify: Notify,
    #[cfg(test)]
    connect_count: AtomicUsize,
}

impl<S> Default for SessionPool<S> {
    fn default() -> Self {
        Self {
            config: ConnectionPoolConfig::default(),
            state: Mutex::new(SessionPoolState::default()),
            notify: Notify::new(),
            #[cfg(test)]
            connect_count: AtomicUsize::new(0),
        }
    }
}

impl<S: PoolSession> SessionPool<S> {
    #[cfg(all(test, target_os = "linux"))]
    fn with_config(config: ConnectionPoolConfig) -> Self {
        Self {
            config,
            state: Mutex::new(SessionPoolState::default()),
            notify: Notify::new(),
            #[cfg(test)]
            connect_count: AtomicUsize::new(0),
        }
    }

    fn config(&self) -> ConnectionPoolConfig {
        self.config
    }

    fn prune_idle_locked(state: &mut SessionPoolState<S>, config: ConnectionPoolConfig) {
        let now = Instant::now();
        let mut retained = Vec::with_capacity(state.sessions.len());
        let mut survivors = 0usize;
        for session in state.sessions.drain(..) {
            let expired = !session.is_closed()
                && session.inflight() == 0
                && now.duration_since(session.last_used()) > config.idle_ttl;
            if session.is_closed() || (expired && survivors >= config.min_idle) {
                continue;
            }
            survivors += 1;
            retained.push(session);
        }
        state.sessions = retained;
    }

    async fn acquire<F, Fut>(self: &Arc<Self>, connect: F) -> io::Result<Arc<S>>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = io::Result<Arc<S>>>,
    {
        loop {
            let config = self.config();
            let mut state = self.state.lock().await;
            Self::prune_idle_locked(&mut state, config);

            if let Some(session) = state
                .sessions
                .iter()
                .filter(|session| !session.is_closed())
                .min_by_key(|session| session.inflight())
                .cloned()
            {
                let should_create = state.sessions.len() < config.max_size
                    && session.inflight() >= SESSION_MAX_INFLIGHT;
                if !should_create {
                    session.touch();
                    return Ok(session);
                }
            }

            if state.sessions.len() < config.max_size {
                drop(state);
                match connect().await {
                    Ok(session) => {
                        #[cfg(test)]
                        self.connect_count.fetch_add(1, Ordering::Relaxed);
                        let mut state = self.state.lock().await;
                        state.sessions.push(Arc::clone(&session));
                        drop(state);
                        return Ok(session);
                    }
                    Err(err) => return Err(err),
                }
            }

            let notified = self.notify.notified();
            drop(state);
            notified.await;
        }
    }

    async fn prune(&self) {
        let mut state = self.state.lock().await;
        Self::prune_idle_locked(&mut state, self.config());
        drop(state);
        self.notify.notify_one();
    }

    #[cfg(all(test, target_os = "linux"))]
    fn connect_count(&self) -> usize {
        self.connect_count.load(Ordering::Relaxed)
    }

    #[cfg(all(test, target_os = "linux"))]
    async fn state_counts(&self) -> (usize, usize) {
        let state = self.state.lock().await;
        let idle = state
            .sessions
            .iter()
            .filter(|session| session.inflight() == 0 && !session.is_closed())
            .count();
        (state.sessions.len(), idle)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct WireResponse {
    request_id: u64,
    status: u8,
    payload: Vec<u8>,
}

#[derive(Debug)]
enum ResponseAction {
    Immediate(WireResponse),
    GetChunk { request_id: u64, checksum: CheckSum },
}

type BoxAsyncReader = Box<dyn AsyncRead + Unpin + Send>;
type BoxAsyncWriter = Box<dyn AsyncWrite + Unpin + Send>;
type PendingResponse = oneshot::Sender<io::Result<WireResponse>>;
type PendingMap = Arc<Mutex<HashMap<u64, PendingResponse>>>;

enum ResponseWriter {
    Tcp(tokio::net::tcp::OwnedWriteHalf),
    Unix(tokio::net::unix::OwnedWriteHalf),
}

trait RequestStream: Send + 'static {
    fn into_request_parts(self) -> (BoxAsyncReader, ResponseWriter);
}

impl RequestStream for TcpStream {
    fn into_request_parts(self) -> (BoxAsyncReader, ResponseWriter) {
        let (reader, writer) = self.into_split();
        (Box::new(reader), ResponseWriter::Tcp(writer))
    }
}

impl RequestStream for UnixStream {
    fn into_request_parts(self) -> (BoxAsyncReader, ResponseWriter) {
        let (reader, writer) = self.into_split();
        (Box::new(reader), ResponseWriter::Unix(writer))
    }
}

struct MultiplexedSession {
    writer: Mutex<BoxAsyncWriter>,
    pending: PendingMap,
    next_request_id: AtomicU64,
    closed: AtomicBool,
    inflight: AtomicUsize,
    last_used_micros: AtomicU64,
}

#[derive(Debug)]
struct InboundRequest {
    request: Request,
    checksums: Option<Vec<CheckSum>>,
}

type TcpConnPool = SessionPool<MultiplexedSession>;
type UnixConnPool = SessionPool<MultiplexedSession>;
type PeerPoolMap = Arc<Mutex<HashMap<SocketAddr, Arc<TcpConnPool>>>>;

impl PoolSession for MultiplexedSession {
    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    fn inflight(&self) -> usize {
        self.inflight.load(Ordering::Relaxed)
    }

    fn last_used(&self) -> Instant {
        instant_from_micros(self.last_used_micros.load(Ordering::Relaxed))
    }

    fn touch(&self) {
        self.last_used_micros
            .store(monotonic_now_micros(), Ordering::Relaxed);
    }
}

impl std::fmt::Debug for MultiplexedSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiplexedSession")
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .field("inflight", &self.inflight.load(Ordering::Relaxed))
            .finish()
    }
}

impl MultiplexedSession {
    fn new<R>(reader: R, writer: BoxAsyncWriter) -> Arc<Self>
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        let session = Arc::new(Self {
            writer: Mutex::new(writer),
            pending: Arc::new(Mutex::new(HashMap::new())),
            next_request_id: AtomicU64::new(1),
            closed: AtomicBool::new(false),
            inflight: AtomicUsize::new(0),
            last_used_micros: AtomicU64::new(monotonic_now_micros()),
        });
        Self::spawn_reader(Arc::clone(&session), reader);
        session
    }

    fn from_tcp(stream: TcpStream) -> Arc<Self> {
        let (reader, writer) = stream.into_split();
        Self::new(reader, Box::new(writer))
    }

    fn from_unix(stream: UnixStream) -> Arc<Self> {
        let (reader, writer) = stream.into_split();
        Self::new(reader, Box::new(writer))
    }

    fn spawn_reader<R>(session: Arc<Self>, mut reader: R)
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        tokio::spawn(async move {
            loop {
                match WireResponse::read_from(&mut reader).await {
                    Ok(response) => {
                        session.touch();
                        let tx = session.pending.lock().await.remove(&response.request_id);
                        if let Some(tx) = tx {
                            let _ = tx.send(Ok(response));
                        } else {
                            session
                                .close(io::Error::new(
                                    ErrorKind::InvalidData,
                                    format!("unknown response request_id={}", response.request_id),
                                ))
                                .await;
                            return;
                        }
                    }
                    Err(err) => {
                        session.close(err).await;
                        return;
                    }
                }
            }
        });
    }

    async fn send_request(
        &self,
        mut request: Request,
        req_timeout: Duration,
    ) -> io::Result<WireResponse> {
        self.send_request_with_payload(&mut request, &[], req_timeout)
            .await
    }

    async fn send_batch_request(
        &self,
        mut request: Request,
        payload: Vec<u8>,
        req_timeout: Duration,
    ) -> io::Result<WireResponse> {
        self.send_request_with_payload(&mut request, &payload, req_timeout)
            .await
    }

    async fn send_request_with_payload(
        &self,
        request: &mut Request,
        payload: &[u8],
        req_timeout: Duration,
    ) -> io::Result<WireResponse> {
        if self.is_closed() {
            return Err(io::Error::new(ErrorKind::BrokenPipe, "session is closed"));
        }

        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        request.request_id = request_id;
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(request_id, tx);
        self.inflight.fetch_add(1, Ordering::Relaxed);
        self.touch();

        let write_res = {
            let mut writer = self.writer.lock().await;
            timeout(req_timeout, async {
                request.write_to(&mut *writer).await?;
                if !payload.is_empty() {
                    writer.write_all(payload).await?;
                }
                writer.flush().await
            })
            .await
        };
        match write_res {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                self.pending.lock().await.remove(&request_id);
                self.inflight.fetch_sub(1, Ordering::Relaxed);
                self.close(io::Error::new(err.kind(), err.to_string()))
                    .await;
                return Err(err);
            }
            Err(_) => {
                self.pending.lock().await.remove(&request_id);
                self.inflight.fetch_sub(1, Ordering::Relaxed);
                let err = timeout_error("request write");
                self.close(io::Error::new(err.kind(), err.to_string()))
                    .await;
                return Err(err);
            }
        }

        let result = timeout(req_timeout, rx).await;
        self.inflight.fetch_sub(1, Ordering::Relaxed);
        self.touch();
        match result {
            Ok(Ok(Ok(response))) => Ok(response),
            Ok(Ok(Err(err))) => Err(err),
            Ok(Err(_)) => Err(io::Error::new(
                ErrorKind::BrokenPipe,
                "response channel closed",
            )),
            Err(_) => {
                self.pending.lock().await.remove(&request_id);
                let err = timeout_error("response read");
                self.close(io::Error::new(err.kind(), err.to_string()))
                    .await;
                Err(err)
            }
        }
    }

    async fn close(&self, err: io::Error) {
        if self.closed.swap(true, Ordering::Relaxed) {
            return;
        }
        let kind = err.kind();
        let message = err.to_string();
        let pending = std::mem::take(&mut *self.pending.lock().await);
        for (_, tx) in pending {
            let _ = tx.send(Err(io::Error::new(kind, message.clone())));
        }
    }
}

#[derive(Debug, Clone)]
struct ChunkServerMetrics {
    requests_total: Counter<u64>,
    request_duration: Histogram<f64>,
    response_bytes: Counter<u64>,
    active_connections: UpDownCounter<i64>,
}

impl ChunkServerMetrics {
    fn new(meter: &Meter) -> Self {
        Self {
            requests_total: meter
                .u64_counter("distill_fs.server.requests_total")
                .with_description("Total requests received")
                .init(),
            request_duration: meter
                .f64_histogram("distill_fs.server.request_duration_ms")
                .with_description("Request processing duration in ms")
                .with_unit("ms")
                .init(),
            response_bytes: meter
                .u64_counter("distill_fs.server.response_bytes")
                .with_description("Total bytes sent in GET_CHUNK responses")
                .with_unit("By")
                .init(),
            active_connections: meter
                .i64_up_down_counter("distill_fs.server.active_connections")
                .with_description("Current active connections")
                .init(),
        }
    }

    fn record_connection_delta(&self, transport: &'static str, delta: i64) {
        self.active_connections
            .add(delta, &[KeyValue::new("transport", transport)]);
    }

    fn record_request(
        &self,
        request_type: &'static str,
        status: &'static str,
        transport: &'static str,
        elapsed_ms: f64,
        payload_len: usize,
    ) {
        let attrs = [
            KeyValue::new("type", request_type),
            KeyValue::new("status", status),
            KeyValue::new("transport", transport),
        ];
        self.requests_total.add(1, &attrs);
        self.request_duration.record(elapsed_ms, &attrs);
        if payload_len > 0 {
            self.response_bytes.add(payload_len as u64, &attrs);
        }
    }
}

#[derive(Debug, Clone)]
struct PeerClientMetrics {
    fetch_total: Counter<u64>,
    fetch_duration: Histogram<f64>,
    query_total: Counter<u64>,
    query_duration: Histogram<f64>,
    retry_total: Counter<u64>,
}

impl PeerClientMetrics {
    fn new(meter: &Meter) -> Self {
        Self {
            fetch_total: meter
                .u64_counter("distill_fs.peer.fetch_total")
                .with_description("Total chunk fetch attempts by source")
                .init(),
            fetch_duration: meter
                .f64_histogram("distill_fs.peer.fetch_duration_ms")
                .with_description("End-to-end fetch duration including retries")
                .with_unit("ms")
                .init(),
            query_total: meter
                .u64_counter("distill_fs.peer.query_total")
                .with_description("Total individual peer queries")
                .init(),
            query_duration: meter
                .f64_histogram("distill_fs.peer.query_duration_ms")
                .with_description("Single peer TCP query duration")
                .with_unit("ms")
                .init(),
            retry_total: meter
                .u64_counter("distill_fs.peer.retry_total")
                .with_description("Total retries in index mode")
                .init(),
        }
    }

    fn record_fetch(&self, source: &'static str, result: &'static str, elapsed_ms: f64) {
        let attrs = [
            KeyValue::new("source", source),
            KeyValue::new("result", result),
        ];
        self.fetch_total.add(1, &attrs);
        self.fetch_duration.record(elapsed_ms, &attrs);
    }

    fn record_query(&self, result: &'static str, elapsed_ms: f64) {
        let attrs = [KeyValue::new("result", result)];
        self.query_total.add(1, &attrs);
        self.query_duration.record(elapsed_ms, &attrs);
    }

    fn record_retry(&self, source: &'static str) {
        self.retry_total.add(1, &[KeyValue::new("source", source)]);
    }
}

#[derive(Debug, Clone)]
struct ChunkIndexMetrics {
    lookup_total: Counter<u64>,
    lookup_duration: Histogram<f64>,
    register_total: Counter<u64>,
    register_success_total: Counter<u64>,
    unregister_total: Counter<u64>,
    refresh_total: Counter<u64>,
    repair_total: Counter<u64>,
    error_total: Counter<u64>,
    candidates_count: Histogram<f64>,
}

impl ChunkIndexMetrics {
    fn new(meter: &Meter) -> Self {
        Self {
            lookup_total: meter
                .u64_counter("distill_fs.index.lookup_total")
                .with_description("Total index lookups")
                .init(),
            lookup_duration: meter
                .f64_histogram("distill_fs.index.lookup_duration_ms")
                .with_description("Index lookup duration (Redis RTT)")
                .with_unit("ms")
                .init(),
            register_total: meter
                .u64_counter("distill_fs.index.register_total")
                .with_description("Total attempted index registrations")
                .init(),
            register_success_total: meter
                .u64_counter("distill_fs.index.register_success_total")
                .with_description("Total successful index registrations")
                .init(),
            unregister_total: meter
                .u64_counter("distill_fs.index.unregister_total")
                .with_description("Total successful index unregisters")
                .init(),
            refresh_total: meter
                .u64_counter("distill_fs.index.refresh_total")
                .with_description("Total chunks refreshed in the index")
                .init(),
            repair_total: meter
                .u64_counter("distill_fs.index.repair_total")
                .with_description("Total chunks repaired in the index")
                .init(),
            error_total: meter
                .u64_counter("distill_fs.index.error_total")
                .with_description("Total index operation errors")
                .init(),
            candidates_count: meter
                .f64_histogram("distill_fs.index.candidates_count")
                .with_description("Number of candidate nodes per lookup")
                .init(),
        }
    }

    fn record_lookup(&self, result: &'static str, elapsed_ms: f64, candidates: usize) {
        let attrs = [KeyValue::new("result", result)];
        self.lookup_total.add(1, &attrs);
        self.lookup_duration.record(elapsed_ms, &attrs);
        self.candidates_count.record(candidates as f64, &attrs);
    }

    fn record_register_attempt(&self, mode: &'static str, count: u64) {
        self.register_total
            .add(count, &[KeyValue::new("mode", mode)]);
    }

    fn record_register_success(&self, mode: &'static str, count: u64) {
        if count > 0 {
            self.register_success_total
                .add(count, &[KeyValue::new("mode", mode)]);
        }
    }

    fn record_unregister(&self, mode: &'static str, count: u64) {
        if count > 0 {
            self.unregister_total
                .add(count, &[KeyValue::new("mode", mode)]);
        }
    }

    fn record_refresh(&self, result: &'static str, count: u64) {
        if count > 0 {
            self.refresh_total
                .add(count, &[KeyValue::new("result", result)]);
        }
    }

    fn record_repair(&self, result: &'static str, count: u64) {
        if count > 0 {
            self.repair_total
                .add(count, &[KeyValue::new("result", result)]);
        }
    }

    fn record_error(&self, op: &'static str) {
        self.error_total.add(1, &[KeyValue::new("op", op)]);
    }
}

#[derive(Debug, Clone)]
struct LocalClientMetrics {
    requests_total: Counter<u64>,
    request_duration: Histogram<f64>,
}

impl LocalClientMetrics {
    fn new(meter: &Meter) -> Self {
        Self {
            requests_total: meter
                .u64_counter("distill_fs.local.request_total")
                .with_description("Total local control requests")
                .init(),
            request_duration: meter
                .f64_histogram("distill_fs.local.request_duration_ms")
                .with_description("Local control request duration")
                .with_unit("ms")
                .init(),
        }
    }

    fn record_request(&self, op: &'static str, result: &'static str, elapsed_ms: f64) {
        let attrs = [KeyValue::new("op", op), KeyValue::new("result", result)];
        self.requests_total.add(1, &attrs);
        self.request_duration.record(elapsed_ms, &attrs);
    }
}

fn timeout_error(op: &str) -> io::Error {
    io::Error::new(ErrorKind::TimedOut, format!("{op} timed out"))
}

#[derive(Clone, Debug)]
pub struct PeerRuntime {
    runtime: Arc<Runtime>,
}

impl PeerRuntime {
    pub fn new() -> anyhow::Result<Self> {
        Self::new_with_worker_threads(8)
    }

    pub fn new_with_worker_threads(worker_threads: usize) -> anyhow::Result<Self> {
        let runtime = Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .max_blocking_threads(16)
            .thread_keep_alive(Duration::from_secs(300))
            .enable_io()
            .enable_time()
            .build()
            .context("failed to build tokio runtime")?;
        Ok(Self {
            runtime: Arc::new(runtime),
        })
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }

    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.runtime.spawn(future)
    }
}

#[derive(Debug, Clone)]
pub struct ShutdownHandle {
    state: Arc<ShutdownState>,
}

#[derive(Debug)]
struct ShutdownState {
    stopped: AtomicBool,
    notify: Notify,
}

impl ShutdownHandle {
    fn new() -> Self {
        Self {
            state: Arc::new(ShutdownState {
                stopped: AtomicBool::new(false),
                notify: Notify::new(),
            }),
        }
    }

    pub fn shutdown(&self) {
        self.state.stopped.store(true, Ordering::Relaxed);
        self.state.notify.notify_waiters();
    }

    fn is_shutdown(&self) -> bool {
        self.state.stopped.load(Ordering::Relaxed)
    }

    async fn wait(&self) {
        if self.is_shutdown() {
            return;
        }
        self.state.notify.notified().await;
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum MessageType {
    GetChunk = 0x01,
    PrefetchChunk = 0x02,
    RegisterChunk = 0x03,
    UnregisterChunk = 0x04,
    RegisterChunks = 0x05,
    UnregisterChunks = 0x06,
    HealthCheck = 0x07,
}

impl TryFrom<u8> for MessageType {
    type Error = io::Error;

    fn try_from(value: u8) -> io::Result<Self> {
        match value {
            0x01 => Ok(Self::GetChunk),
            0x02 => Ok(Self::PrefetchChunk),
            0x03 => Ok(Self::RegisterChunk),
            0x04 => Ok(Self::UnregisterChunk),
            0x05 => Ok(Self::RegisterChunks),
            0x06 => Ok(Self::UnregisterChunks),
            0x07 => Ok(Self::HealthCheck),
            _ => Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("invalid message type: {value}"),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Request {
    pub request_id: u64,
    pub message_type: MessageType,
    pub checksum: CheckSum,
    pub offset: u32,
    pub length: u32,
}

impl Request {
    pub fn new(
        request_id: u64,
        message_type: MessageType,
        checksum: CheckSum,
        offset: u32,
        length: u32,
    ) -> Self {
        Self {
            request_id,
            message_type,
            checksum,
            offset,
            length,
        }
    }

    pub fn whole_chunk(message_type: MessageType, checksum: CheckSum) -> Self {
        Self::new(0, message_type, checksum, 0, 0)
    }

    pub fn control_batch(message_type: MessageType, count: usize) -> Self {
        Self::new(0, message_type, CheckSum::empty(), 0, count as u32)
    }

    fn ensure_full_chunk(&self) -> io::Result<()> {
        if self.offset == 0 && self.length == 0 {
            return Ok(());
        }
        Err(io::Error::new(
            ErrorKind::InvalidInput,
            format!(
                "chunk peer protocol only supports full-chunk requests, got offset={} length={}",
                self.offset, self.length
            ),
        ))
    }

    fn ensure_control_batch(&self) -> io::Result<usize> {
        if self.offset != 0 {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!("invalid batch control request offset={}", self.offset),
            ));
        }
        Ok(self.length as usize)
    }

    fn encode(&self) -> [u8; REQUEST_LEN] {
        let mut buf = [0_u8; REQUEST_LEN];
        buf[0..8].copy_from_slice(&self.request_id.to_be_bytes());
        buf[8] = self.message_type as u8;
        buf[9] = self.checksum.method.into();
        buf[10..42].copy_from_slice(&self.checksum.raw);
        buf[42..46].copy_from_slice(&self.offset.to_be_bytes());
        buf[46..50].copy_from_slice(&self.length.to_be_bytes());
        buf
    }

    fn decode(buf: [u8; REQUEST_LEN]) -> io::Result<Self> {
        let request_id = u64::from_be_bytes(buf[0..8].try_into().unwrap());
        let message_type = MessageType::try_from(buf[8])?;
        let checksum = CheckSum::new(&buf[10..42], buf[9].into())?;
        let offset = u32::from_be_bytes(buf[42..46].try_into().unwrap());
        let length = u32::from_be_bytes(buf[46..50].try_into().unwrap());
        Ok(Self {
            request_id,
            message_type,
            checksum,
            offset,
            length,
        })
    }

    #[allow(dead_code)]
    pub async fn read_from<R>(reader: &mut R) -> io::Result<Self>
    where
        R: AsyncRead + Unpin,
    {
        let mut buf = [0_u8; REQUEST_LEN];
        reader.read_exact(&mut buf).await?;
        Self::decode(buf)
    }

    #[allow(dead_code)]
    pub async fn write_to<W>(&self, writer: &mut W) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        writer.write_all(&self.encode()).await
    }

    #[cfg(all(test, target_os = "linux"))]
    pub fn write_to_sync<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.encode())
    }
}

impl WireResponse {
    async fn read_from<R>(reader: &mut R) -> io::Result<Self>
    where
        R: AsyncRead + Unpin,
    {
        let mut header = [0_u8; RESPONSE_HEADER_LEN];
        reader.read_exact(&mut header).await?;
        let request_id = u64::from_be_bytes(header[0..8].try_into().unwrap());
        let status = header[8];
        let payload_len = u32::from_be_bytes(header[9..13].try_into().unwrap()) as usize;
        if payload_len > MAX_CHUNK_PAYLOAD_SIZE {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("payload too large: {payload_len}"),
            ));
        }
        let mut payload = vec![0_u8; payload_len];
        reader.read_exact(&mut payload).await?;
        Ok(Self {
            request_id,
            status,
            payload,
        })
    }

    #[cfg(test)]
    #[allow(dead_code)]
    fn read_from_sync<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut header = [0_u8; RESPONSE_HEADER_LEN];
        reader.read_exact(&mut header)?;
        let request_id = u64::from_be_bytes(header[0..8].try_into().unwrap());
        let status = header[8];
        let payload_len = u32::from_be_bytes(header[9..13].try_into().unwrap()) as usize;
        if payload_len > MAX_CHUNK_PAYLOAD_SIZE {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("payload too large: {payload_len}"),
            ));
        }
        let mut payload = vec![0_u8; payload_len];
        reader.read_exact(&mut payload)?;
        Ok(Self {
            request_id,
            status,
            payload,
        })
    }

    #[allow(dead_code)]
    async fn write_to<W>(&self, writer: &mut W) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        let header = encode_response_header(self.request_id, self.status, self.payload.len());
        writer.write_all(&header).await?;
        writer.write_all(&self.payload).await
    }
}

fn encode_response_header(
    request_id: u64,
    status: u8,
    payload_len: usize,
) -> [u8; RESPONSE_HEADER_LEN] {
    let mut header = [0_u8; RESPONSE_HEADER_LEN];
    header[0..8].copy_from_slice(&request_id.to_be_bytes());
    header[8] = status;
    header[9..13].copy_from_slice(&(payload_len as u32).to_be_bytes());
    header
}

struct VectoredWriteCursor<'a> {
    header: &'a [u8],
    payload: &'a [u8],
    written: usize,
}

impl<'a> VectoredWriteCursor<'a> {
    fn new(header: &'a [u8], payload: &'a [u8]) -> Self {
        Self {
            header,
            payload,
            written: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.written >= self.header.len() + self.payload.len()
    }

    fn fill_io_slices<'s>(&self, scratch: &'s mut [io::IoSlice<'a>; 2]) -> &'s [io::IoSlice<'a>] {
        let mut count = 0;
        let header_len = self.header.len();
        if self.written < header_len {
            scratch[count] = io::IoSlice::new(&self.header[self.written..]);
            count += 1;
            scratch[count] = io::IoSlice::new(self.payload);
            count += 1;
            return &scratch[..count];
        }

        let payload_offset = self.written - header_len;
        if payload_offset < self.payload.len() {
            scratch[count] = io::IoSlice::new(&self.payload[payload_offset..]);
            count += 1;
        }
        &scratch[..count]
    }

    fn advance(&mut self, written: usize) {
        self.written = (self.written + written).min(self.header.len() + self.payload.len());
    }
}

impl ResponseWriter {
    async fn write_response(&mut self, response: &WireResponse) -> io::Result<()> {
        self.write_response_parts(response.request_id, response.status, &response.payload)
            .await
    }

    async fn write_get_chunk_hit(&mut self, request_id: u64, payload: &[u8]) -> io::Result<()> {
        self.write_response_parts(request_id, STATUS_HIT, payload)
            .await
    }

    async fn write_response_parts(
        &mut self,
        request_id: u64,
        status: u8,
        payload: &[u8],
    ) -> io::Result<()> {
        let header = encode_response_header(request_id, status, payload.len());
        let mut cursor = VectoredWriteCursor::new(&header, payload);
        let mut scratch = [io::IoSlice::new(&[]), io::IoSlice::new(&[])];
        while !cursor.is_empty() {
            self.writable().await?;
            let slices = cursor.fill_io_slices(&mut scratch);
            match self.try_write_vectored(slices) {
                Ok(0) => {
                    return Err(io::Error::new(
                        ErrorKind::WriteZero,
                        "failed to write response to stream",
                    ));
                }
                Ok(written) => cursor.advance(written),
                Err(err) if err.kind() == ErrorKind::WouldBlock => {}
                Err(err) => return Err(err),
            }
        }
        self.flush().await
    }

    async fn writable(&self) -> io::Result<()> {
        match self {
            Self::Tcp(writer) => writer.writable().await,
            Self::Unix(writer) => writer.writable().await,
        }
    }

    fn try_write_vectored(&self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        match self {
            Self::Tcp(writer) => writer.try_write_vectored(bufs),
            Self::Unix(writer) => writer.try_write_vectored(bufs),
        }
    }

    async fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Tcp(writer) => writer.flush().await,
            Self::Unix(writer) => writer.flush().await,
        }
    }
}

pub trait PeerDiscovery: Send + Sync {
    fn get_peers(&self) -> Vec<SocketAddr>;
    fn shutdown(&self) {}
}

#[async_trait]
pub trait ChunkIndex: Send + Sync {
    async fn lookup_owners(&self, cs: &CheckSum) -> anyhow::Result<Vec<SocketAddr>>;
    async fn register(&self, cs: &CheckSum) -> anyhow::Result<()>;
    async fn register_batch(&self, checksums: &[CheckSum]) -> anyhow::Result<()>;
    async fn unregister(&self, cs: &CheckSum) -> anyhow::Result<()>;
    fn refresh_interval(&self) -> Option<Duration> {
        None
    }
    async fn unregister_batch(&self, checksums: &[CheckSum]) -> anyhow::Result<()> {
        for checksum in checksums {
            self.unregister(checksum).await?;
        }
        Ok(())
    }
    async fn sync_existing_chunks(&self, checksums: &[CheckSum]) -> anyhow::Result<usize> {
        self.register_batch(checksums).await?;
        Ok(checksums.len())
    }
    async fn refresh_registered(&self, _spread_over: Duration) -> anyhow::Result<Option<usize>> {
        Ok(None)
    }
    async fn repair_missing_owners(
        &self,
        _checksums: &[CheckSum],
    ) -> anyhow::Result<Option<usize>> {
        Ok(None)
    }
}

#[derive(Debug)]
pub struct StaticPeers {
    peers: Vec<SocketAddr>,
}

impl StaticPeers {
    pub fn new(peers: Vec<SocketAddr>) -> Self {
        Self { peers }
    }
}

impl PeerDiscovery for StaticPeers {
    fn get_peers(&self) -> Vec<SocketAddr> {
        self.peers.clone()
    }
}

pub struct RedisDiscovery {
    peers: Arc<RwLock<Vec<SocketAddr>>>,
    shutdown: ShutdownHandle,
    _worker: JoinHandle<()>,
}

impl std::fmt::Debug for RedisDiscovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisDiscovery").finish()
    }
}

impl RedisDiscovery {
    pub fn new(
        runtime: PeerRuntime,
        url: &str,
        advertise_addr: SocketAddr,
        node_id: &str,
    ) -> anyhow::Result<Self> {
        let peers = Arc::new(RwLock::new(Vec::new()));
        let shutdown = ShutdownHandle::new();
        let discovery_worker = RedisDiscoveryWorker::new(
            url,
            advertise_addr,
            node_id,
            Arc::clone(&peers),
            shutdown.clone(),
        )?;
        let worker = runtime.spawn(discovery_worker.run());
        Ok(Self {
            peers,
            shutdown,
            _worker: worker,
        })
    }
}

impl PeerDiscovery for RedisDiscovery {
    fn get_peers(&self) -> Vec<SocketAddr> {
        self.peers.read().unwrap().clone()
    }

    fn shutdown(&self) {
        self.shutdown.shutdown();
    }
}

fn chunk_index_key(cs: &CheckSum) -> String {
    format!("distill-fs:chunk-owner:{cs}")
}

const REDIS_CHUNK_REGISTER_SCRIPT: &str = r#"
local cutoff = tonumber(ARGV[2]) - tonumber(ARGV[3])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', cutoff)
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
local count = redis.call('ZCARD', KEYS[1])
local max_owners = tonumber(ARGV[4])
if count > max_owners then
    redis.call('ZREMRANGEBYRANK', KEYS[1], 0, count - max_owners - 1)
end
redis.call('EXPIRE', KEYS[1], ARGV[3])
if redis.call('ZSCORE', KEYS[1], ARGV[1]) then
    return 1
end
return 0
"#;

#[derive(Debug, Default)]
struct IndexTracker {
    registered: RwLock<HashSet<CheckSumOnDisk>>,
}

impl IndexTracker {
    fn insert_many(&self, checksums: impl IntoIterator<Item = CheckSumOnDisk>) {
        let mut registered = self.registered.write().unwrap();
        registered.extend(checksums);
    }

    fn remove_many(&self, checksums: impl IntoIterator<Item = CheckSumOnDisk>) {
        let mut registered = self.registered.write().unwrap();
        for checksum in checksums {
            registered.remove(&checksum);
        }
    }

    fn contains(&self, checksum: &CheckSum) -> bool {
        self.registered
            .read()
            .unwrap()
            .contains(&CheckSumOnDisk::from(*checksum))
    }

    fn len(&self) -> usize {
        self.registered.read().unwrap().len()
    }

    fn snapshot(&self) -> Vec<CheckSum> {
        let snapshot = self
            .registered
            .read()
            .unwrap()
            .iter()
            .copied()
            .collect::<Vec<_>>();
        snapshot.into_iter().map(CheckSum::from).collect()
    }
}

pub struct RedisChunkIndex {
    client: redis::Client,
    connection: Mutex<Option<MultiplexedConnection>>,
    register_script_loaded: AtomicBool,
    advertise_addr: SocketAddr,
    node_id: String,
    ttl_secs: u64,
    tracker: IndexTracker,
    register_script: redis::Script,
    metrics: ChunkIndexMetrics,
}

impl std::fmt::Debug for RedisChunkIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisChunkIndex")
            .field("advertise_addr", &self.advertise_addr)
            .field("node_id", &self.node_id)
            .field("ttl_secs", &self.ttl_secs)
            .finish()
    }
}

impl RedisChunkIndex {
    pub fn new(
        url: &str,
        advertise_addr: SocketAddr,
        node_id: &str,
        ttl_secs: u64,
    ) -> anyhow::Result<Self> {
        let meter = global::meter("distill_fs.chunk_index");
        let client = redis::Client::open(url).context("failed to create redis client")?;
        Ok(Self {
            client,
            connection: Mutex::new(None),
            register_script_loaded: AtomicBool::new(false),
            advertise_addr,
            node_id: node_id.to_string(),
            ttl_secs,
            tracker: IndexTracker::default(),
            register_script: redis::Script::new(REDIS_CHUNK_REGISTER_SCRIPT),
            metrics: ChunkIndexMetrics::new(&meter),
        })
    }

    async fn redis_conn(&self) -> anyhow::Result<MultiplexedConnection> {
        let mut guard = self.connection.lock().await;
        if let Some(conn) = guard.as_ref() {
            return Ok(conn.clone());
        }
        let conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .context("failed to connect to redis")?;
        self.register_script_loaded.store(false, Ordering::Relaxed);
        *guard = Some(conn.clone());
        Ok(conn)
    }

    async fn load_register_script(&self, conn: &mut MultiplexedConnection) -> anyhow::Result<()> {
        if self.register_script_loaded.load(Ordering::Relaxed) {
            return Ok(());
        }
        self.register_script
            .prepare_invoke()
            .load_async(conn)
            .await
            .context("failed to load redis chunk register script")?;
        self.register_script_loaded.store(true, Ordering::Relaxed);
        Ok(())
    }

    fn live_owner_cutoff(&self) -> i64 {
        now_epoch_secs().saturating_sub(self.ttl_secs) as i64
    }

    async fn lookup_owner_values(
        &self,
        checksums: &[CheckSum],
    ) -> anyhow::Result<Vec<Vec<String>>> {
        if checksums.is_empty() {
            return Ok(Vec::new());
        }
        let mut conn = self.redis_conn().await?;
        let cutoff = self.live_owner_cutoff();
        let mut owners = Vec::with_capacity(checksums.len());
        for batch in checksums.chunks(REDIS_REGISTER_BATCH_SIZE) {
            let mut pipe = redis::pipe();
            for checksum in batch {
                pipe.cmd("ZRANGEBYSCORE")
                    .arg(chunk_index_key(checksum))
                    .arg(cutoff)
                    .arg("+inf");
            }
            let values: Vec<redis::Value> = pipe.query_async(&mut conn).await?;
            for value in values {
                owners.push(redis::from_redis_value(&value)?);
            }
        }
        Ok(owners)
    }

    async fn resolve_owners(&self, cs: &CheckSum) -> anyhow::Result<Vec<SocketAddr>> {
        let begin = Instant::now();
        let result = async {
            let values = self
                .lookup_owner_values(std::slice::from_ref(cs))
                .await?
                .into_iter()
                .next()
                .unwrap_or_default();
            values
                .into_iter()
                .map(|value| value.parse::<SocketAddr>())
                .collect::<Result<Vec<_>, _>>()
                .map_err(anyhow::Error::from)
        }
        .await;
        let elapsed_ms = begin.elapsed().as_secs_f64() * 1000.0;
        match &result {
            Ok(owners) => {
                let status = if owners.is_empty() { "miss" } else { "hit" };
                self.metrics.record_lookup(status, elapsed_ms, owners.len());
            }
            Err(_) => {
                self.metrics.record_lookup("error", elapsed_ms, 0);
                self.metrics.record_error("lookup");
            }
        }
        result
    }

    async fn register_many(&self, checksums: &[CheckSum]) -> anyhow::Result<usize> {
        if checksums.is_empty() {
            return Ok(0);
        }
        let mode = if checksums.len() == 1 {
            "single"
        } else {
            "batch"
        };
        self.metrics
            .record_register_attempt(mode, checksums.len() as u64);
        let mut conn = match self.redis_conn().await {
            Ok(conn) => conn,
            Err(err) => {
                self.metrics.record_error("register");
                return Err(err);
            }
        };
        if let Err(err) = self.load_register_script(&mut conn).await {
            self.metrics.record_error("register");
            return Err(err);
        }
        let advertise_addr = self.advertise_addr.to_string();
        let now = now_epoch_secs() as i64;
        let ttl_secs = self.ttl_secs as i64;
        let mut retained = Vec::new();
        for batch in checksums.chunks(REDIS_REGISTER_BATCH_SIZE) {
            let mut pipe = redis::pipe();
            for checksum in batch {
                let key = chunk_index_key(checksum);
                let mut invocation = self.register_script.prepare_invoke();
                invocation
                    .key(&key)
                    .arg(&advertise_addr)
                    .arg(now)
                    .arg(ttl_secs)
                    .arg(MAX_CHUNK_OWNERS as i64);
                pipe.invoke_script(&invocation);
            }
            let results: Vec<i32> = match pipe.query_async(&mut conn).await {
                Ok(results) => results,
                Err(err) => {
                    self.metrics.record_error("register");
                    return Err(err.into());
                }
            };
            retained.extend(batch.iter().zip(results.into_iter()).filter_map(
                |(checksum, result)| (result == 1).then_some(CheckSumOnDisk::from(*checksum)),
            ));
        }
        let registered = retained.len();
        let dropped = checksums.len().saturating_sub(registered);
        self.tracker.insert_many(retained);
        if dropped > 0 {
            let tracker_size = self.tracker.len();
            if dropped * 4 >= checksums.len() {
                warn!(
                    dropped,
                    attempted = checksums.len(),
                    tracker_size,
                    advertise_addr = %self.advertise_addr,
                    "chunk index registration results were evicted before retention"
                );
            } else {
                debug!(
                    dropped,
                    attempted = checksums.len(),
                    tracker_size,
                    advertise_addr = %self.advertise_addr,
                    "chunk index registration results were evicted before retention"
                );
            }
        }
        self.metrics
            .record_register_success(mode, registered as u64);
        Ok(registered)
    }

    async fn unregister_many(&self, checksums: &[CheckSum]) -> anyhow::Result<()> {
        if checksums.is_empty() {
            return Ok(());
        }
        let mode = if checksums.len() == 1 {
            "single"
        } else {
            "batch"
        };
        let mut conn = match self.redis_conn().await {
            Ok(conn) => conn,
            Err(err) => {
                self.metrics.record_error("unregister");
                return Err(err);
            }
        };
        let advertise_addr = self.advertise_addr.to_string();
        for batch in checksums.chunks(REDIS_REGISTER_BATCH_SIZE) {
            let mut pipe = redis::pipe();
            for checksum in batch {
                let key = chunk_index_key(checksum);
                pipe.cmd("ZREM").arg(&key).arg(&advertise_addr).ignore();
            }
            if let Err(err) = pipe.query_async::<()>(&mut conn).await {
                self.metrics.record_error("unregister");
                return Err(err.into());
            }
        }
        self.tracker
            .remove_many(checksums.iter().copied().map(CheckSumOnDisk::from));
        self.metrics.record_unregister(mode, checksums.len() as u64);
        Ok(())
    }

    async fn sync_chunks(&self, checksums: &[CheckSum]) -> anyhow::Result<usize> {
        let owner_sets = self.lookup_owner_values(checksums).await?;
        let advertise_addr = self.advertise_addr.to_string();
        let mut tracked = Vec::new();
        let mut missing = Vec::new();
        for (checksum, owners) in checksums.iter().zip(owner_sets.into_iter()) {
            if owners.iter().any(|owner| owner == &advertise_addr) {
                tracked.push(CheckSumOnDisk::from(*checksum));
            } else if owners.len() < MAX_CHUNK_OWNERS {
                missing.push(*checksum);
            }
        }
        let tracked_count = tracked.len();
        self.tracker.insert_many(tracked);
        let registered = self.register_many(&missing).await?;
        Ok(tracked_count + registered)
    }

    fn refresh_batch_spacing(spread_over: Duration, num_batches: usize) -> Duration {
        if num_batches <= 1 {
            Duration::ZERO
        } else {
            spread_over.mul_f64(0.9) / (num_batches as u32)
        }
    }

    async fn refresh_registrations(&self, spread_over: Duration) -> anyhow::Result<usize> {
        let checksums = self.tracker.snapshot();
        let total = checksums.len();
        if total == 0 {
            return Ok(0);
        }
        let num_batches = checksums.chunks(REDIS_REGISTER_BATCH_SIZE).len();
        let sleep_between = Self::refresh_batch_spacing(spread_over, num_batches);
        debug!(
            tracker_size = total,
            num_batches,
            sleep_ms = sleep_between.as_millis(),
            "refreshing tracked chunk index registrations (spread)"
        );
        let started = tokio::time::Instant::now();
        let mut registered = 0usize;
        for (index, batch) in checksums.chunks(REDIS_REGISTER_BATCH_SIZE).enumerate() {
            registered += self.register_many(batch).await?;
            if index + 1 < num_batches && !sleep_between.is_zero() {
                let next_batch_at = started + sleep_between.mul_f64((index + 1) as f64);
                tokio::time::sleep_until(next_batch_at).await;
            }
        }
        Ok(registered)
    }

    async fn repair_missing_chunks(&self, checksums: &[CheckSum]) -> anyhow::Result<usize> {
        let candidates = checksums
            .iter()
            .copied()
            .filter(|checksum| !self.tracker.contains(checksum))
            .collect::<Vec<_>>();
        self.sync_chunks(&candidates).await
    }
}

#[async_trait]
impl ChunkIndex for RedisChunkIndex {
    async fn lookup_owners(&self, cs: &CheckSum) -> anyhow::Result<Vec<SocketAddr>> {
        self.resolve_owners(cs).await
    }

    async fn register(&self, cs: &CheckSum) -> anyhow::Result<()> {
        self.register_many(&[*cs]).await.map(|_| ())
    }

    async fn register_batch(&self, checksums: &[CheckSum]) -> anyhow::Result<()> {
        self.register_many(checksums).await.map(|_| ())
    }

    async fn unregister(&self, cs: &CheckSum) -> anyhow::Result<()> {
        self.unregister_many(&[*cs]).await
    }

    fn refresh_interval(&self) -> Option<Duration> {
        Some(Duration::from_secs((self.ttl_secs / 3).max(1)))
    }

    async fn unregister_batch(&self, checksums: &[CheckSum]) -> anyhow::Result<()> {
        self.unregister_many(checksums).await
    }

    async fn sync_existing_chunks(&self, checksums: &[CheckSum]) -> anyhow::Result<usize> {
        self.sync_chunks(checksums).await
    }

    async fn refresh_registered(&self, spread_over: Duration) -> anyhow::Result<Option<usize>> {
        match self.refresh_registrations(spread_over).await {
            Ok(count) => {
                self.metrics.record_refresh("ok", count as u64);
                Ok(Some(count))
            }
            Err(err) => {
                self.metrics.record_error("refresh");
                Err(err)
            }
        }
    }

    async fn repair_missing_owners(&self, checksums: &[CheckSum]) -> anyhow::Result<Option<usize>> {
        match self.repair_missing_chunks(checksums).await {
            Ok(count) => {
                self.metrics.record_repair("ok", count as u64);
                Ok(Some(count))
            }
            Err(err) => {
                self.metrics.record_error("repair");
                Err(err)
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct PeerHitHints {
    recent_hits: RwLock<HashMap<SocketAddr, HitRecord>>,
}

#[derive(Debug, Clone, Default)]
struct HitRecord {
    hit_count: u64,
    last_hit_time: u64,
    recent_checksums: VecDeque<CheckSumOnDisk>,
}

impl PeerHitHints {
    pub fn record_hit(&self, addr: SocketAddr, checksum: CheckSum) {
        let now = now_epoch_secs();
        let mut hits = self.recent_hits.write().unwrap();
        let record = hits.entry(addr).or_default();
        record.hit_count += 1;
        record.last_hit_time = now;
        record.recent_checksums.push_back(checksum.into());
        while record.recent_checksums.len() > HINT_MAX_RECENT {
            record.recent_checksums.pop_front();
        }
    }

    #[allow(dead_code)]
    #[cfg(test)]
    pub fn score_peer(&self, addr: &SocketAddr) -> f64 {
        let hits = self.recent_hits.read().unwrap();
        let now = now_epoch_secs();
        hits.get(addr).map_or(0.0, |record| {
            let count_score = (record.hit_count as f64).ln_1p();
            let age = now.saturating_sub(record.last_hit_time) as f64;
            let decay = (-age / 60.0).exp();
            count_score * decay
        })
    }

    fn score_snapshot(&self) -> HashMap<SocketAddr, f64> {
        let hits = self.recent_hits.read().unwrap();
        let now = now_epoch_secs();
        hits.iter()
            .map(|(addr, record)| {
                let count_score = (record.hit_count as f64).ln_1p();
                let age = now.saturating_sub(record.last_hit_time) as f64;
                let decay = (-age / 60.0).exp();
                (*addr, count_score * decay)
            })
            .collect()
    }

    pub fn gc_expired(&self) {
        let cutoff = now_epoch_secs().saturating_sub(HINT_TTL_SECS);
        let mut hits = self.recent_hits.write().unwrap();
        hits.retain(|_, record| record.last_hit_time >= cutoff);
    }
}

#[derive(Debug, Default)]
pub struct PeerHealthTracker {
    peers: RwLock<HashMap<SocketAddr, PeerHealth>>,
}

#[derive(Debug, Clone)]
struct PeerHealth {
    avg_rtt_ms: f64,
    consecutive_failures: u32,
    last_failure_time: u64,
    total_requests: u64,
    total_hits: u64,
}

#[derive(Debug, Clone)]
struct PeerHealthSnapshotEntry {
    addr: SocketAddr,
    avg_rtt_ms: f64,
    healthy: bool,
}

#[derive(Debug, Clone, Default)]
struct PeerHealthSnapshot {
    entries: Vec<PeerHealthSnapshotEntry>,
    healthy_count: u64,
    unhealthy_count: u64,
}

impl Default for PeerHealth {
    fn default() -> Self {
        Self {
            avg_rtt_ms: 50.0,
            consecutive_failures: 0,
            last_failure_time: 0,
            total_requests: 0,
            total_hits: 0,
        }
    }
}

impl PeerHealthTracker {
    pub fn record_success(&self, addr: SocketAddr, rtt_ms: f64) {
        let mut peers = self.peers.write().unwrap();
        let entry = peers.entry(addr).or_default();
        entry.avg_rtt_ms = 0.3 * rtt_ms + 0.7 * entry.avg_rtt_ms;
        entry.consecutive_failures = 0;
        entry.total_requests += 1;
        entry.total_hits += 1;
    }

    pub fn record_failure(&self, addr: SocketAddr) {
        let mut peers = self.peers.write().unwrap();
        let entry = peers.entry(addr).or_default();
        entry.consecutive_failures += 1;
        entry.last_failure_time = now_epoch_secs();
        entry.total_requests += 1;
    }

    pub fn record_miss(&self, addr: SocketAddr) {
        let mut peers = self.peers.write().unwrap();
        let entry = peers.entry(addr).or_default();
        entry.total_requests += 1;
    }

    #[allow(dead_code)]
    pub fn is_unhealthy(&self, addr: &SocketAddr) -> bool {
        let peers = self.peers.read().unwrap();
        match peers.get(addr) {
            Some(peer) => {
                peer.consecutive_failures >= 3
                    && now_epoch_secs().saturating_sub(peer.last_failure_time) < 30
            }
            None => false,
        }
    }

    #[allow(dead_code)]
    pub fn get_rtt_ms(&self, addr: &SocketAddr) -> f64 {
        let peers = self.peers.read().unwrap();
        peers.get(addr).map_or(50.0, |peer| peer.avg_rtt_ms)
    }

    fn snapshot(&self) -> PeerHealthSnapshot {
        let peers = self.peers.read().unwrap();
        let mut snapshot = PeerHealthSnapshot::default();
        let now = now_epoch_secs();
        for (addr, peer) in peers.iter() {
            let healthy = !(peer.consecutive_failures >= 3
                && now.saturating_sub(peer.last_failure_time) < 30);
            if healthy {
                snapshot.healthy_count += 1;
            } else {
                snapshot.unhealthy_count += 1;
            }
            snapshot.entries.push(PeerHealthSnapshotEntry {
                addr: *addr,
                avg_rtt_ms: peer.avg_rtt_ms,
                healthy,
            });
        }
        snapshot
    }

    fn register_metrics(self: &Arc<Self>, meter: &Meter) -> Vec<Box<dyn CallbackRegistration>> {
        let peer_rtt = meter
            .f64_observable_gauge("distill_fs.health.peer_rtt_ms")
            .with_description("Current EMA RTT per peer")
            .with_unit("ms")
            .init();
        let peer_status = meter
            .u64_observable_gauge("distill_fs.health.peer_status")
            .with_description("Peer status: 1 healthy, 0 unhealthy")
            .init();
        let peers_total = meter
            .u64_observable_gauge("distill_fs.health.peers_total")
            .with_description("Current total peers by health status")
            .init();

        let health_rtt = self.clone();
        let reg1 = meter
            .register_callback(
                &[
                    peer_rtt.as_any(),
                    peer_status.as_any(),
                    peers_total.as_any(),
                ],
                move |observer| {
                    let snapshot = health_rtt.snapshot();
                    for entry in &snapshot.entries {
                        let peer_attr = [KeyValue::new("peer", entry.addr.to_string())];
                        observer.observe_f64(&peer_rtt, entry.avg_rtt_ms, &peer_attr);
                        observer.observe_u64(
                            &peer_status,
                            if entry.healthy { 1 } else { 0 },
                            &peer_attr,
                        );
                    }
                    observer.observe_u64(
                        &peers_total,
                        snapshot.healthy_count,
                        &[KeyValue::new("status", "healthy")],
                    );
                    observer.observe_u64(
                        &peers_total,
                        snapshot.unhealthy_count,
                        &[KeyValue::new("status", "unhealthy")],
                    );
                },
            )
            .map_err(|err| debug!(err = debug(err), "failed to register peer health metrics"))
            .ok();

        reg1.into_iter().collect()
    }
}

pub struct PeerClient {
    runtime: PeerRuntime,
    discovery: Arc<dyn PeerDiscovery>,
    chunk_index: Option<Arc<dyn ChunkIndex>>,
    query_timeout: Duration,
    max_query_peers: usize,
    local_addr: Option<SocketAddr>,
    hit_hints: Arc<PeerHitHints>,
    health: Arc<PeerHealthTracker>,
    pools: PeerPoolMap,
    metrics: PeerClientMetrics,
    _metric_regs: Vec<Box<dyn CallbackRegistration>>,
}

impl std::fmt::Debug for PeerClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerClient")
            .field("query_timeout", &self.query_timeout)
            .field("max_query_peers", &self.max_query_peers)
            .finish()
    }
}

impl PeerClient {
    pub fn new(runtime: PeerRuntime, discovery: Arc<dyn PeerDiscovery>) -> Self {
        let meter = global::meter("distill_fs.peer");
        let health = Arc::new(PeerHealthTracker::default());
        let metric_regs = health.register_metrics(&meter);
        Self {
            runtime,
            discovery,
            chunk_index: None,
            query_timeout: Duration::from_millis(DEFAULT_TIMEOUT_MS),
            max_query_peers: DEFAULT_MAX_QUERY_PEERS,
            local_addr: None,
            hit_hints: Arc::new(PeerHitHints::default()),
            health,
            pools: Arc::new(Mutex::new(HashMap::new())),
            metrics: PeerClientMetrics::new(&meter),
            _metric_regs: metric_regs,
        }
    }

    pub fn with_chunk_index(mut self, chunk_index: Arc<dyn ChunkIndex>) -> Self {
        self.chunk_index = Some(chunk_index);
        self
    }

    pub fn with_local_addr(mut self, local_addr: SocketAddr) -> Self {
        self.local_addr = Some(local_addr);
        self
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub fn with_timeout(mut self, query_timeout: Duration) -> Self {
        self.query_timeout = query_timeout;
        self
    }

    pub fn start_background_tasks(&self, shutdown: ShutdownHandle) -> JoinHandle<()> {
        let hints = Arc::clone(&self.hit_hints);
        self.runtime.spawn(async move {
            let mut ticker = interval(Duration::from_secs(HINT_GC_INTERVAL_SECS));
            loop {
                tokio::select! {
                    _ = shutdown.wait() => break,
                    _ = ticker.tick() => hints.gc_expired(),
                }
            }
        })
    }

    fn is_self(&self, addr: &SocketAddr) -> bool {
        self.local_addr.is_some_and(|local| local == *addr)
    }

    fn ranked_peers_with_scores(
        &self,
        mut peers: Vec<SocketAddr>,
        scores: &HashMap<SocketAddr, f64>,
    ) -> Vec<SocketAddr> {
        let health = self
            .health
            .snapshot()
            .entries
            .into_iter()
            .map(|entry| (entry.addr, (entry.healthy, entry.avg_rtt_ms)))
            .collect::<HashMap<_, _>>();
        peers.retain(|addr| !self.is_self(addr));
        peers.sort_by(|a, b| {
            let (a_healthy, a_rtt_ms) = health.get(a).copied().unwrap_or((true, 50.0));
            let (b_healthy, b_rtt_ms) = health.get(b).copied().unwrap_or((true, 50.0));
            let a_unhealthy = !a_healthy;
            let b_unhealthy = !b_healthy;
            match a_unhealthy.cmp(&b_unhealthy) {
                std::cmp::Ordering::Equal => {
                    let a_score = scores.get(a).copied().unwrap_or_default() * 10.0
                        + 100.0 / a_rtt_ms.max(1.0);
                    let b_score = scores.get(b).copied().unwrap_or_default() * 10.0
                        + 100.0 / b_rtt_ms.max(1.0);
                    b_score
                        .partial_cmp(&a_score)
                        .unwrap_or(std::cmp::Ordering::Equal)
                }
                other => other,
            }
        });
        peers.dedup();
        peers
    }

    fn discovery_peers(&self) -> Vec<SocketAddr> {
        self.discovery
            .get_peers()
            .into_iter()
            .filter(|addr| !self.is_self(addr))
            .collect()
    }

    async fn candidate_peers(&self, checksum: &CheckSum) -> (Vec<SocketAddr>, &'static str) {
        let scores = self.hit_hints.score_snapshot();
        if let Some(index) = &self.chunk_index {
            let owners = match index.lookup_owners(checksum).await {
                Ok(owners) => self.ranked_peers_with_scores(owners, &scores),
                Err(err) => {
                    debug!(err = debug(err), checksum = %checksum, "chunk index lookup failed");
                    Vec::new()
                }
            };
            if !owners.is_empty() {
                return (
                    owners.into_iter().take(self.max_query_peers).collect(),
                    "index",
                );
            }
        }

        let ranked = self.ranked_peers_with_scores(self.discovery_peers(), &scores);
        if ranked.is_empty() {
            return (ranked, "random");
        }

        let mut hinted = Vec::new();
        let mut unknown = Vec::new();
        for addr in ranked {
            if scores.get(&addr).copied().unwrap_or_default() > 0.0 {
                hinted.push(addr);
            } else {
                unknown.push(addr);
            }
        }

        if !hinted.is_empty() {
            let mut selected = hinted;
            if selected.len() < self.max_query_peers && !unknown.is_empty() {
                let mut rng = rand::thread_rng();
                unknown.shuffle(&mut rng);
                selected.extend(
                    unknown
                        .into_iter()
                        .take(self.max_query_peers - selected.len()),
                );
            }
            selected.truncate(self.max_query_peers);
            return (selected, "hithints");
        }

        let mut rng = rand::thread_rng();
        unknown.shuffle(&mut rng);
        unknown.truncate(self.max_query_peers);
        (unknown, "random")
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub fn fetch_chunk_blocking(&self, checksum: &CheckSum) -> Option<Vec<u8>> {
        self.runtime.block_on(self.fetch_chunk(checksum))
    }

    #[allow(dead_code)]
    pub fn health_check_blocking(&self, peer: SocketAddr) -> bool {
        self.runtime.block_on(self.health_check(peer))
    }

    pub async fn fetch_chunk(&self, checksum: &CheckSum) -> Option<Vec<u8>> {
        let begin_total = Instant::now();
        let (peers, source) = self.candidate_peers(checksum).await;
        let mut saw_miss = false;
        let mut saw_timeout = false;
        let mut saw_error = false;
        for (attempt, peer) in peers.into_iter().enumerate() {
            if source == "index" && attempt > 0 {
                self.metrics.record_retry(source);
            }
            let begin = Instant::now();
            match self.query_peer(peer, checksum).await {
                Ok(Some(data)) => {
                    let elapsed_ms = begin.elapsed().as_secs_f64() * 1000.0;
                    self.health.record_success(peer, elapsed_ms);
                    self.hit_hints.record_hit(peer, *checksum);
                    self.metrics.record_fetch(
                        source,
                        "hit",
                        begin_total.elapsed().as_secs_f64() * 1000.0,
                    );
                    return Some(data);
                }
                Ok(None) => {
                    saw_miss = true;
                    self.health.record_miss(peer);
                }
                Err(err) => {
                    debug!(peer = %peer, err = debug(&err), "peer query failed");
                    if err.kind() == ErrorKind::TimedOut {
                        saw_timeout = true;
                    } else {
                        saw_error = true;
                    }
                    self.health.record_failure(peer);
                }
            }
        }
        let result = if saw_miss {
            "miss"
        } else if saw_timeout {
            "timeout"
        } else if saw_error {
            "error"
        } else {
            "miss"
        };
        self.metrics
            .record_fetch(source, result, begin_total.elapsed().as_secs_f64() * 1000.0);
        None
    }

    #[allow(dead_code)]
    pub async fn health_check(&self, peer: SocketAddr) -> bool {
        let pool = self.tcp_pool(peer).await;
        let session = match timeout(
            self.query_timeout,
            pool.acquire(|| async move {
                let stream = TcpStream::connect(peer).await?;
                stream.set_nodelay(true)?;
                Ok(MultiplexedSession::from_tcp(stream))
            }),
        )
        .await
        {
            Ok(Ok(session)) => session,
            Ok(Err(err)) => {
                debug!(peer = %peer, err = debug(err), "peer health check connect failed");
                return false;
            }
            Err(_) => return false,
        };
        let response = session
            .send_request(
                Request::whole_chunk(MessageType::HealthCheck, CheckSum::empty()),
                self.query_timeout,
            )
            .await;
        pool.prune().await;
        matches!(response, Ok(resp) if resp.status == STATUS_HIT)
    }

    async fn query_peer(
        &self,
        peer: SocketAddr,
        checksum: &CheckSum,
    ) -> io::Result<Option<Vec<u8>>> {
        let begin = Instant::now();
        let pool = self.tcp_pool(peer).await;
        let result = async {
            let session = timeout(
                self.query_timeout,
                pool.acquire(|| async move {
                    let stream = TcpStream::connect(peer).await?;
                    stream.set_nodelay(true)?;
                    Ok(MultiplexedSession::from_tcp(stream))
                }),
            )
            .await
            .map_err(|_| timeout_error("peer connect"))??;
            let response = session
                .send_request(
                    Request::whole_chunk(MessageType::GetChunk, *checksum),
                    self.query_timeout,
                )
                .await?;
            pool.prune().await;
            match response.status {
                STATUS_HIT => Ok(Some(response.payload)),
                STATUS_MISS => Ok(None),
                _ => Err(io::Error::other("peer returned error")),
            }
        }
        .await;
        let elapsed_ms = begin.elapsed().as_secs_f64() * 1000.0;
        let status = match &result {
            Ok(Some(_)) => "hit",
            Ok(None) => "miss",
            Err(err) if err.kind() == ErrorKind::TimedOut => "timeout",
            Err(_) => "error",
        };
        self.metrics.record_query(status, elapsed_ms);
        result
    }

    async fn tcp_pool(&self, peer: SocketAddr) -> Arc<TcpConnPool> {
        let mut pools = self.pools.lock().await;
        Arc::clone(
            pools
                .entry(peer)
                .or_insert_with(|| Arc::new(TcpConnPool::default())),
        )
    }
}

const CIRCUIT_BREAKER_COOLDOWN: Duration = Duration::from_secs(30);
const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Tracks connection failures and short-circuits requests when the server
/// is known to be unavailable, re-probing after a cooldown period.
#[derive(Debug)]
struct CircuitBreaker {
    open: AtomicBool,
    /// Monotonic tick (in millis since an arbitrary epoch) of the last failure.
    last_failure_ms: AtomicU64,
    cooldown: Duration,
    /// Anchor `Instant` used to convert `Instant::now()` into a u64.
    epoch: Instant,
}

impl CircuitBreaker {
    fn new(cooldown: Duration) -> Self {
        Self {
            open: AtomicBool::new(false),
            last_failure_ms: AtomicU64::new(0),
            cooldown,
            epoch: Instant::now(),
        }
    }

    /// Returns `true` if the circuit is open and the cooldown has **not** yet
    /// elapsed — callers should skip the request.
    fn should_reject(&self) -> bool {
        if !self.open.load(Ordering::Acquire) {
            return false;
        }
        let last = self.last_failure_ms.load(Ordering::Relaxed);
        let now = self.epoch.elapsed().as_millis() as u64;
        now.saturating_sub(last) < self.cooldown.as_millis() as u64
    }

    fn record_success(&self) {
        self.open.store(false, Ordering::Release);
    }

    fn record_failure(&self) {
        let now = self.epoch.elapsed().as_millis() as u64;
        self.last_failure_ms.store(now, Ordering::Relaxed);
        self.open.store(true, Ordering::Release);
    }
}

impl Clone for CircuitBreaker {
    fn clone(&self) -> Self {
        Self {
            open: AtomicBool::new(self.open.load(Ordering::Relaxed)),
            last_failure_ms: AtomicU64::new(self.last_failure_ms.load(Ordering::Relaxed)),
            cooldown: self.cooldown,
            epoch: self.epoch,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LocalChunkClient {
    runtime: PeerRuntime,
    socket_path: PathBuf,
    timeout: Duration,
    pool: Arc<UnixConnPool>,
    metrics: LocalClientMetrics,
    breaker: Arc<CircuitBreaker>,
}

impl LocalChunkClient {
    pub fn new<P: AsRef<Path>>(runtime: PeerRuntime, socket_path: P, timeout: Duration) -> Self {
        let meter = global::meter("distill_fs.local");
        Self {
            runtime,
            socket_path: socket_path.as_ref().to_path_buf(),
            timeout,
            pool: Arc::new(UnixConnPool::default()),
            metrics: LocalClientMetrics::new(&meter),
            breaker: Arc::new(CircuitBreaker::new(CIRCUIT_BREAKER_COOLDOWN)),
        }
    }

    /// Spawn a background task that periodically probes the chunk server
    /// socket and updates the circuit breaker.  This keeps the FUSE I/O
    /// path from having to discover unavailability itself in most cases.
    pub fn start_health_checker(&self) {
        let socket_path = self.socket_path.clone();
        let timeout_dur = self.timeout;
        let breaker = Arc::clone(&self.breaker);
        let metrics = self.metrics.clone();
        self.runtime.spawn(async move {
            let mut tick = interval(HEALTH_CHECK_INTERVAL);
            loop {
                tick.tick().await;
                let result = timeout(timeout_dur, UnixStream::connect(&socket_path)).await;
                match result {
                    Ok(Ok(_stream)) => {
                        breaker.record_success();
                        metrics.record_request("health_check", "ok", 0.0);
                    }
                    _ => {
                        breaker.record_failure();
                        metrics.record_request("health_check", "unavailable", 0.0);
                    }
                }
            }
        });
    }

    pub fn prefetch_chunk_blocking(&self, checksum: &CheckSum) -> bool {
        self.runtime.block_on(self.prefetch_chunk(checksum))
    }

    #[allow(dead_code)]
    pub fn health_check_blocking(&self) -> bool {
        self.runtime.block_on(self.health_check())
    }

    pub fn register_local_chunk(&self, checksum: &CheckSum) -> bool {
        self.runtime.block_on(self.control_request(
            "register",
            MessageType::RegisterChunk,
            checksum,
        ))
    }

    pub fn register_local_chunks(&self, checksums: &[CheckSum]) -> bool {
        self.runtime.block_on(self.control_batch_request(
            "register_batch",
            MessageType::RegisterChunks,
            checksums,
        ))
    }

    pub fn unregister_local_chunk(&self, checksum: &CheckSum) -> bool {
        self.runtime.block_on(self.control_request(
            "unregister",
            MessageType::UnregisterChunk,
            checksum,
        ))
    }

    pub fn unregister_local_chunks(&self, checksums: &[CheckSum]) -> bool {
        self.runtime.block_on(self.control_batch_request(
            "unregister_batch",
            MessageType::UnregisterChunks,
            checksums,
        ))
    }

    pub async fn prefetch_chunk(&self, checksum: &CheckSum) -> bool {
        self.control_request("prefetch", MessageType::PrefetchChunk, checksum)
            .await
    }

    #[allow(dead_code)]
    pub async fn health_check(&self) -> bool {
        self.control_request("health_check", MessageType::HealthCheck, &CheckSum::empty())
            .await
    }

    async fn control_request(
        &self,
        op: &'static str,
        message_type: MessageType,
        checksum: &CheckSum,
    ) -> bool {
        if self.breaker.should_reject() {
            self.metrics.record_request(op, "circuit_open", 0.0);
            return false;
        }
        let begin = Instant::now();
        let result = self.send_control(message_type, checksum).await;
        let status = match &result {
            Ok(true) => "hit",
            Ok(false) => "miss",
            Err(err) if err.kind() == ErrorKind::TimedOut => "timeout",
            Err(_) => "error",
        };
        self.metrics
            .record_request(op, status, begin.elapsed().as_secs_f64() * 1000.0);
        if result.is_ok() {
            self.breaker.record_success();
        } else {
            self.breaker.record_failure();
        }
        matches!(result, Ok(true))
    }

    async fn control_batch_request(
        &self,
        op: &'static str,
        message_type: MessageType,
        checksums: &[CheckSum],
    ) -> bool {
        if self.breaker.should_reject() {
            self.metrics.record_request(op, "circuit_open", 0.0);
            return false;
        }
        let begin = Instant::now();
        let result = self.send_control_batch(message_type, checksums).await;
        let status = match &result {
            Ok(true) => "hit",
            Ok(false) => "miss",
            Err(err) if err.kind() == ErrorKind::TimedOut => "timeout",
            Err(_) => "error",
        };
        self.metrics
            .record_request(op, status, begin.elapsed().as_secs_f64() * 1000.0);
        if result.is_ok() {
            self.breaker.record_success();
        } else {
            self.breaker.record_failure();
        }
        matches!(result, Ok(true))
    }

    async fn send_control(
        &self,
        message_type: MessageType,
        checksum: &CheckSum,
    ) -> io::Result<bool> {
        let session = match timeout(
            self.timeout,
            self.pool.acquire(|| async {
                let stream = UnixStream::connect(&self.socket_path).await?;
                Ok(MultiplexedSession::from_unix(stream))
            }),
        )
        .await
        {
            Ok(Ok(session)) => session,
            Ok(Err(err)) => {
                debug!(
                    sock = display(self.socket_path.display()),
                    err = debug(&err),
                    "local chunk socket unavailable"
                );
                return Err(err);
            }
            Err(_) => return Err(timeout_error("local connect")),
        };
        let response = session
            .send_request(Request::whole_chunk(message_type, *checksum), self.timeout)
            .await;
        self.pool.prune().await;
        let response = response?;
        match response.status {
            STATUS_HIT => Ok(true),
            STATUS_MISS => Ok(false),
            _ => Err(io::Error::other("local chunk server returned error")),
        }
    }

    async fn send_control_batch(
        &self,
        message_type: MessageType,
        checksums: &[CheckSum],
    ) -> io::Result<bool> {
        if checksums.is_empty() {
            return Ok(true);
        }
        let session = match timeout(
            self.timeout,
            self.pool.acquire(|| async {
                let stream = UnixStream::connect(&self.socket_path).await?;
                Ok(MultiplexedSession::from_unix(stream))
            }),
        )
        .await
        {
            Ok(Ok(session)) => session,
            Ok(Err(err)) => {
                debug!(
                    sock = display(self.socket_path.display()),
                    err = debug(&err),
                    "local chunk socket unavailable"
                );
                return Err(err);
            }
            Err(_) => return Err(timeout_error("local connect")),
        };
        let mut payload = Vec::with_capacity(checksums.len() * 33);
        for checksum in checksums {
            payload.push(checksum.method.into());
            payload.extend_from_slice(&checksum.raw);
        }
        let request = Request::control_batch(message_type, checksums.len());
        let response = session
            .send_batch_request(request, payload, self.timeout)
            .await;
        self.pool.prune().await;
        let response = response?;
        match response.status {
            STATUS_HIT => Ok(true),
            STATUS_MISS => Ok(false),
            _ => Err(io::Error::other("local chunk server returned error")),
        }
    }
}

async fn read_checksum_batch<R>(reader: &mut R, count: usize) -> io::Result<Vec<CheckSum>>
where
    R: AsyncRead + Unpin,
{
    let mut checksums = Vec::with_capacity(count);
    let mut raw = [0_u8; 32];
    for _ in 0..count {
        let mut method = [0_u8; 1];
        reader.read_exact(&mut method).await?;
        reader.read_exact(&mut raw).await?;
        checksums.push(CheckSum::new(&raw, method[0].into())?);
    }
    Ok(checksums)
}

impl ChunkIndexControl for LocalChunkClient {
    fn register_chunk(&self, checksum: &CheckSum) -> bool {
        self.register_local_chunk(checksum)
    }

    fn register_chunks(&self, checksums: &[CheckSum]) -> bool {
        self.register_local_chunks(checksums)
    }

    fn unregister_chunk(&self, checksum: &CheckSum) -> bool {
        self.unregister_local_chunk(checksum)
    }

    fn unregister_chunks(&self, checksums: &[CheckSum]) -> bool {
        self.unregister_local_chunks(checksums)
    }
}

#[derive(Clone)]
pub struct SyncChunkIndexControl {
    runtime: PeerRuntime,
    chunk_index: Arc<dyn ChunkIndex>,
}

impl SyncChunkIndexControl {
    pub fn new(runtime: PeerRuntime, chunk_index: Arc<dyn ChunkIndex>) -> Self {
        Self {
            runtime,
            chunk_index,
        }
    }
}

impl ChunkIndexControl for SyncChunkIndexControl {
    fn register_chunk(&self, checksum: &CheckSum) -> bool {
        self.runtime
            .block_on(self.chunk_index.register(checksum))
            .is_ok()
    }

    fn register_chunks(&self, checksums: &[CheckSum]) -> bool {
        self.runtime
            .block_on(self.chunk_index.register_batch(checksums))
            .is_ok()
    }

    fn unregister_chunk(&self, checksum: &CheckSum) -> bool {
        self.runtime
            .block_on(self.chunk_index.unregister(checksum))
            .is_ok()
    }

    fn unregister_chunks(&self, checksums: &[CheckSum]) -> bool {
        self.runtime
            .block_on(self.chunk_index.unregister_batch(checksums))
            .is_ok()
    }
}

pub struct ChunkServer {
    runtime: PeerRuntime,
    chunk_db: Arc<ChunkDB>,
    tcp_listener: StdTcpListener,
    unix_listener: StdUnixListener,
    unix_socket_path: PathBuf,
    shutdown: ShutdownHandle,
    peer_client: Option<Arc<PeerClient>>,
    max_connections: usize,
    metrics: Arc<ChunkServerMetrics>,
}

impl std::fmt::Debug for ChunkServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkServer").finish()
    }
}

impl ChunkServer {
    pub fn new<P: AsRef<Path>>(
        runtime: PeerRuntime,
        chunk_db: Arc<ChunkDB>,
        listen_addr: SocketAddr,
        unix_socket_path: P,
        peer_client: Option<Arc<PeerClient>>,
    ) -> anyhow::Result<Self> {
        let unix_socket_path = unix_socket_path.as_ref().to_path_buf();
        if let Some(parent) = unix_socket_path.parent() {
            fs::create_dir_all(parent)?;
        }
        if unix_socket_path.exists() {
            fs::remove_file(&unix_socket_path)?;
        }

        let tcp_listener = StdTcpListener::bind(listen_addr)
            .with_context(|| format!("failed to bind tcp listener on {listen_addr}"))?;
        let unix_listener = StdUnixListener::bind(&unix_socket_path).with_context(|| {
            format!(
                "failed to bind unix listener on {}",
                unix_socket_path.display()
            )
        })?;
        tcp_listener.set_nonblocking(true)?;
        unix_listener.set_nonblocking(true)?;

        let meter = global::meter("distill_fs.chunkserver");
        Ok(Self {
            runtime,
            chunk_db,
            tcp_listener,
            unix_listener,
            unix_socket_path,
            shutdown: ShutdownHandle::new(),
            peer_client,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            metrics: Arc::new(ChunkServerMetrics::new(&meter)),
        })
    }

    #[allow(dead_code)] // Used by tests and auxiliary binaries.
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        self.shutdown.clone()
    }

    #[allow(dead_code)] // Used by tests and auxiliary binaries.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.tcp_listener.local_addr()
    }

    pub fn run(self) -> anyhow::Result<()> {
        let runtime = self.runtime.clone();
        runtime.block_on(self.run_inner())
    }

    async fn run_inner(self) -> anyhow::Result<()> {
        let shutdown = self.shutdown.clone();
        let tcp_listener = TcpListener::from_std(self.tcp_listener.try_clone()?)?;
        let unix_listener = UnixListener::from_std(self.unix_listener.try_clone()?)?;
        let semaphore = Arc::new(Semaphore::new(self.max_connections));
        self.run_index_sync();

        let gc_task = self
            .peer_client
            .as_ref()
            .map(|client| client.start_background_tasks(shutdown.clone()));
        let index_refresh_task = self.run_index_refresh(shutdown.clone());

        // Periodically reclaim LMDB reader slots left by dead threads.
        // block_in_place causes tokio worker thread replacement; the old
        // threads die and leave stale reader slots in the LMDB lock file.
        let stale_chunk_db = Arc::clone(&self.chunk_db);
        let stale_shutdown = shutdown.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            interval.tick().await; // skip immediate first tick
            loop {
                interval.tick().await;
                if stale_shutdown.is_shutdown() {
                    break;
                }
                let _ = stale_chunk_db.clear_stale_readers();
            }
        });

        let tcp_accept = ConnectionAcceptor {
            chunk_db: Arc::clone(&self.chunk_db),
            peer_client: self.peer_client.clone(),
            metrics: Arc::clone(&self.metrics),
            semaphore: Arc::clone(&semaphore),
            shutdown: shutdown.clone(),
        };
        let unix_accept = ConnectionAcceptor {
            chunk_db: Arc::clone(&self.chunk_db),
            peer_client: self.peer_client.clone(),
            metrics: Arc::clone(&self.metrics),
            semaphore,
            shutdown: shutdown.clone(),
        };
        let tcp_task = tokio::spawn(tcp_accept.serve_tcp(tcp_listener));
        let unix_task = tokio::spawn(unix_accept.serve_unix(unix_listener));

        let tcp_res = tcp_task
            .await
            .map_err(|_| io::Error::other("tcp accept task panicked"))?;
        let unix_res = unix_task
            .await
            .map_err(|_| io::Error::other("unix accept task panicked"))?;
        if let Some(task) = gc_task {
            shutdown.shutdown();
            let _ = task.await;
        }
        if let Some(task) = index_refresh_task {
            let _ = task.await;
        }
        let _ = fs::remove_file(&self.unix_socket_path);
        tcp_res?;
        unix_res?;
        Ok(())
    }
}

struct ConnectionAcceptor {
    chunk_db: Arc<ChunkDB>,
    peer_client: Option<Arc<PeerClient>>,
    metrics: Arc<ChunkServerMetrics>,
    semaphore: Arc<Semaphore>,
    shutdown: ShutdownHandle,
}

impl ConnectionAcceptor {
    async fn serve_tcp(self, listener: TcpListener) -> io::Result<()> {
        self.accept_loop("tcp", listener, |stream| {
            let _ = stream.set_nodelay(true);
            stream
        })
        .await
    }

    async fn serve_unix(self, listener: UnixListener) -> io::Result<()> {
        self.accept_loop("unix", listener, |stream| stream).await
    }

    async fn accept_loop<L, S, F>(
        &self,
        transport: &'static str,
        listener: L,
        prepare: F,
    ) -> io::Result<()>
    where
        L: Listener<Stream = S>,
        S: RequestStream,
        F: Fn(S) -> S,
    {
        loop {
            tokio::select! {
                _ = self.shutdown.wait() => break,
                accepted = listener.accept_stream() => {
                    let stream = prepare(accepted?);
                    let permit = self.semaphore.clone().acquire_owned().await.map_err(|_| io::Error::other("semaphore closed"))?;
                    let request_handler = RequestHandler::new(
                        Arc::clone(&self.chunk_db),
                        self.peer_client.clone(),
                        Arc::clone(&self.metrics),
                        transport,
                    );
                    let metrics = Arc::clone(&self.metrics);
                    tokio::spawn(async move {
                        let _permit = permit;
                        metrics.record_connection_delta(transport, 1);
                        if let Err(err) = request_handler.handle(stream).await {
                            debug!(err = debug(err), "{transport} request failed");
                        }
                        metrics.record_connection_delta(transport, -1);
                    });
                }
            }
        }
        Ok(())
    }
}

trait Listener {
    type Stream;
    async fn accept_stream(&self) -> io::Result<Self::Stream>;
}

impl Listener for TcpListener {
    type Stream = TcpStream;
    async fn accept_stream(&self) -> io::Result<TcpStream> {
        self.accept().await.map(|(stream, _)| stream)
    }
}

impl Listener for UnixListener {
    type Stream = UnixStream;
    async fn accept_stream(&self) -> io::Result<UnixStream> {
        self.accept().await.map(|(stream, _)| stream)
    }
}

struct RequestContext<'a> {
    chunk_db: &'a ChunkDB,
    peer_client: Option<&'a PeerClient>,
    metrics: &'a ChunkServerMetrics,
    transport: &'static str,
}

struct GetRequest<'a> {
    ctx: &'a RequestContext<'a>,
    request: &'a Request,
}

impl GetRequest<'_> {
    fn action(&self) -> ResponseAction {
        let begin = Instant::now();
        match self.request.ensure_full_chunk() {
            Ok(()) => ResponseAction::GetChunk {
                request_id: self.request.request_id,
                checksum: self.request.checksum,
            },
            Err(err) => {
                debug!(err = debug(err), "invalid get request");
                self.ctx.metrics.record_request(
                    "get_chunk",
                    "error",
                    self.ctx.transport,
                    begin.elapsed().as_secs_f64() * 1000.0,
                    0,
                );
                ResponseAction::Immediate(WireResponse {
                    request_id: self.request.request_id,
                    status: STATUS_ERROR,
                    payload: Vec::new(),
                })
            }
        }
    }
}

struct PrefetchRequest<'a> {
    ctx: &'a RequestContext<'a>,
    request: &'a Request,
}

impl PrefetchRequest<'_> {
    async fn process(&self) -> WireResponse {
        let begin = Instant::now();
        let (status_code, status) = match self.request.ensure_full_chunk() {
            Ok(()) => match Self::prefetch_from_peer(self.ctx, self.request).await {
                Ok(true) => (STATUS_HIT, "hit"),
                Ok(false) => (STATUS_MISS, "miss"),
                Err(err) => {
                    debug!(err = debug(err), "prefetch failed");
                    (STATUS_ERROR, "error")
                }
            },
            Err(err) => {
                debug!(err = debug(err), "invalid prefetch request");
                (STATUS_ERROR, "error")
            }
        };
        self.ctx.metrics.record_request(
            "prefetch",
            status,
            self.ctx.transport,
            begin.elapsed().as_secs_f64() * 1000.0,
            0,
        );
        WireResponse {
            request_id: self.request.request_id,
            status: status_code,
            payload: Vec::new(),
        }
    }

    async fn prefetch_from_peer(
        ctx: &RequestContext<'_>,
        request: &Request,
    ) -> anyhow::Result<bool> {
        if block_in_place(|| ctx.chunk_db.has_chunk(&request.checksum))? {
            return Ok(true);
        }
        let Some(peer_client) = ctx.peer_client else {
            return Ok(false);
        };
        let Some(data) = peer_client.fetch_chunk(&request.checksum).await else {
            return Ok(false);
        };
        let data_cs = CheckSum::from_data(&data, request.checksum.method);
        if data_cs != request.checksum {
            return Ok(false);
        }
        block_in_place(|| ctx.chunk_db.add_chunk(&request.checksum, data))?;
        Ok(true)
    }
}

struct HealthCheckRequest<'a> {
    ctx: &'a RequestContext<'a>,
    request: &'a Request,
}

impl HealthCheckRequest<'_> {
    async fn process(&self) -> WireResponse {
        let begin = Instant::now();
        let status_code = match self.request.ensure_full_chunk() {
            Ok(()) => STATUS_HIT,
            Err(err) => {
                debug!(
                    err = debug(err),
                    transport = self.ctx.transport,
                    "invalid health check request"
                );
                STATUS_ERROR
            }
        };
        let status = if status_code == STATUS_HIT {
            "hit"
        } else {
            "error"
        };
        self.ctx.metrics.record_request(
            "health_check",
            status,
            self.ctx.transport,
            begin.elapsed().as_secs_f64() * 1000.0,
            0,
        );
        WireResponse {
            request_id: self.request.request_id,
            status: status_code,
            payload: Vec::new(),
        }
    }
}

struct ControlRequest<'a> {
    ctx: &'a RequestContext<'a>,
    request: &'a Request,
}

impl ControlRequest<'_> {
    async fn process(&self) -> WireResponse {
        let begin = Instant::now();
        let request_type = if self.request.message_type == MessageType::RegisterChunk {
            "register"
        } else {
            "unregister"
        };
        let (status_code, status) = if self.ctx.transport != "unix" {
            (STATUS_ERROR, "error")
        } else {
            match self.request.ensure_full_chunk() {
                Ok(()) => match self.handle_index_control().await {
                    Ok(()) => (STATUS_HIT, "hit"),
                    Err(err) => {
                        debug!(
                            err = debug(err),
                            request_type = request_type,
                            "chunk index control request failed"
                        );
                        (STATUS_ERROR, "error")
                    }
                },
                Err(err) => {
                    debug!(
                        err = debug(err),
                        request_type = request_type,
                        "invalid chunk index control request"
                    );
                    (STATUS_ERROR, "error")
                }
            }
        };
        self.ctx.metrics.record_request(
            request_type,
            status,
            self.ctx.transport,
            begin.elapsed().as_secs_f64() * 1000.0,
            0,
        );
        WireResponse {
            request_id: self.request.request_id,
            status: status_code,
            payload: Vec::new(),
        }
    }

    async fn handle_index_control(&self) -> anyhow::Result<()> {
        let Some(index) = self
            .ctx
            .peer_client
            .and_then(|client| client.chunk_index.as_ref())
        else {
            return Ok(());
        };
        match self.request.message_type {
            MessageType::RegisterChunk => index.register(&self.request.checksum).await,
            MessageType::UnregisterChunk => index.unregister(&self.request.checksum).await,
            _ => Ok(()),
        }
    }
}

struct ControlBatchRequest<'a> {
    ctx: &'a RequestContext<'a>,
    request: &'a Request,
    checksums: Vec<CheckSum>,
}

impl ControlBatchRequest<'_> {
    async fn process(&self) -> WireResponse {
        let begin = Instant::now();
        let request_type = if self.request.message_type == MessageType::RegisterChunks {
            "register_batch"
        } else {
            "unregister_batch"
        };
        let (status_code, status) = if self.ctx.transport != "unix" {
            (STATUS_ERROR, "error")
        } else {
            match self.request.ensure_control_batch() {
                Ok(count) if count == self.checksums.len() => {
                    match self.handle_control_batch().await {
                        Ok(()) => (STATUS_HIT, "hit"),
                        Err(err) => {
                            debug!(
                                err = debug(err),
                                count = self.checksums.len(),
                                "batch chunk index control request failed"
                            );
                            (STATUS_ERROR, "error")
                        }
                    }
                }
                Ok(count) => {
                    debug!(
                        count = count,
                        actual = self.checksums.len(),
                        "invalid batch control request length"
                    );
                    (STATUS_ERROR, "error")
                }
                Err(err) => {
                    debug!(err = debug(err), "invalid batch control request header");
                    (STATUS_ERROR, "error")
                }
            }
        };
        self.ctx.metrics.record_request(
            request_type,
            status,
            self.ctx.transport,
            begin.elapsed().as_secs_f64() * 1000.0,
            0,
        );
        WireResponse {
            request_id: self.request.request_id,
            status: status_code,
            payload: Vec::new(),
        }
    }

    async fn handle_control_batch(&self) -> anyhow::Result<()> {
        let Some(index) = self
            .ctx
            .peer_client
            .and_then(|client| client.chunk_index.as_ref())
        else {
            return Ok(());
        };
        match self.request.message_type {
            MessageType::RegisterChunks => index.register_batch(&self.checksums).await,
            MessageType::UnregisterChunks => index.unregister_batch(&self.checksums).await,
            _ => Ok(()),
        }
    }
}

struct RequestHandler {
    chunk_db: Arc<ChunkDB>,
    peer_client: Option<Arc<PeerClient>>,
    metrics: Arc<ChunkServerMetrics>,
    transport: &'static str,
}

impl RequestHandler {
    fn new(
        chunk_db: Arc<ChunkDB>,
        peer_client: Option<Arc<PeerClient>>,
        metrics: Arc<ChunkServerMetrics>,
        transport: &'static str,
    ) -> Self {
        Self {
            chunk_db,
            peer_client,
            metrics,
            transport,
        }
    }

    async fn handle<S>(self, stream: S) -> io::Result<()>
    where
        S: RequestStream,
    {
        let (mut reader, mut writer) = stream.into_request_parts();
        let (response_tx, mut response_rx) = mpsc::unbounded_channel::<ResponseAction>();
        let chunk_db = Arc::clone(&self.chunk_db);
        let metrics = Arc::clone(&self.metrics);
        let transport = self.transport;
        let writer_task = tokio::spawn(async move {
            while let Some(action) = response_rx.recv().await {
                match action {
                    ResponseAction::Immediate(response) => {
                        writer.write_response(&response).await?;
                    }
                    ResponseAction::GetChunk {
                        request_id,
                        checksum,
                    } => {
                        let begin = Instant::now();
                        let result = block_in_place(|| {
                            chunk_db.with_chunk(&checksum, |data| {
                                Handle::current()
                                    .block_on(writer.write_get_chunk_hit(request_id, data))?;
                                Ok(data.len())
                            })
                        });
                        let elapsed_ms = begin.elapsed().as_secs_f64() * 1000.0;
                        match result {
                            Ok(Some(payload_len)) => metrics.record_request(
                                "get_chunk",
                                "hit",
                                transport,
                                elapsed_ms,
                                payload_len,
                            ),
                            Ok(None) => {
                                writer
                                    .write_response(&WireResponse {
                                        request_id,
                                        status: STATUS_MISS,
                                        payload: Vec::new(),
                                    })
                                    .await?;
                                metrics.record_request(
                                    "get_chunk",
                                    "miss",
                                    transport,
                                    elapsed_ms,
                                    0,
                                );
                            }
                            Err(err) => {
                                debug!(err = debug(&err), "get chunk failed");
                                writer
                                    .write_response(&WireResponse {
                                        request_id,
                                        status: STATUS_ERROR,
                                        payload: Vec::new(),
                                    })
                                    .await?;
                                metrics.record_request(
                                    "get_chunk",
                                    "error",
                                    transport,
                                    elapsed_ms,
                                    0,
                                );
                            }
                        }
                    }
                }
            }
            Ok::<(), io::Error>(())
        });

        loop {
            let request = match timeout(
                Duration::from_secs(SERVER_KEEPALIVE_IDLE_TIMEOUT_SECS),
                Request::read_from(&mut reader),
            )
            .await
            {
                Ok(Ok(request)) => request,
                Ok(Err(err)) if err.kind() == ErrorKind::UnexpectedEof => break,
                Ok(Err(err)) => {
                    drop(response_tx);
                    let _ = writer_task.await;
                    return Err(err);
                }
                Err(_) => break,
            };
            let checksums = match request.message_type {
                MessageType::RegisterChunks | MessageType::UnregisterChunks => {
                    let count = request.ensure_control_batch()?;
                    Some(read_checksum_batch(&mut reader, count).await?)
                }
                _ => None,
            };
            let inbound = InboundRequest { request, checksums };
            let chunk_db = Arc::clone(&self.chunk_db);
            let peer_client = self.peer_client.clone();
            let metrics = Arc::clone(&self.metrics);
            let transport = self.transport;
            let response_tx = response_tx.clone();
            // Requests are processed concurrently, so write order follows completion order.
            // Multiplexed clients must match responses by request_id instead of stream order.
            tokio::spawn(async move {
                let ctx = RequestContext {
                    chunk_db: &chunk_db,
                    peer_client: peer_client.as_deref(),
                    metrics: &metrics,
                    transport,
                };
                let response = Self::process_request(&ctx, inbound).await;
                let _ = response_tx.send(response);
            });
        }

        drop(response_tx);
        writer_task
            .await
            .map_err(|_| io::Error::other("writer task panicked"))??;
        Ok(())
    }

    async fn process_request(ctx: &RequestContext<'_>, inbound: InboundRequest) -> ResponseAction {
        let request = &inbound.request;
        match request.message_type {
            MessageType::GetChunk => GetRequest { ctx, request }.action(),
            MessageType::PrefetchChunk => {
                ResponseAction::Immediate(PrefetchRequest { ctx, request }.process().await)
            }
            MessageType::HealthCheck => {
                ResponseAction::Immediate(HealthCheckRequest { ctx, request }.process().await)
            }
            MessageType::RegisterChunk | MessageType::UnregisterChunk => {
                ResponseAction::Immediate(ControlRequest { ctx, request }.process().await)
            }
            MessageType::RegisterChunks | MessageType::UnregisterChunks => {
                ResponseAction::Immediate(
                    ControlBatchRequest {
                        ctx,
                        request,
                        checksums: inbound
                            .checksums
                            .expect("checksums must be present for batch control messages"),
                    }
                    .process()
                    .await,
                )
            }
        }
    }
}

struct IndexMaintainer {
    chunk_db: Arc<ChunkDB>,
    chunk_index: Arc<dyn ChunkIndex>,
}

impl IndexMaintainer {
    fn new(chunk_db: Arc<ChunkDB>, chunk_index: Arc<dyn ChunkIndex>) -> Self {
        Self {
            chunk_db,
            chunk_index,
        }
    }

    async fn read_chunk_batch(
        &self,
        cursor: Option<CheckSum>,
        batch_size: usize,
    ) -> anyhow::Result<Vec<CheckSum>> {
        let chunk_db = Arc::clone(&self.chunk_db);
        block_in_place(|| chunk_db.next_chunk_batch(cursor, batch_size))
    }

    async fn sync(self: Arc<Self>) {
        let mut total = 0usize;
        let mut cursor = None;
        loop {
            let batch = match self.read_chunk_batch(cursor, INDEX_SYNC_BATCH_SIZE).await {
                Ok(batch) => batch,
                Err(err) => {
                    warn!(
                        err = debug(err),
                        count = total,
                        "failed to register existing chunks in index"
                    );
                    return;
                }
            };
            if batch.is_empty() {
                info!(count = total, "registered existing chunks in index");
                return;
            }
            cursor = batch.last().copied();
            match self.chunk_index.sync_existing_chunks(&batch).await {
                Ok(count) => total += count,
                Err(err) => {
                    warn!(
                        err = debug(err),
                        count = total,
                        "failed to register existing chunks in index"
                    );
                    return;
                }
            }
        }
    }

    async fn refresh(&self, spread_over: Duration) -> anyhow::Result<usize> {
        match self.chunk_index.refresh_registered(spread_over).await? {
            Some(total) => Ok(total),
            None => {
                let mut total = 0usize;
                let mut cursor = None;
                loop {
                    let batch = self.read_chunk_batch(cursor, INDEX_SYNC_BATCH_SIZE).await?;
                    if batch.is_empty() {
                        return Ok(total);
                    }
                    cursor = batch.last().copied();
                    self.chunk_index.register_batch(&batch).await?;
                    total += batch.len();
                }
            }
        }
    }

    async fn repair(&self, repair_cursor: &mut Option<CheckSum>) -> anyhow::Result<Option<usize>> {
        let batch = self
            .read_chunk_batch(*repair_cursor, INDEX_REPAIR_BATCH_SIZE)
            .await?;
        if batch.is_empty() {
            *repair_cursor = None;
            return Ok(None);
        }
        *repair_cursor = batch.last().copied();
        self.chunk_index.repair_missing_owners(&batch).await
    }

    async fn run_refresh_loop(
        self: Arc<Self>,
        refresh_interval: Duration,
        shutdown: ShutdownHandle,
    ) {
        let mut repair_cursor = None;
        let jitter = {
            let mut rng = rand::thread_rng();
            let max_jitter_ms = refresh_interval.as_millis().min(u128::from(u64::MAX)) as u64;
            Duration::from_millis(rng.gen_range(0..max_jitter_ms))
        };
        if !jitter.is_zero() {
            tokio::select! {
                _ = shutdown.wait() => return,
                _ = tokio::time::sleep(jitter) => {}
            }
        }
        let mut ticker = interval(refresh_interval);
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = shutdown.wait() => break,
                _ = ticker.tick() => {
                    match self.refresh(refresh_interval).await {
                        Ok(total) => info!(count = total, "refreshed chunk index registrations"),
                        Err(err) => warn!(
                            err = debug(err),
                            "failed to refresh chunk index registrations"
                        ),
                    }

                    match self.repair(&mut repair_cursor).await {
                        Ok(Some(repaired)) if repaired > 0 => {
                            info!(count = repaired, "repaired missing chunk index registrations");
                        }
                        Ok(_) => {}
                        Err(err) => warn!(
                            err = debug(err),
                            "failed to repair chunk index registrations"
                        ),
                    }
                }
            }
        }
    }
}

impl ChunkServer {
    #[cfg(all(test, target_os = "linux"))]
    async fn refresh_index(
        chunk_db: Arc<ChunkDB>,
        chunk_index: Arc<dyn ChunkIndex>,
        spread_over: Duration,
    ) -> anyhow::Result<usize> {
        IndexMaintainer::new(chunk_db, chunk_index)
            .refresh(spread_over)
            .await
    }

    fn run_index_sync(&self) {
        let Some(peer_client) = &self.peer_client else {
            return;
        };
        let Some(chunk_index) = peer_client.chunk_index.clone() else {
            return;
        };
        let maintainer = Arc::new(IndexMaintainer::new(
            Arc::clone(&self.chunk_db),
            chunk_index,
        ));
        self.runtime.spawn(maintainer.sync());
    }

    fn run_index_refresh(&self, shutdown: ShutdownHandle) -> Option<JoinHandle<()>> {
        let peer_client = self.peer_client.as_ref()?;
        let chunk_index = peer_client.chunk_index.clone()?;
        let refresh_interval = chunk_index.refresh_interval()?;
        let maintainer = Arc::new(IndexMaintainer::new(
            Arc::clone(&self.chunk_db),
            chunk_index,
        ));
        Some(
            self.runtime
                .spawn(maintainer.run_refresh_loop(refresh_interval, shutdown)),
        )
    }
}

impl Drop for ChunkServer {
    fn drop(&mut self) {
        self.shutdown.shutdown();
        if let Some(peer_client) = &self.peer_client {
            peer_client.discovery.shutdown();
        }
        let _ = fs::remove_file(&self.unix_socket_path);
    }
}

struct RedisDiscoveryWorker {
    client: redis::Client,
    conn: Option<MultiplexedConnection>,
    key: String,
    value: String,
    advertise_addr: SocketAddr,
    peers: Arc<RwLock<Vec<SocketAddr>>>,
    shutdown: ShutdownHandle,
}

impl RedisDiscoveryWorker {
    fn new(
        url: &str,
        advertise_addr: SocketAddr,
        node_id: &str,
        peers: Arc<RwLock<Vec<SocketAddr>>>,
        shutdown: ShutdownHandle,
    ) -> anyhow::Result<Self> {
        let client = redis::Client::open(url).context("failed to create redis discovery client")?;
        Ok(Self {
            client,
            conn: None,
            key: format!("distill-fs:peers:{node_id}"),
            value: advertise_addr.to_string(),
            advertise_addr,
            peers,
            shutdown,
        })
    }

    async fn run(mut self) {
        let mut ticker = interval(Duration::from_secs(DISCOVERY_REFRESH_SECS));
        loop {
            match self.refresh_peers().await {
                Ok(new_peers) => *self.peers.write().unwrap() = new_peers,
                Err(err) => {
                    self.conn = None;
                    debug!(err = debug(err), "failed to refresh redis peers");
                }
            }

            tokio::select! {
                _ = self.shutdown.wait() => {
                    if let Err(err) = self.delete_peer().await {
                        debug!(err = debug(err), "failed to delete redis peer");
                    }
                    return;
                }
                _ = ticker.tick() => {}
            }
        }
    }

    async fn refresh_peers(&mut self) -> anyhow::Result<Vec<SocketAddr>> {
        let mut conn = self.ensure_conn().await?;
        let _: () = conn
            .set_ex(&self.key, &self.value, DISCOVERY_TTL_SECS)
            .await?;
        let keys: Vec<String> = conn.keys("distill-fs:peers:*").await?;
        let vals: Vec<Option<String>> = if keys.is_empty() {
            Vec::new()
        } else {
            redis::cmd("MGET").arg(&keys).query_async(&mut conn).await?
        };
        Ok(vals
            .into_iter()
            .flatten()
            .filter_map(|addr| addr.parse::<SocketAddr>().ok())
            .filter(|addr| *addr != self.advertise_addr)
            .collect())
    }

    async fn delete_peer(&mut self) -> anyhow::Result<()> {
        let mut conn = self.ensure_conn().await?;
        let _: usize = conn.del(&self.key).await?;
        Ok(())
    }

    async fn ensure_conn(&mut self) -> anyhow::Result<MultiplexedConnection> {
        if let Some(conn) = self.conn.as_ref() {
            return Ok(conn.clone());
        }
        let conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .context("failed to connect to redis discovery")?;
        self.conn = Some(conn.clone());
        Ok(conn)
    }
}

pub fn default_chunk_server_socket<P: AsRef<Path>>(chunk_db_dir: P) -> PathBuf {
    chunk_db_dir.as_ref().join("chunkserver.sock")
}

pub fn default_node_id() -> String {
    std::env::var("HOSTNAME").unwrap_or_else(|_| format!("distill-fs-{}", process::id()))
}

pub fn parse_peer_addrs(peer_addrs: &str) -> anyhow::Result<Vec<SocketAddr>> {
    let mut peers = Vec::new();
    for addr in peer_addrs
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        peers.push(
            addr.parse()
                .with_context(|| format!("invalid peer addr: {addr}"))?,
        );
    }
    Ok(peers)
}

pub fn build_discovery(
    runtime: PeerRuntime,
    peer_discovery: &str,
    peer_addrs: &str,
    advertise_addr: SocketAddr,
    node_id: &str,
) -> anyhow::Result<Arc<dyn PeerDiscovery>> {
    if peer_discovery.starts_with("redis://") {
        return Ok(Arc::new(RedisDiscovery::new(
            runtime,
            peer_discovery,
            advertise_addr,
            node_id,
        )?));
    }
    if !peer_discovery.trim().is_empty() {
        let _ = (runtime, advertise_addr, node_id);
        bail!(
            "unsupported peer discovery configuration: {}",
            peer_discovery
        );
    }
    Ok(Arc::new(StaticPeers::new(parse_peer_addrs(peer_addrs)?)))
}

#[cfg(all(test, target_os = "linux"))]
mod tests;
