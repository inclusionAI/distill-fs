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

use super::slow_write_txn;
use crate::utils::now_epoch_secs;
use bytemuck::{Pod, Zeroable};
use heed::{BoxedError, BytesDecode, BytesEncode, Database, Env, EnvOpenOptions, MdbError, RwTxn};
use heed_types::Bytes;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram, Meter};
use opentelemetry::KeyValue;
use serde_json::{json, Value};
use sha2::Digest;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::io::{self, ErrorKind, Write};
use std::ops::Bound::{Excluded, Unbounded};
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

pub const CHUNK_DB_NAME: &str = "data";
const ACCESS_DB_NAME: &str = "chunk_access";
const ACCESS_INDEX_DB_NAME: &str = "chunk_access_index";
pub const MAX_DBS: u32 = 16;
pub const LMDB_MAX_READERS: u32 = 4096;
#[cfg(all(target_os = "linux", not(test)))]
pub const CHUNK_DB_SIZE: usize = 100 * 1024 * 1024 * 1024_usize;
// Keep test/non-Linux LMDB maps small enough for local development environments.
// This intentionally differs from production to avoid EPERM/oversized map issues.
#[cfg(any(test, not(target_os = "linux")))]
pub const CHUNK_DB_SIZE: usize = 512 * 1024 * 1024_usize;
const GC_BATCH: usize = 128;
const ACCESS_BATCH_SIZE: usize = 1024;
const ADD_CHUNKS_BATCH_SIZE: usize = 16;
const ACCESS_REFRESH_INTERVAL_SECS: u64 = 300;
const ACCESS_UPDATE_MIN_INTERVAL_SECS: u64 = 60;
const DEFAULT_GC_EXPIRE_SECS: u64 = 24 * 60 * 60;
const GC_HIGH_WATERMARK: f64 = 0.90;
const GC_LOW_WATERMARK: f64 = 0.80;

#[derive(Debug)]
struct ChunkDbMetrics {
    get_total: Counter<u64>,
    get_duration: Histogram<f64>,
    get_bytes: Counter<u64>,
    add_total: Counter<u64>,
    add_bytes: Counter<u64>,
    touch_total: Counter<u64>,
    gc_removed_total: Counter<u64>,
    gc_duration: Histogram<f64>,
}

impl ChunkDbMetrics {
    fn new(meter: &Meter) -> Self {
        Self {
            get_total: meter
                .u64_counter("distill_fs.chunkdb.get_total")
                .with_description("Total ChunkDB read attempts")
                .init(),
            get_duration: meter
                .f64_histogram("distill_fs.chunkdb.get_duration_ms")
                .with_description("ChunkDB read duration")
                .with_unit("ms")
                .init(),
            get_bytes: meter
                .u64_counter("distill_fs.chunkdb.get_bytes")
                .with_description("Bytes read from ChunkDB")
                .with_unit("By")
                .init(),
            add_total: meter
                .u64_counter("distill_fs.chunkdb.add_total")
                .with_description("Total ChunkDB add attempts")
                .init(),
            add_bytes: meter
                .u64_counter("distill_fs.chunkdb.add_bytes")
                .with_description("Bytes added to ChunkDB")
                .with_unit("By")
                .init(),
            touch_total: meter
                .u64_counter("distill_fs.chunkdb.touch_total")
                .with_description("Total ChunkDB touch attempts")
                .init(),
            gc_removed_total: meter
                .u64_counter("distill_fs.chunkdb.gc_removed_total")
                .with_description("Chunks removed by ChunkDB GC")
                .init(),
            gc_duration: meter
                .f64_histogram("distill_fs.chunkdb.gc_duration_ms")
                .with_description("ChunkDB GC duration")
                .with_unit("ms")
                .init(),
        }
    }

    fn record_get(&self, result: &'static str, elapsed_ms: f64, bytes: usize) {
        let attrs = [KeyValue::new("result", result)];
        self.get_total.add(1, &attrs);
        self.get_duration.record(elapsed_ms, &attrs);
        if bytes > 0 {
            self.get_bytes.add(bytes as u64, &attrs);
        }
    }

    fn record_add(&self, result: &'static str, count: u64, bytes: u64) {
        let attrs = [KeyValue::new("result", result)];
        self.add_total.add(count, &attrs);
        if bytes > 0 {
            self.add_bytes.add(bytes, &attrs);
        }
    }

    fn record_touch(&self, result: &'static str) {
        self.touch_total.add(1, &[KeyValue::new("result", result)]);
    }

    fn record_gc(&self, mode: &'static str, result: &'static str, elapsed_ms: f64, removed: usize) {
        let attrs = [KeyValue::new("mode", mode), KeyValue::new("result", result)];
        self.gc_duration.record(elapsed_ms, &attrs);
        if removed > 0 {
            self.gc_removed_total.add(removed as u64, &attrs);
        }
    }
}

pub trait ChunkIndexControl: Send + Sync {
    fn register_chunk(&self, checksum: &CheckSum) -> bool;
    fn register_chunks(&self, checksums: &[CheckSum]) -> bool {
        checksums
            .iter()
            .all(|checksum| self.register_chunk(checksum))
    }
    fn unregister_chunk(&self, checksum: &CheckSum) -> bool;
    fn unregister_chunks(&self, checksums: &[CheckSum]) -> bool {
        checksums
            .iter()
            .all(|checksum| self.unregister_chunk(checksum))
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum CheckSumMethod {
    Sha256 = 0,
    Blake3 = 1,
    Unknown = 2,
}

impl From<u8> for CheckSumMethod {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Sha256,
            1 => Self::Blake3,
            _ => Self::Unknown,
        }
    }
}

impl From<CheckSumMethod> for u8 {
    fn from(value: CheckSumMethod) -> Self {
        value as u8
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct CheckSum {
    pub(crate) raw: [u8; 32],
    pub(crate) method: CheckSumMethod,
}

impl Display for CheckSum {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}:{}", self.method, hex::encode(self.raw))
    }
}

impl Ord for CheckSum {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.method != other.method {
            self.method.cmp(&other.method)
        } else {
            self.raw.cmp(&other.raw)
        }
    }
}

impl PartialOrd for CheckSum {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl CheckSum {
    pub fn empty() -> Self {
        Self {
            raw: [0_u8; 32],
            method: CheckSumMethod::Unknown,
        }
    }

    pub fn new(raw: &[u8], method: CheckSumMethod) -> io::Result<Self> {
        let mut cs = Self {
            raw: [0_u8; 32],
            method,
        };
        if cs.raw.len() != raw.len() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "Invalid checksum data len",
            ));
        }
        cs.raw.copy_from_slice(raw);
        Ok(cs)
    }

    pub fn from_data(data: &[u8], method: CheckSumMethod) -> Self {
        let raw: [u8; 32] = match method {
            CheckSumMethod::Sha256 => sha2::Sha256::new().chain_update(data).finalize().into(),
            CheckSumMethod::Blake3 => blake3::hash(data).into(),
            _ => blake3::hash(data).into(),
        };
        Self::new(&raw, method).unwrap()
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Pod, Zeroable)]
#[repr(C)]
pub(crate) struct CheckSumOnDisk {
    raw: [u8; 32],
    method: u8,
    reserved: [u8; 7],
}

impl CheckSumOnDisk {
    pub(crate) fn is_valid(&self) -> bool {
        self.method < 2
    }
}

impl From<CheckSum> for CheckSumOnDisk {
    fn from(value: CheckSum) -> Self {
        let method = value.method.into();
        Self {
            raw: value.raw,
            method,
            reserved: [0_u8; 7],
        }
    }
}

impl From<CheckSumOnDisk> for CheckSum {
    fn from(value: CheckSumOnDisk) -> Self {
        let method = value.method.into();
        Self {
            raw: value.raw,
            method,
        }
    }
}

impl<'a> BytesEncode<'a> for CheckSumOnDisk {
    type EItem = CheckSumOnDisk;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, BoxedError> {
        Ok(Cow::Borrowed(bytemuck::bytes_of(item)))
    }
}

impl<'a> BytesDecode<'a> for CheckSumOnDisk {
    type DItem = &'a CheckSumOnDisk;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        Ok(bytemuck::from_bytes(bytes))
    }
}

impl Ord for CheckSumOnDisk {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.method != other.method {
            self.method.cmp(&other.method)
        } else {
            self.raw.cmp(&other.raw)
        }
    }
}

impl PartialOrd for CheckSumOnDisk {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
struct AccessTime {
    secs: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GcDeleteResult {
    pub removed: usize,
    pub checksums: Vec<CheckSum>,
}

impl<'a> BytesEncode<'a> for AccessTime {
    type EItem = AccessTime;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, BoxedError> {
        Ok(Cow::Owned(item.secs.to_be_bytes().to_vec()))
    }
}

impl<'a> BytesDecode<'a> for AccessTime {
    type DItem = AccessTime;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        if bytes.len() != 8 {
            return Err("Invalid access time length".into());
        }
        Ok(AccessTime {
            secs: u64::from_be_bytes(bytes.try_into()?),
        })
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
struct AccessKey {
    last_access: u64,
    cs: CheckSumOnDisk,
}

impl AccessKey {
    fn new(last_access: u64, cs: CheckSumOnDisk) -> Self {
        Self { last_access, cs }
    }
}

impl Ord for AccessKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.last_access.cmp(&other.last_access) {
            Ordering::Equal => self.cs.cmp(&other.cs),
            other => other,
        }
    }
}

impl PartialOrd for AccessKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> BytesEncode<'a> for AccessKey {
    type EItem = AccessKey;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, BoxedError> {
        let mut key = Vec::with_capacity(8 + std::mem::size_of::<CheckSumOnDisk>());
        key.extend_from_slice(&item.last_access.to_be_bytes());
        key.extend_from_slice(bytemuck::bytes_of(&item.cs));
        Ok(Cow::Owned(key))
    }
}

impl<'a> BytesDecode<'a> for AccessKey {
    type DItem = AccessKey;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        let expected = 8 + std::mem::size_of::<CheckSumOnDisk>();
        if bytes.len() != expected {
            return Err("Invalid access key length".into());
        }
        let last_access = u64::from_be_bytes(bytes[0..8].try_into()?);
        let cs = *bytemuck::from_bytes(&bytes[8..expected]);
        Ok(AccessKey { last_access, cs })
    }
}

type ChunkDataDb = Database<CheckSumOnDisk, Bytes>;
type ChunkAccessDb = Database<CheckSumOnDisk, AccessTime>;
type ChunkAccessIndexDb = Database<AccessKey, Bytes>;

pub struct ChunkDB {
    env: Env,
    data_db: ChunkDataDb,
    #[cfg_attr(not(test), allow(dead_code))]
    access_db: ChunkAccessDb,
    access_index: ChunkAccessIndexDb,
    #[allow(dead_code)] // cloned into WriterThread during construction
    index_ctl: Option<Arc<dyn ChunkIndexControl>>,
    metrics: ChunkDbMetrics,
    writer_channel: Arc<WriterChannel>,
    writer_handle: Option<thread::JoinHandle<()>>,
}

impl std::fmt::Debug for ChunkDB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkDB").finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Writer thread — serialises all write_txn calls with priority ordering.
// ---------------------------------------------------------------------------

use std::collections::VecDeque;
use std::sync::mpsc;

enum WriteRequest {
    AddChunk {
        cs: CheckSum,
        data: Vec<u8>,
        reply: mpsc::SyncSender<anyhow::Result<()>>,
    },
    AddChunksBatch {
        chunks: Vec<(CheckSum, Vec<u8>)>,
        reply: mpsc::SyncSender<anyhow::Result<()>>,
    },
    DeleteKeys {
        keys: Vec<AccessKey>,
        reply: mpsc::SyncSender<anyhow::Result<GcDeleteResult>>,
    },
}

#[derive(Default)]
struct WriteQueue {
    /// Priority 2: add_chunk / add_chunks (highest)
    high: VecDeque<WriteRequest>,
    /// Priority 1: delete_keys (GC)
    medium: VecDeque<WriteRequest>,
    /// Priority 0: access updates (lowest, coalesced via HashSet)
    access_pending: HashSet<CheckSumOnDisk>,
    stop: bool,
}

struct WriterChannel {
    inner: Mutex<WriteQueue>,
    cond: Condvar,
}

impl Default for WriterChannel {
    fn default() -> Self {
        Self {
            inner: Mutex::new(WriteQueue::default()),
            cond: Condvar::new(),
        }
    }
}

impl WriterChannel {
    fn submit_high(&self, req: WriteRequest) {
        let mut q = self.inner.lock().unwrap();
        q.high.push_back(req);
        drop(q);
        self.cond.notify_one();
    }

    fn submit_medium(&self, req: WriteRequest) {
        let mut q = self.inner.lock().unwrap();
        q.medium.push_back(req);
        drop(q);
        self.cond.notify_one();
    }

    fn enqueue_access(&self, cs: CheckSumOnDisk) {
        let mut q = self.inner.lock().unwrap();
        let inserted = q.access_pending.insert(cs);
        drop(q);
        if inserted {
            self.cond.notify_one();
        }
    }

    fn enqueue_access_many<I: IntoIterator<Item = CheckSumOnDisk>>(&self, checksums: I) {
        let mut q = self.inner.lock().unwrap();
        let mut any = false;
        for cs in checksums {
            if q.access_pending.insert(cs) {
                any = true;
            }
        }
        drop(q);
        if any {
            self.cond.notify_one();
        }
    }
}

struct WriterThread {
    env: Env,
    data_db: ChunkDataDb,
    access_db: ChunkAccessDb,
    access_index: ChunkAccessIndexDb,
    index_ctl: Option<Arc<dyn ChunkIndexControl>>,
    channel: Arc<WriterChannel>,
}

impl WriterThread {
    fn run(self) {
        let refresh = Duration::from_secs(ACCESS_REFRESH_INTERVAL_SECS);
        loop {
            // --- drain by priority ---
            let (high, medium, access, should_stop) = {
                let mut q = self.channel.inner.lock().unwrap();
                if q.high.is_empty()
                    && q.medium.is_empty()
                    && q.access_pending.is_empty()
                    && !q.stop
                {
                    let (guard, _) = self.channel.cond.wait_timeout(q, refresh).unwrap();
                    q = guard;
                }
                let high: Vec<WriteRequest> = q.high.drain(..).collect();
                let medium: Vec<WriteRequest> = q.medium.drain(..).collect();
                let access: Vec<CheckSumOnDisk> = q.access_pending.drain().collect();
                let stop = q.stop;
                (high, medium, access, stop)
            };

            // --- process high priority: add_chunk / add_chunks ---
            self.process_high(high);

            // --- process medium priority: delete_keys ---
            self.process_medium(medium);

            // --- process low priority: access updates ---
            self.process_access_updates(&access);

            if should_stop {
                break;
            }
        }
    }

    fn process_high(&self, requests: Vec<WriteRequest>) {
        for req in requests {
            match req {
                WriteRequest::AddChunk { cs, data, reply } => {
                    let result = self.do_add_chunk(&cs, &data);
                    let _ = reply.send(result);
                }
                WriteRequest::AddChunksBatch { chunks, reply } => {
                    let result = self.do_add_chunks_batch(&chunks);
                    let _ = reply.send(result);
                }
                _ => unreachable!(),
            }
        }
    }

    fn process_medium(&self, requests: Vec<WriteRequest>) {
        for req in requests {
            match req {
                WriteRequest::DeleteKeys { keys, reply } => {
                    let result = self.do_delete_keys(keys);
                    let _ = reply.send(result);
                }
                _ => unreachable!(),
            }
        }
    }

    fn process_access_updates(&self, checksums: &[CheckSumOnDisk]) {
        if checksums.is_empty() {
            return;
        }
        let now = now_epoch_secs();
        // Always process the first batch unconditionally.
        let first_end = checksums.len().min(ACCESS_BATCH_SIZE);
        self.commit_access_batch(&checksums[..first_end], now);
        let mut offset = first_end;
        while offset < checksums.len() {
            // Between batches, check if higher-priority work has arrived.
            // If so, re-enqueue ALL remaining checksums and yield.
            {
                let q = self.channel.inner.lock().unwrap();
                if !q.high.is_empty() || !q.medium.is_empty() {
                    drop(q);
                    self.channel
                        .enqueue_access_many(checksums[offset..].iter().copied());
                    return;
                }
            }
            let batch_end = (offset + ACCESS_BATCH_SIZE).min(checksums.len());
            self.commit_access_batch(&checksums[offset..batch_end], now);
            offset = batch_end;
        }
    }

    fn commit_access_batch(&self, batch: &[CheckSumOnDisk], now: u64) {
        match slow_write_txn(&self.env, "WriterThread::access_update") {
            Ok(mut wtxn) => {
                for cs in batch {
                    if let Err(e) = ChunkDB::update_access(
                        &self.access_db,
                        &self.access_index,
                        &mut wtxn,
                        cs,
                        now,
                    ) {
                        warn!(err = debug(e), "Failed to update chunk access time.");
                    }
                }
                if let Err(e) = wtxn.commit() {
                    warn!(err = debug(e), "Failed to commit chunk access updates.");
                }
            }
            Err(e) => {
                warn!(err = debug(e), "Failed to start chunk access update txn.");
            }
        }
    }

    fn do_add_chunk(&self, cs: &CheckSum, data: &[u8]) -> anyhow::Result<()> {
        let cs_on_disk = CheckSumOnDisk::from(*cs);
        loop {
            let mut wtxn = slow_write_txn(&self.env, "WriterThread::add_chunk")?;
            match self
                .data_db
                .get_or_put_reserved(&mut wtxn, &cs_on_disk, data.len(), |reserved| {
                    reserved.write_all(data)
                }) {
                Ok(None) | Ok(Some(_)) => {
                    wtxn.commit()?;
                    if let Some(ctl) = &self.index_ctl {
                        let _ = ctl.register_chunk(cs);
                    }
                    self.channel.enqueue_access(cs_on_disk);
                    return Ok(());
                }
                Err(heed::Error::Mdb(MdbError::MapFull)) => {
                    drop(wtxn);
                    let removed = self.gc_lru_direct(GC_BATCH)?;
                    if removed == 0 {
                        return Err(heed::Error::Mdb(MdbError::MapFull).into());
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    fn do_add_chunks_batch(&self, chunks: &[(CheckSum, Vec<u8>)]) -> anyhow::Result<()> {
        for batch in chunks.chunks(ADD_CHUNKS_BATCH_SIZE) {
            self.do_add_chunks_sub_batch(batch)?;
        }
        Ok(())
    }

    fn do_add_chunks_sub_batch(&self, batch: &[(CheckSum, Vec<u8>)]) -> anyhow::Result<()> {
        loop {
            let mut wtxn = slow_write_txn(&self.env, "WriterThread::add_chunks")?;
            let mut map_full = false;
            for (cs, data) in batch {
                let cs_on_disk = CheckSumOnDisk::from(*cs);
                match self.data_db.get_or_put_reserved(
                    &mut wtxn,
                    &cs_on_disk,
                    data.len(),
                    |reserved| reserved.write_all(data),
                ) {
                    Ok(None) | Ok(Some(_)) => {}
                    Err(heed::Error::Mdb(MdbError::MapFull)) => {
                        map_full = true;
                        break;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            if !map_full {
                wtxn.commit()?;
                // Register immediately after commit so that even if a later
                // sub-batch fails, these already-durable chunks are visible
                // in the chunk index.
                if let Some(ctl) = &self.index_ctl {
                    let checksums: Vec<CheckSum> = batch.iter().map(|(cs, _)| *cs).collect();
                    let _ = ctl.register_chunks(&checksums);
                }
                self.channel
                    .enqueue_access_many(batch.iter().map(|(cs, _)| CheckSumOnDisk::from(*cs)));
                return Ok(());
            } else {
                drop(wtxn);
                let removed = self.gc_lru_direct(GC_BATCH)?;
                if removed == 0 {
                    return Err(heed::Error::Mdb(MdbError::MapFull).into());
                }
            }
        }
    }

    fn do_delete_keys(&self, keys: Vec<AccessKey>) -> anyhow::Result<GcDeleteResult> {
        if keys.is_empty() {
            return Ok(GcDeleteResult {
                removed: 0,
                checksums: Vec::new(),
            });
        }
        let mut wtxn = slow_write_txn(&self.env, "WriterThread::delete_keys")?;
        let mut checksums = Vec::with_capacity(keys.len());
        for key in &keys {
            checksums.push(CheckSum::from(key.cs));
            if let Err(e) = self.data_db.delete(&mut wtxn, &key.cs) {
                warn!(err = debug(e), "Failed to delete chunk data.");
            }
            if let Err(e) = self.access_db.delete(&mut wtxn, &key.cs) {
                warn!(err = debug(e), "Failed to delete chunk access metadata.");
            }
            if let Err(e) = self.access_index.delete(&mut wtxn, key) {
                warn!(err = debug(e), "Failed to delete chunk access index.");
            }
        }
        wtxn.commit()?;
        Ok(GcDeleteResult {
            removed: keys.len(),
            checksums,
        })
    }

    /// GC directly within the writer thread (no channel round-trip) for MapFull handling.
    fn gc_lru_direct(&self, max_delete: usize) -> anyhow::Result<usize> {
        let rtxn = self.env.read_txn()?;
        let iter = self.access_index.iter(&rtxn)?;
        let mut targets = Vec::new();
        for it in iter {
            let (key, _) = it?;
            targets.push(key);
            if targets.len() >= max_delete {
                break;
            }
        }
        drop(rtxn);
        if targets.is_empty() {
            return Ok(0);
        }
        let checksums: Vec<CheckSum> = targets.iter().map(|k| CheckSum::from(k.cs)).collect();
        let mut wtxn = slow_write_txn(&self.env, "WriterThread::gc_lru_direct")?;
        for key in &targets {
            let _ = self.data_db.delete(&mut wtxn, &key.cs);
            let _ = self.access_db.delete(&mut wtxn, &key.cs);
            let _ = self.access_index.delete(&mut wtxn, key);
        }
        wtxn.commit()?;
        // Unregister evicted chunks from the chunk index so peers stop
        // routing fetches to this node for data that no longer exists.
        if let Some(ctl) = &self.index_ctl {
            if !ctl.unregister_chunks(&checksums) {
                debug!(
                    count = checksums.len(),
                    "Failed to unregister chunks from index during MapFull GC."
                );
            }
        }
        Ok(targets.len())
    }
}

impl ChunkDB {
    pub fn new<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        Self::new_with_index_ctl(path, None)
    }

    pub fn new_with_index_ctl<P: AsRef<Path>>(
        path: P,
        index_ctl: Option<Arc<dyn ChunkIndexControl>>,
    ) -> anyhow::Result<Self> {
        let meter = global::meter("distill_fs.chunkdb");
        let open_begin = Instant::now();
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(CHUNK_DB_SIZE)
                .max_readers(LMDB_MAX_READERS)
                .max_dbs(MAX_DBS)
                .open(path)?
        };
        let stale_readers = env.clear_stale_readers()?;
        let actual_max_readers = env.max_readers();
        info!(
            max_readers = actual_max_readers,
            stale_readers_cleared = stale_readers,
            elapsed = ?open_begin.elapsed(),
            "ChunkDB opened LMDB environment"
        );
        if actual_max_readers < LMDB_MAX_READERS {
            warn!(
                requested_max_readers = LMDB_MAX_READERS,
                actual_max_readers,
                "ChunkDB LMDB max_readers is lower than requested; restart all processes sharing this env to apply the new limit."
            );
        }
        let (data_db, access_db, access_index) = Self::open_or_create_databases(&env)?;
        let writer_channel = Arc::new(WriterChannel::default());
        let writer_handle = Self::spawn_writer_thread(
            env.clone(),
            data_db,
            access_db,
            access_index,
            index_ctl.clone(),
            Arc::clone(&writer_channel),
        );
        Ok(Self {
            env,
            data_db,
            access_db,
            access_index,
            index_ctl,
            metrics: ChunkDbMetrics::new(&meter),
            writer_channel,
            writer_handle: Some(writer_handle),
        })
    }

    fn open_or_create_databases(
        env: &Env,
    ) -> anyhow::Result<(ChunkDataDb, ChunkAccessDb, ChunkAccessIndexDb)> {
        // Try read transaction first to avoid blocking on the write mutex
        // when another process (e.g. GC) holds a write transaction.
        let t = std::time::Instant::now();
        let rtxn = env.read_txn()?;
        let data_db = env.open_database::<CheckSumOnDisk, Bytes>(&rtxn, Some(CHUNK_DB_NAME))?;
        let access_db =
            env.open_database::<CheckSumOnDisk, AccessTime>(&rtxn, Some(ACCESS_DB_NAME))?;
        let access_index =
            env.open_database::<AccessKey, Bytes>(&rtxn, Some(ACCESS_INDEX_DB_NAME))?;
        rtxn.commit()?;

        if let (Some(data_db), Some(access_db), Some(access_index)) =
            (data_db, access_db, access_index)
        {
            info!(
                "ChunkDB opened existing databases via read txn in {:?}",
                t.elapsed()
            );
            return Ok((data_db, access_db, access_index));
        }

        // Databases don't exist yet, create them with a write transaction.
        info!("ChunkDB databases not found, creating via write txn");
        let mut wtxn = slow_write_txn(env, "ChunkDB::open_or_create_databases")?;
        let data_db = env.create_database(&mut wtxn, Some(CHUNK_DB_NAME))?;
        let access_db = env.create_database(&mut wtxn, Some(ACCESS_DB_NAME))?;
        let access_index = env.create_database(&mut wtxn, Some(ACCESS_INDEX_DB_NAME))?;
        wtxn.commit()?;
        info!(
            "ChunkDB created databases via write txn in {:?}",
            t.elapsed()
        );
        Ok((data_db, access_db, access_index))
    }

    pub fn clear_stale_readers(&self) -> anyhow::Result<usize> {
        Ok(self.env.clear_stale_readers()?)
    }

    pub fn has_chunk(&self, cs: &CheckSum) -> anyhow::Result<bool> {
        let rtxn = self.env.read_txn()?;
        let cs_on_disk = CheckSumOnDisk::from(*cs);
        Ok(self.data_db.get(&rtxn, &cs_on_disk)?.is_some())
    }

    pub fn add_chunk(&self, cs: &CheckSum, data: Vec<u8>) -> anyhow::Result<()> {
        let data_len = data.len() as u64;
        let (tx, rx) = mpsc::sync_channel(1);
        self.writer_channel.submit_high(WriteRequest::AddChunk {
            cs: *cs,
            data,
            reply: tx,
        });
        let result: anyhow::Result<()> = rx
            .recv()
            .map_err(|_| anyhow::anyhow!("Writer thread dropped"))?;
        // Registration is done inside the writer thread immediately after commit.
        let status = if result.is_ok() { "ok" } else { "error" };
        self.metrics
            .record_add(status, 1, if result.is_ok() { data_len } else { 0 });
        result
    }

    pub fn add_chunks(&self, chunks: Vec<(CheckSum, Vec<u8>)>) -> anyhow::Result<()> {
        if chunks.is_empty() {
            return Ok(());
        }
        let total_bytes: u64 = chunks.iter().map(|(_, data)| data.len() as u64).sum();
        let chunk_count = chunks.len() as u64;

        let (tx, rx) = mpsc::sync_channel(1);
        self.writer_channel
            .submit_high(WriteRequest::AddChunksBatch { chunks, reply: tx });
        let result: anyhow::Result<()> = rx
            .recv()
            .map_err(|_| anyhow::anyhow!("Writer thread dropped"))?;
        // Registration is done inside the writer thread per sub-batch after commit.
        let status = if result.is_ok() { "ok" } else { "error" };
        self.metrics.record_add(
            status,
            chunk_count,
            if result.is_ok() { total_bytes } else { 0 },
        );
        result
    }

    pub fn with_chunk<F, T>(&self, cs: &CheckSum, f: F) -> anyhow::Result<Option<T>>
    where
        F: FnOnce(&[u8]) -> io::Result<T>,
    {
        let begin = Instant::now();
        let rtxn = self.env.read_txn()?;
        let cs_on_disk = CheckSumOnDisk::from(*cs);
        let result = match self.data_db.get(&rtxn, &cs_on_disk)? {
            Some(chunk) => {
                let len = chunk.len();
                let res = f(chunk)?;
                drop(rtxn);
                self.enqueue_access_update(cs_on_disk);
                Ok(Some((res, len)))
            }
            None => Ok(None),
        };
        match result {
            Ok(Some((value, bytes))) => {
                self.metrics
                    .record_get("hit", begin.elapsed().as_secs_f64() * 1000.0, bytes);
                Ok(Some(value))
            }
            Ok(None) => {
                self.metrics
                    .record_get("miss", begin.elapsed().as_secs_f64() * 1000.0, 0);
                Ok(None)
            }
            Err(err) => {
                self.metrics
                    .record_get("error", begin.elapsed().as_secs_f64() * 1000.0, 0);
                Err(err)
            }
        }
    }

    pub fn with_chunk_range<F, T>(
        &self,
        cs: &CheckSum,
        start: usize,
        len: usize,
        f: F,
    ) -> anyhow::Result<Option<T>>
    where
        F: FnOnce(&[u8]) -> io::Result<T>,
    {
        let begin = Instant::now();
        let rtxn = self.env.read_txn()?;
        let cs_on_disk = CheckSumOnDisk::from(*cs);
        let result = match self.data_db.get(&rtxn, &cs_on_disk)? {
            Some(b) => {
                let end = start + len;
                if end > b.len() {
                    let err =
                        Err(io::Error::new(ErrorKind::InvalidData, "Chunk range invalid").into());
                    drop(rtxn);
                    self.metrics
                        .record_get("error", begin.elapsed().as_secs_f64() * 1000.0, 0);
                    return err;
                }
                let res = f(&b[start..end])?;
                drop(rtxn);
                self.enqueue_access_update(cs_on_disk);
                Ok(Some((res, len)))
            }
            None => Ok(None),
        };
        match result {
            Ok(Some((value, bytes))) => {
                self.metrics
                    .record_get("hit", begin.elapsed().as_secs_f64() * 1000.0, bytes);
                Ok(Some(value))
            }
            Ok(None) => {
                self.metrics
                    .record_get("miss", begin.elapsed().as_secs_f64() * 1000.0, 0);
                Ok(None)
            }
            Err(err) => {
                self.metrics
                    .record_get("error", begin.elapsed().as_secs_f64() * 1000.0, 0);
                Err(err)
            }
        }
    }

    pub fn get_chunk(&self, cs: &CheckSum) -> anyhow::Result<Option<Vec<u8>>> {
        self.with_chunk(cs, |chunk| Ok(chunk.to_vec()))
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub fn list_all_chunks(&self) -> anyhow::Result<Vec<CheckSum>> {
        let mut checksums = Vec::new();
        let mut cursor = None;
        loop {
            let batch = self.next_chunk_batch(cursor, 1024)?;
            if batch.is_empty() {
                return Ok(checksums);
            }
            cursor = batch.last().copied();
            checksums.extend_from_slice(&batch);
        }
    }

    pub fn next_chunk_batch(
        &self,
        cursor: Option<CheckSum>,
        batch_size: usize,
    ) -> anyhow::Result<Vec<CheckSum>> {
        let batch_size = batch_size.max(1);
        let rtxn = self.env.read_txn()?;
        let mut batch = Vec::with_capacity(batch_size);
        match cursor.map(CheckSumOnDisk::from) {
            Some(last) => {
                let range = (Excluded(last), Unbounded);
                for item in self.data_db.range(&rtxn, &range)?.lazily_decode_data() {
                    let (cs, _) = item?;
                    if cs.is_valid() {
                        batch.push((*cs).into());
                        if batch.len() >= batch_size {
                            break;
                        }
                    }
                }
            }
            None => {
                for item in self.data_db.iter(&rtxn)?.lazily_decode_data() {
                    let (cs, _) = item?;
                    if cs.is_valid() {
                        batch.push((*cs).into());
                        if batch.len() >= batch_size {
                            break;
                        }
                    }
                }
            }
        }
        Ok(batch)
    }

    pub fn gc_expired(&self, timeout: Duration) -> anyhow::Result<GcDeleteResult> {
        let begin = Instant::now();
        let cutoff = now_epoch_secs().saturating_sub(timeout.as_secs());
        let rtxn = self.env.read_txn()?;
        let iter = self.access_index.iter(&rtxn)?;
        let mut expired = Vec::new();
        for it in iter {
            let (key, _) = it?;
            if key.last_access >= cutoff {
                break;
            }
            expired.push(key);
        }
        drop(rtxn);

        // Delete in batches to avoid holding the write lock for too long.
        let mut total_removed = 0;
        let mut all_checksums = Vec::new();
        for batch in expired.chunks(GC_BATCH) {
            let result = self.delete_keys(batch.to_vec())?;
            total_removed += result.removed;
            all_checksums.extend(result.checksums);
        }

        let result = GcDeleteResult {
            removed: total_removed,
            checksums: all_checksums,
        };
        self.metrics.record_gc(
            "expired",
            "ok",
            begin.elapsed().as_secs_f64() * 1000.0,
            result.removed,
        );
        Ok(result)
    }

    pub fn gc_lru(&self, max_delete: usize) -> anyhow::Result<GcDeleteResult> {
        let begin = Instant::now();
        let rtxn = self.env.read_txn()?;
        let iter = self.access_index.iter(&rtxn)?;
        let mut targets = Vec::new();
        for it in iter {
            let (key, _) = it?;
            targets.push(key);
            if targets.len() >= max_delete {
                break;
            }
        }
        drop(rtxn);
        let result = self.delete_keys(targets);
        let (status, removed) = match &result {
            Ok(out) => ("ok", out.removed),
            Err(_) => ("error", 0),
        };
        self.metrics.record_gc(
            "lru",
            status,
            begin.elapsed().as_secs_f64() * 1000.0,
            removed,
        );
        result
    }

    fn delete_keys(&self, keys: Vec<AccessKey>) -> anyhow::Result<GcDeleteResult> {
        if keys.is_empty() {
            return Ok(GcDeleteResult {
                removed: 0,
                checksums: Vec::new(),
            });
        }
        let (tx, rx) = mpsc::sync_channel(1);
        self.writer_channel
            .submit_medium(WriteRequest::DeleteKeys { keys, reply: tx });
        rx.recv()
            .map_err(|_| anyhow::anyhow!("Writer thread dropped"))?
    }

    fn update_access<'env>(
        access_db: &Database<CheckSumOnDisk, AccessTime>,
        access_index: &Database<AccessKey, Bytes>,
        wtxn: &mut RwTxn<'env>,
        cs: &CheckSumOnDisk,
        now: u64,
    ) -> anyhow::Result<()> {
        if let Some(old) = access_db.get(wtxn, cs)? {
            if now <= old.secs || now.saturating_sub(old.secs) < ACCESS_UPDATE_MIN_INTERVAL_SECS {
                return Ok(());
            }
            let old_key = AccessKey::new(old.secs, *cs);
            access_index.delete(wtxn, &old_key)?;
        }
        let access_time = AccessTime { secs: now };
        access_db.put(wtxn, cs, &access_time)?;
        let new_key = AccessKey::new(now, *cs);
        access_index.put(wtxn, &new_key, &[])?;
        Ok(())
    }

    pub fn touch_chunk(&self, cs: &CheckSum) -> anyhow::Result<()> {
        let cs_on_disk = CheckSumOnDisk::from(*cs);
        self.enqueue_access_update(cs_on_disk);
        self.metrics.record_touch("ok");
        Ok(())
    }

    fn enqueue_access_update(&self, cs: CheckSumOnDisk) {
        self.writer_channel.enqueue_access(cs);
    }

    /// Get lightweight statistics about the ChunkDB without full database traversal.
    /// Returns JSON-formatted statistics including storage usage and chunk count.
    pub fn get_stats(&self) -> anyhow::Result<Value> {
        // Storage statistics (must be collected before creating read transaction)
        let info = self.env.info();
        let map_size = info.map_size as u64;
        let used_size = self.env.non_free_pages_size()?;
        let free_size = map_size.saturating_sub(used_size);
        let usage_ratio = if map_size > 0 {
            (used_size as f64 / map_size as f64) * 100.0
        } else {
            0.0
        };

        // Create read transaction for database queries
        let rtxn = self.env.read_txn()?;

        // Get chunk count from database statistics (O(1) operation)
        let data_stat = self.data_db.stat(&rtxn)?;
        let total_chunks = data_stat.entries as u64;

        // Get access time range from sorted index (O(1) - first and last entry only)
        let mut oldest_access: Option<u64> = None;
        let mut newest_access: Option<u64> = None;

        if let Some(first) = self.access_index.first(&rtxn)? {
            oldest_access = Some(first.0.last_access);
        }
        if let Some(last) = self.access_index.last(&rtxn)? {
            newest_access = Some(last.0.last_access);
        }

        // Reader statistics
        let num_readers = info.number_of_readers;
        let max_readers = info.maximum_number_of_readers;
        let stale_readers = self.env.clear_stale_readers()?;

        Ok(json!({
            "storage": {
                "total_size_bytes": map_size,
                "used_size_bytes": used_size,
                "free_size_bytes": free_size,
                "usage_percent": format!("{:.2}", usage_ratio),
            },
            "chunks": {
                "total_count": total_chunks,
            },
            "access_time": {
                "oldest_epoch_secs": oldest_access,
                "newest_epoch_secs": newest_access,
            },
            "readers": {
                "current": num_readers,
                "max": max_readers,
                "stale_cleared": stale_readers,
            },
        }))
    }

    fn spawn_writer_thread(
        env: Env,
        data_db: ChunkDataDb,
        access_db: ChunkAccessDb,
        access_index: ChunkAccessIndexDb,
        index_ctl: Option<Arc<dyn ChunkIndexControl>>,
        channel: Arc<WriterChannel>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let writer = WriterThread {
                env,
                data_db,
                access_db,
                access_index,
                index_ctl,
                channel,
            };
            writer.run();
        })
    }
}

pub struct GcWorker {
    chunk_db: ChunkDB,
    local_chunk_client: Option<Arc<dyn ChunkIndexControl>>,
    expire_after: Duration,
    high_watermark: f64,
    low_watermark: f64,
}

impl GcWorker {
    pub fn new_with_local_client<P: AsRef<Path>>(
        chunk_db_dir: P,
        local_chunk_client: Option<Arc<dyn ChunkIndexControl>>,
    ) -> anyhow::Result<Self> {
        Self::new_with_opts_and_client(
            chunk_db_dir,
            Duration::from_secs(DEFAULT_GC_EXPIRE_SECS),
            GC_HIGH_WATERMARK,
            GC_LOW_WATERMARK,
            local_chunk_client,
        )
    }

    pub fn new_with_opts_and_client<P: AsRef<Path>>(
        chunk_db_dir: P,
        expire_after: Duration,
        high_watermark: f64,
        low_watermark: f64,
        local_chunk_client: Option<Arc<dyn ChunkIndexControl>>,
    ) -> anyhow::Result<Self> {
        if !(0.0..=1.0).contains(&high_watermark)
            || !(0.0..=1.0).contains(&low_watermark)
            || low_watermark > high_watermark
        {
            return Err(io::Error::new(ErrorKind::InvalidInput, "Invalid GC watermark").into());
        }
        let chunk_db = ChunkDB::new(chunk_db_dir)?;
        Ok(Self {
            chunk_db,
            local_chunk_client,
            expire_after,
            high_watermark,
            low_watermark,
        })
    }

    fn usage_ratio(&self) -> anyhow::Result<f64> {
        let info = self.chunk_db.env.info();
        let map_size = info.map_size as f64;
        if map_size <= 0.0 {
            return Ok(0.0);
        }
        let used = self.chunk_db.env.non_free_pages_size()? as f64;
        Ok((used / map_size).min(1.0))
    }

    pub fn run(&self, dry_run: bool) -> anyhow::Result<()> {
        if dry_run {
            info!("GcWorker dry run enabled; skipping deletions.");
            return Ok(());
        }
        let expired = self.chunk_db.gc_expired(self.expire_after)?;
        self.unregister_chunks(&expired.checksums);
        info!(expired = expired.removed, "GC expired chunks completed.");
        loop {
            let usage = self.usage_ratio()?;
            if usage <= self.high_watermark {
                break;
            }
            let removed = self.chunk_db.gc_lru(GC_BATCH)?;
            self.unregister_chunks(&removed.checksums);
            info!(
                removed = removed.removed,
                usage = usage,
                "GC LRU batch completed."
            );
            if removed.removed == 0 {
                break;
            }
            let new_usage = self.usage_ratio()?;
            if new_usage <= self.low_watermark {
                break;
            }
        }
        Ok(())
    }

    fn unregister_chunks(&self, checksums: &[CheckSum]) {
        let Some(client) = &self.local_chunk_client else {
            return;
        };
        if !client.unregister_chunks(checksums) {
            debug!(
                count = checksums.len(),
                "Failed to unregister chunks with local chunk server during GC."
            );
        }
    }
}

impl Drop for ChunkDB {
    fn drop(&mut self) {
        {
            let mut q = self.writer_channel.inner.lock().unwrap();
            q.stop = true;
        }
        self.writer_channel.cond.notify_one();
        if let Some(handle) = self.writer_handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests;
