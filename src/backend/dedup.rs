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

use crate::backend::chunkdb::CheckSumMethod::Blake3;
use crate::backend::chunkdb::ChunkDB;
pub use crate::backend::chunkdb::{CheckSum, CheckSumMethod};
use crate::backend::indexdb::{DedupInfo, DedupRange, IndexDB};
use crate::backend::peer::LocalChunkClient;
use crate::backend::{Backend, BackendEx, CHUNK_SIZE};
use crate::utils::align_up;
#[cfg(target_os = "linux")]
use fuse_backend_rs::api::filesystem::ZeroCopyWriter;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram, Meter};
use opentelemetry::KeyValue;
use std::io;
use std::io::ErrorKind;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tracing::{debug, error, info, warn};

const SYNC_CHUNK_REQ_BUF_SIZE: usize = 256;
const DEDUP_CHUNK_REQ_BUF_SIZE: usize = 256;

#[derive(Debug)]
struct DedupMetrics {
    read_total: Counter<u64>,
    read_duration: Histogram<f64>,
    chunk_hit_total: Counter<u64>,
    backend_fallback_total: Counter<u64>,
    store_total: Counter<u64>,
    store_bytes: Counter<u64>,
}

impl DedupMetrics {
    fn new(meter: &Meter) -> Self {
        Self {
            read_total: meter
                .u64_counter("distill_fs.dedup.read_total")
                .with_description("Total dedup read attempts")
                .init(),
            read_duration: meter
                .f64_histogram("distill_fs.dedup.read_duration_ms")
                .with_description("Dedup read duration")
                .with_unit("ms")
                .init(),
            chunk_hit_total: meter
                .u64_counter("distill_fs.dedup.chunk_hit_total")
                .with_description("Total dedup chunk hits")
                .init(),
            backend_fallback_total: meter
                .u64_counter("distill_fs.dedup.backend_fallback_total")
                .with_description("Total dedup backend fallbacks")
                .init(),
            store_total: meter
                .u64_counter("distill_fs.dedup.store_total")
                .with_description("Total dedup stores into ChunkDB")
                .init(),
            store_bytes: meter
                .u64_counter("distill_fs.dedup.store_bytes")
                .with_description("Bytes stored into ChunkDB by dedup")
                .with_unit("By")
                .init(),
        }
    }

    fn record_read(&self, result: &'static str, elapsed_ms: f64) {
        let attrs = [KeyValue::new("result", result)];
        self.read_total.add(1, &attrs);
        self.read_duration.record(elapsed_ms, &attrs);
    }

    fn record_chunk_hit(&self) {
        self.chunk_hit_total.add(1, &[]);
    }

    fn record_backend_fallback(&self, reason: &'static str) {
        self.backend_fallback_total
            .add(1, &[KeyValue::new("reason", reason)]);
    }

    fn record_store(&self, result: &'static str, bytes: usize) {
        let attrs = [KeyValue::new("result", result)];
        self.store_total.add(1, &attrs);
        if bytes > 0 {
            self.store_bytes.add(bytes as u64, &attrs);
        }
    }
}

// Core deduplication logic with all workers and channels managed internally
#[derive(Debug)]
struct DedupCore {
    index_db: Arc<IndexDB>,
    chunk_db: Arc<ChunkDB>,
    b: Arc<dyn BackendEx>,
    id: [u8; 32],
    dedup_chunks: Vec<AtomicU64>,
    local_chunk_client: Option<Arc<LocalChunkClient>>,
    metrics: DedupMetrics,
}

#[derive(Debug)]
pub struct DedupReader {
    core: Arc<DedupCore>,
    sync_tx: SyncSender<DedupInfo>,
    dedup_tx: SyncSender<DedupRequest>,
}

impl DedupCore {
    fn chunk_is_dedup(&self, idx: usize) -> bool {
        let bit_at = idx / 64;
        let bit_off = idx % 64;
        let value = self.dedup_chunks[bit_at].load(Relaxed);
        value & (1_u64 << bit_off) != 0
    }

    fn mark_chunk_dedup(&self, idx: usize) {
        let bit_at = idx / 64;
        let bit_off = idx % 64;
        loop {
            let old = self.dedup_chunks[bit_at].load(Relaxed);
            let new = old | (1_u64 << bit_off);
            if old == new {
                break;
            }
            if self.dedup_chunks[bit_at]
                .compare_exchange(old, new, SeqCst, SeqCst)
                .is_ok()
            {
                break;
            }
        }
    }

    fn has_data(&self, cs: &CheckSum) -> anyhow::Result<bool> {
        self.chunk_db.has_chunk(cs)
    }

    fn add_data(&self, cs: &CheckSum, data: &[u8]) -> anyhow::Result<()> {
        let len = data.len();
        if let Err(err) = self.chunk_db.add_chunk(cs, data.to_vec()) {
            self.metrics.record_store("error", 0);
            return Err(err);
        }
        self.metrics.record_store("ok", len);
        Ok(())
    }

    fn add_local_dedup_info(&self, range: &DedupRange, cs: &CheckSum) -> anyhow::Result<()> {
        let mut wtxn =
            super::slow_write_txn(&self.index_db.env, "DedupReader::add_local_dedup_info")?;
        let info = DedupInfo::new(range.start, range.size() as u32, *cs);
        self.index_db.storage.put(&mut wtxn, range, &info)?;
        wtxn.commit()?;
        Ok(())
    }

    fn dedup_check_range(&self, range: &DedupRange) -> anyhow::Result<bool> {
        let infos = self.dedup_range(range.start, range.size())?;
        if infos.len() != 1 {
            return Err(io::Error::new(ErrorKind::InvalidInput, "Invalid range").into());
        }
        Ok(infos[0].0.cs.is_valid())
    }

    fn dedup_range(&self, mut off: u64, mut len: usize) -> anyhow::Result<Vec<(DedupInfo, u64)>> {
        if len == 0 {
            return Ok(vec![]);
        }
        let rtxn = self.index_db.env.read_txn()?;
        let range = DedupRange::new_with_id(self.id, off, 1)..;
        let iter = self.index_db.storage.range(&rtxn, &range)?;
        let mut infos = vec![];
        for it in iter {
            let (k, v) = it?;
            if k.id != self.id {
                break;
            }
            if v.off > off {
                let invalid_len = (v.off - off).min(len as u64);
                infos.push((
                    DedupInfo::new(off, invalid_len as u32, CheckSum::empty()),
                    invalid_len,
                ));
                off += invalid_len;
                len -= invalid_len as usize;
                if len == 0 {
                    break;
                }
            } else if v.off <= off {
                if v.end() <= off {
                    continue;
                }
                let valid_len = (v.end() - off).min(len as u64);
                infos.push((*v, valid_len));
                off += valid_len;
                len -= valid_len as usize;
            }
            if len == 0 || v.off >= off + (len as u64) {
                break;
            }
        }
        if len != 0 {
            infos.push((
                DedupInfo::new(off, len as u32, CheckSum::empty()),
                len as u64,
            ));
        }
        Ok(infos)
    }

    fn dedup(
        &self,
        off: u64,
        len: u32,
        cs: Option<CheckSum>,
        mut method: CheckSumMethod,
    ) -> anyhow::Result<()> {
        if len == 0 {
            return Ok(());
        }

        let backend_size = self.b.size();
        if off >= backend_size {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "Offset {} is out of range for backend size {}",
                    off, backend_size
                ),
            )
            .into());
        }

        // Ensure off and len are CHUNK_SIZE aligned
        if off % CHUNK_SIZE as u64 != 0 {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "Offset {} is not aligned to CHUNK_SIZE ({})",
                    off, CHUNK_SIZE
                ),
            )
            .into());
        }
        // Allow unaligned length only for the last chunk (at EOF)
        let end = off.checked_add(len as u64).ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidInput,
                format!("Range overflow: off={}, len={}", off, len),
            )
        })?;
        let is_last_chunk = end >= backend_size;
        if len % CHUNK_SIZE as u32 != 0 && !is_last_chunk {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "Length {} is not aligned to CHUNK_SIZE ({})",
                    len, CHUNK_SIZE
                ),
            )
            .into());
        }

        let range = DedupRange::new_with_id(self.id, off, len);
        if self.dedup_check_range(&range)? {
            debug!(off = off, len = len, "Data is duplicated.");
            return Ok(());
        }
        info!(off = off, len = len, "Dedup backend.");
        if let Some(checksum) = cs.as_ref() {
            if self.has_data(checksum)? {
                if let Err(e) = self.chunk_db.touch_chunk(checksum) {
                    warn!(err = debug(e), "Failed to update chunk access time.");
                }
                self.add_local_dedup_info(&range, checksum)?;
                return Ok(());
            }
            method = checksum.method;
        }
        // get raw data
        let mut buf = vec![0_u8; len as usize];
        let n = self.b.fetch(off as usize, &mut buf)?;
        if n != len as usize {
            // Check if short read is due to EOF
            if off + (n as u64) != backend_size {
                return Err(io::Error::new(
                    ErrorKind::UnexpectedEof,
                    format!(
                        "Short read: expected {} bytes, got {} bytes at offset {}, backend size {}",
                        len, n, off, backend_size
                    ),
                )
                .into());
            }
            // Truncate buffer to actual read size for EOF case
            buf.truncate(n);
            debug!(
                off = off,
                expected = len,
                actual = n,
                "Partial chunk at EOF, deduplicating {} bytes",
                n
            );
        }
        // use blake3/sha256 to calculate checksum
        let data_cs = CheckSum::from_data(&buf, method);
        self.add_data(&data_cs, &buf)?;
        // Update range to reflect actual data size
        let actual_range = DedupRange::new_with_id(self.id, off, n as u32);
        self.add_local_dedup_info(&actual_range, &data_cs)?;
        Ok(())
    }

    fn gc_chunk(&self, chunk_id: usize) -> anyhow::Result<()> {
        let start = chunk_id * CHUNK_SIZE;
        if start as u64 > self.b.size() {
            return Ok(());
        }
        let end = ((chunk_id + 1) * CHUNK_SIZE).min(self.b.size() as usize);
        let infos = self.dedup_range(start as u64, end - start)?;
        for (info, _) in &infos {
            if !info.cs.is_valid() {
                return Ok(());
            }
        }
        self.b.invalidate_chunk(chunk_id)?;
        Ok(())
    }
}

impl DedupReader {
    pub fn new(
        b: Arc<dyn BackendEx>,
        chunk_db: Arc<ChunkDB>,
        index_db: Arc<IndexDB>,
        data_id: &str,
        local_chunk_client: Option<Arc<LocalChunkClient>>,
    ) -> anyhow::Result<Self> {
        let meter = global::meter("distill_fs.dedup");
        Self::new_with_metrics(
            b,
            chunk_db,
            index_db,
            data_id,
            local_chunk_client,
            DedupMetrics::new(&meter),
        )
    }

    fn new_with_metrics(
        b: Arc<dyn BackendEx>,
        chunk_db: Arc<ChunkDB>,
        index_db: Arc<IndexDB>,
        data_id: &str,
        local_chunk_client: Option<Arc<LocalChunkClient>>,
        metrics: DedupMetrics,
    ) -> anyhow::Result<Self> {
        let id_hash = blake3::hash(data_id.as_bytes());
        let id = *id_hash.as_bytes();

        // Create channels
        let (sync_tx, sync_rx) = mpsc::sync_channel(SYNC_CHUNK_REQ_BUF_SIZE);
        let (dedup_tx, dedup_rx) = mpsc::sync_channel(DEDUP_CHUNK_REQ_BUF_SIZE);

        // Create dedup bitmap
        let nr_chunks = align_up(b.size() as usize, CHUNK_SIZE) / CHUNK_SIZE;
        let dedup_chunks = (0..align_up(nr_chunks, 64) / 64)
            .map(|_| AtomicU64::new(0))
            .collect();

        // Create DedupCore
        let core = Arc::new(DedupCore {
            index_db,
            chunk_db,
            b: b.clone(),
            id,
            dedup_chunks,
            local_chunk_client,
            metrics,
        });

        // Create SyncWorker
        let sync_worker_core = core.clone();
        let sync_worker_handle = SyncWorker::new(sync_worker_core, sync_rx);
        thread::spawn(move || sync_worker_handle.run());

        // Create DedupWorker
        let dedup_worker_core = core.clone();
        let dedup_worker_handle = DedupWorker::new(dedup_worker_core, dedup_rx);
        thread::spawn(move || dedup_worker_handle.run());

        Ok(Self {
            core,
            sync_tx,
            dedup_tx,
        })
    }

    #[cfg(all(test, target_os = "linux"))]
    fn dedup(
        &self,
        off: u64,
        len: u32,
        cs: Option<CheckSum>,
        method: CheckSumMethod,
    ) -> anyhow::Result<()> {
        self.core.dedup(off, len, cs, method)
    }

    #[cfg(all(test, target_os = "linux"))]
    fn gc_chunk(&self, chunk_id: usize) -> anyhow::Result<()> {
        self.core.gc_chunk(chunk_id)
    }

    fn enqueue_sync(&self, info: DedupInfo) {
        if let Err(e) = self.sync_tx.try_send(info) {
            warn!(err = debug(e), "Failed to enqueue dedup sync task.");
        }
    }

    fn try_dedup_chunk(&self, off: usize, len: usize, method: CheckSumMethod) {
        if len == 0 {
            return;
        }
        let chunk_start = off / CHUNK_SIZE;
        let chunk_end = (off + len - 1) / CHUNK_SIZE;

        for idx in chunk_start..=chunk_end {
            if self.core.chunk_is_dedup(idx) {
                continue;
            }
            let req = DedupRequest::new((idx * CHUNK_SIZE) as u64, CHUNK_SIZE as u32, method);
            if let Err(e) = self.dedup_tx.try_send(req) {
                warn!(err = debug(e), "Failed to send dedup request.")
            } else {
                self.core.mark_chunk_dedup(idx);
            }
        }
    }

    fn process_dedup_op<T: DataTransfer>(
        &self,
        off: usize,
        len: usize,
        op: &mut T,
    ) -> io::Result<usize> {
        let infos = match self.core.dedup_range(off as u64, len) {
            Ok(infos) => infos,
            Err(e) => {
                self.core.metrics.record_backend_fallback("range_error");
                error!(
                    err = debug(e),
                    "Failed to dedup_range, fallback to backend op."
                );
                return op.copy_from_backend(off, 0, len);
            }
        };
        if infos.is_empty() {
            return op.copy_from_backend(off, 0, len);
        }
        let mut pos = 0_usize;
        let mut whole_off = off as u64;
        for (info, seg_len) in infos {
            let seg_len_usize = seg_len as usize;
            let n = if !info.cs.is_valid() {
                self.core
                    .metrics
                    .record_backend_fallback("missing_metadata");
                op.copy_from_backend(whole_off as usize, pos, seg_len_usize)?
            } else {
                let cs = CheckSum::from(info.cs);
                let start = (whole_off - info.off) as usize;
                match self.read_dedup_chunk(&cs, start, seg_len_usize, op, pos) {
                    Ok(Some(n)) => {
                        self.core.metrics.record_chunk_hit();
                        n
                    }
                    Ok(None) => {
                        self.enqueue_sync(info);
                        self.core.metrics.record_backend_fallback("chunk_miss");
                        error!("Failed to get dedup data, fallback to backend op.");
                        op.copy_from_backend(whole_off as usize, pos, seg_len_usize)?
                    }
                    Err(e) => {
                        self.core.metrics.record_backend_fallback("chunk_error");
                        error!(
                            err = debug(e),
                            "Failed to get dedup data, fallback to backend op."
                        );
                        op.copy_from_backend(whole_off as usize, pos, seg_len_usize)?
                    }
                }
            };
            pos += n;
            whole_off += n as u64;
        }
        Ok(pos)
    }

    fn read_dedup_chunk<T: DataTransfer>(
        &self,
        cs: &CheckSum,
        start: usize,
        len: usize,
        op: &mut T,
        pos: usize,
    ) -> io::Result<Option<usize>> {
        match self
            .core
            .chunk_db
            .with_chunk_range(cs, start, len, |slice| op.copy_from_chunk(slice, pos, len))
        {
            Ok(Some(n)) => Ok(Some(n)),
            Ok(None) => {
                if let Some(client) = &self.core.local_chunk_client {
                    let _ = client.prefetch_chunk_blocking(cs);
                    match self
                        .core
                        .chunk_db
                        .with_chunk_range(cs, start, len, |slice| {
                            op.copy_from_chunk(slice, pos, len)
                        }) {
                        Ok(Some(n)) => Ok(Some(n)),
                        Ok(None) => Ok(None),
                        Err(e) => Err(io::Error::other(e.to_string())),
                    }
                } else {
                    Ok(None)
                }
            }
            Err(e) => Err(io::Error::other(e.to_string())),
        }
    }
}

trait DataTransfer {
    fn copy_from_backend(&mut self, off: usize, pos: usize, len: usize) -> io::Result<usize>;
    fn copy_from_chunk(&mut self, data: &[u8], pos: usize, len: usize) -> io::Result<usize>;
}

struct FetchOp<'a> {
    dedup: &'a DedupReader,
    data: &'a mut [u8],
}

impl<'a> DataTransfer for FetchOp<'a> {
    fn copy_from_backend(&mut self, off: usize, pos: usize, len: usize) -> io::Result<usize> {
        self.dedup.core.b.fetch(off, &mut self.data[pos..pos + len])
    }

    fn copy_from_chunk(&mut self, data: &[u8], pos: usize, len: usize) -> io::Result<usize> {
        self.data[pos..pos + len].copy_from_slice(data);
        Ok(len)
    }
}

#[cfg(target_os = "linux")]
struct FuseWriteOp<'a> {
    reader: &'a DedupReader,
    writer: &'a mut dyn ZeroCopyWriter,
}

#[cfg(target_os = "linux")]
impl<'a> DataTransfer for FuseWriteOp<'a> {
    fn copy_from_backend(&mut self, off: usize, _pos: usize, len: usize) -> io::Result<usize> {
        self.reader
            .core
            .b
            .write_to_fuse_writer(off, len as u32, self.writer)
    }

    fn copy_from_chunk(&mut self, data: &[u8], _pos: usize, _len: usize) -> io::Result<usize> {
        self.writer.write(data)
    }
}

struct SyncWorker {
    core: Arc<DedupCore>,
    rx: Receiver<DedupInfo>,
}

impl SyncWorker {
    fn new(core: Arc<DedupCore>, rx: Receiver<DedupInfo>) -> Self {
        Self { core, rx }
    }

    fn run(self) {
        while let Ok(info) = self.rx.recv() {
            self.process(info);
        }
    }

    fn process(&self, info: DedupInfo) {
        if !info.cs.is_valid() || info.len == 0 {
            return;
        }
        let cs = CheckSum::from(info.cs);
        let len = info.len as usize;
        let mut buf = vec![0_u8; len];
        let n = match self.core.b.fetch(info.off as usize, &mut buf) {
            Ok(n) => n,
            Err(e) => {
                warn!(err = debug(e), "Failed to fetch data for sync.");
                return;
            }
        };
        if n != len {
            warn!(
                expected = len,
                actual = n,
                "Short read when syncing dedup chunk."
            );
            return;
        }
        let data_cs = CheckSum::from_data(&buf, cs.method);
        if data_cs != cs {
            warn!("Checksum mismatch when syncing dedup chunk.");
            return;
        }
        let buf_len = buf.len();
        if let Err(e) = self.core.chunk_db.add_chunk(&cs, buf) {
            self.core.metrics.record_store("error", 0);
            warn!(err = debug(e), "Failed to add synced chunk to chunkdb.");
            return;
        }
        self.core.metrics.record_store("ok", buf_len);

        // Try to GC the chunks now that data is in ChunkDB
        for chunk_id in
            (info.off as usize) / CHUNK_SIZE..=(info.off as usize + len - 1) / CHUNK_SIZE
        {
            if let Err(e) = self.core.gc_chunk(chunk_id) {
                debug!(
                    chunk_id = chunk_id,
                    err = debug(e),
                    "Failed to gc chunk after sync."
                );
            }
        }
    }
}

impl Backend for DedupReader {
    fn size(&self) -> u64 {
        self.core.b.size()
    }

    fn fetch(&self, off: usize, data: &mut [u8]) -> io::Result<usize> {
        let begin = Instant::now();
        if data.is_empty() {
            self.core.metrics.record_read("ok", 0.0);
            return Ok(0);
        }
        let size = self.core.b.size() as usize;
        if off >= size {
            self.core.metrics.record_read("ok", 0.0);
            return Ok(0);
        }
        let len = data.len().min(size - off);
        let mut op = FetchOp { dedup: self, data };
        let res = self.process_dedup_op(off, len, &mut op);
        if res.is_ok() {
            self.try_dedup_chunk(off, len, Blake3);
        }
        let status = if res.is_ok() { "ok" } else { "error" };
        self.core
            .metrics
            .record_read(status, begin.elapsed().as_secs_f64() * 1000.0);
        res
    }

    #[cfg(target_os = "linux")]
    fn write_to_fuse_writer(
        &self,
        off: usize,
        size: u32,
        w: &mut dyn ZeroCopyWriter,
    ) -> io::Result<usize> {
        let begin = Instant::now();
        if size == 0 {
            self.core.metrics.record_read("ok", 0.0);
            return Ok(0);
        }
        let total = self.core.b.size() as usize;
        if off >= total {
            self.core.metrics.record_read("ok", 0.0);
            return Ok(0);
        }
        let len = (size as usize).min(total - off);
        let mut op = FuseWriteOp {
            reader: self,
            writer: w,
        };
        let res = self.process_dedup_op(off, len, &mut op);
        if res.is_ok() {
            self.try_dedup_chunk(off, len, Blake3);
        }
        let status = if res.is_ok() { "ok" } else { "error" };
        self.core
            .metrics
            .record_read(status, begin.elapsed().as_secs_f64() * 1000.0);
        res
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct DedupRequest {
    off: u64,
    len: u32,
    check_sum: Option<CheckSum>,
    method: CheckSumMethod,
}

impl DedupRequest {
    pub fn new(off: u64, len: u32, method: CheckSumMethod) -> Self {
        Self {
            off,
            len,
            check_sum: None,
            method,
        }
    }

    pub fn offset(&self) -> u64 {
        self.off
    }

    pub fn size(&self) -> usize {
        self.len as usize
    }
}

#[derive(Debug)]
struct DedupWorker {
    core: Arc<DedupCore>,
    requests: Receiver<DedupRequest>,
}

impl DedupWorker {
    fn new(core: Arc<DedupCore>, requests: Receiver<DedupRequest>) -> Self {
        Self { core, requests }
    }

    fn run(self) {
        loop {
            match self.requests.recv() {
                Ok(req) => {
                    if let Err(e) = self.core.dedup(req.off, req.len, req.check_sum, req.method) {
                        error!(req = debug(req), err = debug(e), "Failed to do dedup.");
                    } else {
                        if req.size() == 0 {
                            continue;
                        }
                        // gc chunks directly
                        let start_chunk = (req.offset() as usize) / CHUNK_SIZE;
                        let end_chunk = ((req.offset() as usize) + req.size() - 1) / CHUNK_SIZE;
                        info!(
                            start = start_chunk,
                            end = end_chunk,
                            "Start trying to gc chunks"
                        );
                        for chunk_id in start_chunk..=end_chunk {
                            if let Err(e) = self.core.gc_chunk(chunk_id) {
                                error!(
                                    chunk_id = chunk_id,
                                    error = debug(e),
                                    "Failed to gc chunk."
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(err = debug(e), "Failed to recv dedup requests.");
                    break;
                }
            }
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests;
