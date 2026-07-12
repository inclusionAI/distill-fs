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

use crate::backend::{Backend, BackendEx, CHUNK_SIZE};
use crate::utils::{align_up, new_std_io_error, page_size};
use fuse_backend_rs::api::filesystem::ZeroCopyWriter;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram, Meter};
use opentelemetry::KeyValue;
use std::collections::BTreeMap;
use std::fmt::Formatter;
use std::fs::File;
use std::os::fd::AsRawFd;
#[cfg(not(target_os = "linux"))]
use std::os::unix::fs::FileExt;
use std::sync::atomic::Ordering::{Acquire, Relaxed, SeqCst};
use std::sync::atomic::{AtomicPtr, AtomicU8};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::{Duration, Instant};
use tracing::{error, info};

const IO_WAIT_TIMEOUT: u64 = 120;

#[derive(Debug)]
struct CacheMetrics {
    read_total: Counter<u64>,
    read_duration: Histogram<f64>,
    read_bytes: Counter<u64>,
    backend_fetch_total: Counter<u64>,
    backend_fetch_bytes: Counter<u64>,
}

impl CacheMetrics {
    fn new(meter: &Meter) -> Self {
        Self {
            read_total: meter
                .u64_counter("distill_fs.cache.read_total")
                .with_description("Total cache read attempts")
                .init(),
            read_duration: meter
                .f64_histogram("distill_fs.cache.read_duration_ms")
                .with_description("Cache read duration")
                .with_unit("ms")
                .init(),
            read_bytes: meter
                .u64_counter("distill_fs.cache.read_bytes")
                .with_description("Bytes returned by cache reads")
                .with_unit("By")
                .init(),
            backend_fetch_total: meter
                .u64_counter("distill_fs.cache.backend_fetch_total")
                .with_description("Total backend fetches issued by the cache")
                .init(),
            backend_fetch_bytes: meter
                .u64_counter("distill_fs.cache.backend_fetch_bytes")
                .with_description("Bytes fetched from the backend into cache")
                .with_unit("By")
                .init(),
        }
    }

    fn record_read(&self, result: &'static str, elapsed_ms: f64, bytes: usize) {
        let attrs = [KeyValue::new("result", result)];
        self.read_total.add(1, &attrs);
        self.read_duration.record(elapsed_ms, &attrs);
        if bytes > 0 {
            self.read_bytes.add(bytes as u64, &attrs);
        }
    }

    fn record_backend_fetch(&self, result: &'static str, bytes: usize) {
        let attrs = [KeyValue::new("result", result)];
        self.backend_fetch_total.add(1, &attrs);
        if bytes > 0 {
            self.backend_fetch_bytes.add(bytes as u64, &attrs);
        }
    }
}

#[derive(Debug)]
pub enum CacheError {
    IoError(std::io::Error),
    InvalidRange,
    Timeout,
}

impl std::fmt::Display for CacheError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheError::IoError(e) => {
                write!(f, "io_error: {}", e)
            }
            CacheError::InvalidRange => {
                write!(f, "invalid range")
            }
            CacheError::Timeout => {
                write!(f, "timeout")
            }
        }
    }
}

impl From<std::io::Error> for CacheError {
    fn from(e: std::io::Error) -> Self {
        Self::IoError(e)
    }
}

impl std::error::Error for CacheError {}

pub type CacheResult<T> = Result<T, CacheError>;

#[derive(Debug)]
struct MmapInner {
    file: File,
    buf: AtomicPtr<u8>,
    len: usize,
}

impl MmapInner {
    fn new(file: File) -> std::io::Result<Self> {
        let meta = file.metadata()?;
        let buf = unsafe {
            let ptr = libc::mmap(
                std::ptr::null_mut(),
                meta.len() as libc::size_t,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            );
            if ptr == libc::MAP_FAILED {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(ptr as *mut u8)
            }
        }?;
        Ok(Self {
            file,
            buf: AtomicPtr::new(buf),
            len: meta.len() as usize,
        })
    }

    fn base(&self) -> *mut u8 {
        self.buf.load(Relaxed)
    }

    #[allow(clippy::mut_from_ref)]
    fn as_mut_slice(&self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.base(), self.len) }
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.base() as *const u8, self.len) }
    }
}

impl Drop for MmapInner {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.base() as *mut libc::c_void, self.len);
        }
    }
}

#[cfg(target_os = "linux")]
fn punch_hole(file: &File, start: usize, size: usize) -> std::io::Result<()> {
    let fd = file.as_raw_fd();
    let mode = libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE;
    let ret = unsafe { libc::fallocate(fd, mode, start as libc::off_t, size as libc::off_t) };
    if ret == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn punch_hole(file: &File, start: usize, size: usize) -> std::io::Result<()> {
    let zeros = vec![0_u8; size.min(CHUNK_SIZE)];
    let mut written = 0;
    while written < size {
        let chunk = (size - written).min(zeros.len());
        let n = file.write_at(&zeros[..chunk], (start + written) as u64)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "failed to zero cached range",
            ));
        }
        written += n;
    }
    Ok(())
}

fn bitmap_file_size(size: u64) -> u64 {
    let nr_chunks = align_up(size, CHUNK_SIZE as u64) / CHUNK_SIZE as u64;
    (align_up(nr_chunks, 8) / 8).max(page_size())
}

fn set_bit(target: &AtomicU8, bit_shift: usize) {
    loop {
        let old = target.load(Acquire);
        let new = old | (1_u8 << bit_shift);
        if old == new {
            break;
        }
        if target.compare_exchange(old, new, SeqCst, SeqCst).is_ok() {
            break;
        }
    }
}

fn clear_bit(target: &AtomicU8, bit_shift: usize) {
    loop {
        let old = target.load(Acquire);
        let new = old & !(1_u8 << bit_shift);
        if old == new {
            break;
        }
        if target.compare_exchange(old, new, SeqCst, SeqCst).is_ok() {
            break;
        }
    }
}

#[derive(Debug)]
pub struct EmptyBackend(u64);

impl Backend for EmptyBackend {
    fn size(&self) -> u64 {
        self.0
    }

    fn fetch(&self, _off: usize, data: &mut [u8]) -> std::io::Result<usize> {
        Ok(data.len())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum InFlightStatus {
    Fetching,
    Done,
}

#[derive(Debug)]
struct InFlightIO {
    state: Mutex<InFlightStatus>,
    cond: Condvar,
}

impl InFlightIO {
    fn new() -> Self {
        Self {
            state: Mutex::new(InFlightStatus::Fetching),
            cond: Condvar::new(),
        }
    }

    fn notify(&self) {
        self.cond.notify_all();
    }

    fn done(&self) {
        let mut state = self.state.lock().unwrap();
        *state = InFlightStatus::Done;
        drop(state);
        self.notify();
    }

    fn wait(&self) -> bool {
        let state = self.state.lock().unwrap();
        if *state == InFlightStatus::Fetching {
            let r = self
                .cond
                .wait_timeout(state, Duration::from_secs(IO_WAIT_TIMEOUT))
                .unwrap();
            r.1.timed_out()
        } else {
            false
        }
    }
}

struct ProcessIOGuard(Arc<InFlightIO>);

impl Drop for ProcessIOGuard {
    fn drop(&mut self) {
        self.0.done();
    }
}

#[derive(Debug)]
pub struct Cache<B> {
    raw: MmapInner,
    bitmap: MmapInner,
    drop_lock: RwLock<()>,
    in_flight_ios: Mutex<BTreeMap<usize, Arc<InFlightIO>>>,
    metrics: CacheMetrics,
    b: B,
}

impl Cache<EmptyBackend> {
    pub fn from_raw_file(file_path: &str) -> CacheResult<Cache<EmptyBackend>> {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .open(file_path)?;
        let bitmap_path = format!("{}.bitmap", file_path);
        let meta = file.metadata()?;
        let bitmap_file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(false)
            .open(bitmap_path)?;
        bitmap_file.set_len(bitmap_file_size(meta.len()))?;
        let c = Self::raw(file, bitmap_file, EmptyBackend(meta.len()))?;
        c.fetch_data(0, meta.len() as usize)?;
        Ok(c)
    }
}

impl<B: Backend> Cache<B> {
    fn raw(raw_file: File, bitmap_file: File, b: B) -> CacheResult<Self> {
        let meter = global::meter("distill_fs.cache");
        Ok(Self {
            raw: MmapInner::new(raw_file)?,
            bitmap: MmapInner::new(bitmap_file)?,
            drop_lock: Default::default(),
            in_flight_ios: Mutex::new(BTreeMap::new()),
            metrics: CacheMetrics::new(&meter),
            b,
        })
    }

    pub fn new(b: B, cache_file_path: &str) -> CacheResult<Self> {
        let bitmap_path = format!("{}.bitmap", cache_file_path);
        let raw_file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(false)
            .open(cache_file_path)?;
        let meta = raw_file.metadata()?;
        if meta.len() != b.size() {
            raw_file.set_len(b.size())?;
        }
        let bitmap_file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(false)
            .open(bitmap_path)?;
        bitmap_file.set_len(bitmap_file_size(b.size()))?;
        Self::raw(raw_file, bitmap_file, b)
    }

    fn register_or_wait_inflight_io(&self, chunk_idx: usize) -> (Arc<InFlightIO>, bool) {
        let mut in_flight_ios = self.in_flight_ios.lock().unwrap();
        if let Some(io) = in_flight_ios.get(&chunk_idx) {
            (io.clone(), true)
        } else {
            let io = Arc::new(InFlightIO::new());
            in_flight_ios.insert(chunk_idx, io.clone());
            (io, false)
        }
    }

    fn remove_inflight_io(&self, chunk_idx: usize) {
        self.in_flight_ios.lock().unwrap().remove(&chunk_idx);
    }

    fn fetch_data(&self, off: usize, len: usize) -> CacheResult<()> {
        if len == 0 {
            return Ok(());
        }
        let first_chunk_idx = off / CHUNK_SIZE;
        let last_chunk_idx = (off + len - 1) / CHUNK_SIZE;
        let bitmap_data = self.bitmap.as_mut_slice();
        let raw_data = self.raw.as_mut_slice();
        'next_chunk: for idx in first_chunk_idx..=last_chunk_idx {
            let bit_at = idx / 8;
            let bit_shift = idx % 8;
            let raw_ptr: *mut u8 = &mut bitmap_data[bit_at] as *mut u8;
            let atomic_ref: &AtomicU8 = unsafe { &*(raw_ptr as *const AtomicU8) };
            loop {
                if atomic_ref.load(Acquire) & (1_u8 << bit_shift) != 0 {
                    continue 'next_chunk;
                }
                let (io, do_wait) = self.register_or_wait_inflight_io(idx);
                if do_wait {
                    let timeout = io.wait();
                    if timeout {
                        error!(chunk_idx = idx, "Wait fetch data timeout");
                        return Err(CacheError::Timeout);
                    }
                    continue;
                }
                let _guard = ProcessIOGuard(io);
                let start = idx * CHUNK_SIZE;
                let end = if (idx + 1) * CHUNK_SIZE <= self.b.size() as usize {
                    (idx + 1) * CHUNK_SIZE
                } else {
                    self.b.size() as usize
                };
                let fetch_len = end - start;
                if let Err(e) = self.b.fetch(idx * CHUNK_SIZE, &mut raw_data[start..end]) {
                    self.metrics.record_backend_fetch("io_error", 0);
                    self.remove_inflight_io(idx);
                    return Err(e.into());
                }
                self.metrics.record_backend_fetch("ok", fetch_len);
                // update bitmap
                set_bit(atomic_ref, bit_shift);
                self.remove_inflight_io(idx);
                break;
            }
        }
        Ok(())
    }

    fn fix_range(&self, off: usize, size: usize) -> CacheResult<(usize, usize)> {
        if off >= self.b.size() as usize {
            return Err(CacheError::InvalidRange);
        }
        let end = if off + size > self.b.size() as usize {
            self.b.size() as usize
        } else {
            off + size
        };
        Ok((off, end))
    }

    pub fn read_at(&self, off: usize, buf: &mut [u8]) -> CacheResult<usize> {
        let begin = Instant::now();
        if buf.is_empty() {
            self.metrics.record_read("ok", 0.0, 0);
            return Ok(0);
        }
        let _guard = self.drop_lock.read().unwrap();
        let result = (|| {
            let (off, end) = self.fix_range(off, buf.len())?;
            let len = end - off;
            self.fetch_data(off, len)?;
            let raw_data = self.raw.as_slice();
            buf[..len].copy_from_slice(&raw_data[off..end]);
            Ok(len)
        })();
        let (status, bytes) = match &result {
            Ok(len) => ("ok", *len),
            Err(CacheError::InvalidRange) => ("invalid_range", 0),
            Err(CacheError::Timeout) => ("timeout", 0),
            Err(CacheError::IoError(_)) => ("io_error", 0),
        };
        self.metrics
            .record_read(status, begin.elapsed().as_secs_f64() * 1000.0, bytes);
        result
    }
}

impl<B: Backend> Backend for Cache<B> {
    fn size(&self) -> u64 {
        self.b.size()
    }

    fn fetch(&self, off: usize, data: &mut [u8]) -> std::io::Result<usize> {
        self.read_at(off, data).map_err(new_std_io_error)
    }

    fn write_to_fuse_writer(
        &self,
        off: usize,
        size: u32,
        w: &mut dyn ZeroCopyWriter,
    ) -> std::io::Result<usize> {
        let (off, end) = self
            .fix_range(off, size as usize)
            .map_err(new_std_io_error)?;
        let _guard = self.drop_lock.read().unwrap();
        self.fetch_data(off, end - off).map_err(new_std_io_error)?;
        let data = self.raw.as_slice();
        w.write(&data[off..end])
    }
}

impl<B: Backend> BackendEx for Cache<B> {
    fn invalidate_chunk(&self, chunk_id: usize) -> std::io::Result<()> {
        let _guard = self.drop_lock.write().unwrap();
        if (chunk_id * CHUNK_SIZE) >= self.size() as usize {
            return Ok(());
        }
        let bitmap_data = self.bitmap.as_mut_slice();
        let bit_at = chunk_id / 8;
        let bit_shift = chunk_id % 8;
        let raw_ptr: *mut u8 = &mut bitmap_data[bit_at] as *mut u8;
        let atomic_ref: &AtomicU8 = unsafe { &*(raw_ptr as *const AtomicU8) };
        if (atomic_ref.load(Acquire) & (1_u8 << bit_shift)) == 0 {
            return Ok(());
        }
        clear_bit(atomic_ref, bit_shift);
        // confirm data has already flush to disk
        self.bitmap.file.sync_data()?;
        let start = chunk_id * CHUNK_SIZE;
        let size = CHUNK_SIZE;
        info!(offset = start, length = size, "Invalidate cache.");
        punch_hole(&self.raw.file, start, size)
    }
}

#[cfg(test)]
mod tests;
