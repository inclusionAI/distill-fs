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

#[cfg(target_os = "linux")]
use crate::backend::cache::Cache;
#[cfg(target_os = "linux")]
use crate::backend::chunkdb::CheckSumMethod;
use crate::backend::chunkdb::{CheckSum, ChunkDB};
#[cfg(target_os = "linux")]
use crate::backend::dedup::DedupReader;
use crate::backend::general::GeneralBackend;
use crate::backend::indexdb::IndexDB;
use crate::backend::peer::LocalChunkClient;
use crate::backend::Backend;
use crate::image::FsReadMetrics;
#[cfg(target_os = "linux")]
use crate::rate_limited_log;
#[cfg(target_os = "linux")]
use crate::utils::new_std_io_error;
use crate::utils::RateLimitedLog;
#[cfg(target_os = "linux")]
use fuse_backend_rs::abi::fuse_abi::statvfs64;
#[cfg(target_os = "linux")]
use fuse_backend_rs::abi::fuse_abi::{stat64, Attr, FsOptions, ROOT_ID};
#[cfg(target_os = "linux")]
use fuse_backend_rs::api::filesystem::{
    Context, DirEntry, Entry, GetxattrReply, ListxattrReply, OpenOptions, ZeroCopyWriter,
};
use nydus_api::ConfigV2;
use nydus_rafs::metadata::RafsSuper;
#[cfg(target_os = "linux")]
use nydus_rafs::metadata::{RafsInode, RafsInodeExt};
#[cfg(target_os = "linux")]
use nydus_rafs::metadata::{RafsInodeWalkAction, DOT, DOTDOT};
#[cfg(target_os = "linux")]
use nydus_storage::device::BlobChunkInfo;
use nydus_storage::device::BlobInfo;
#[cfg(target_os = "linux")]
use nydus_utils::compress;
#[cfg(target_os = "linux")]
use nydus_utils::digest::Algorithm as DigestAlgorithm;
use std::collections::HashMap;
#[cfg(target_os = "linux")]
use std::ffi::{CStr, OsStr, OsString};
use std::io::{self, ErrorKind};
#[cfg(target_os = "linux")]
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::atomic::AtomicU64;
#[cfg(target_os = "linux")]
use std::sync::atomic::Ordering;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;
#[cfg(target_os = "linux")]
use std::time::Duration;
#[cfg(target_os = "linux")]
use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(target_os = "linux")]
use tracing::error;
use tracing::info;
use tracing::warn;

const CHUNK_STORE_REQ_BUF_SIZE: usize = 4096;
const CHUNK_STORE_BATCH_SIZE: usize = 64;

struct ChunkStoreRequest {
    checksum: CheckSum,
    data: Vec<u8>,
}

struct ChunkStoreWorker {
    chunk_db: Arc<ChunkDB>,
    rx: Receiver<ChunkStoreRequest>,
}

impl ChunkStoreWorker {
    fn new(chunk_db: Arc<ChunkDB>, rx: Receiver<ChunkStoreRequest>) -> Self {
        Self { chunk_db, rx }
    }

    fn run(self) {
        let mut batch = Vec::with_capacity(CHUNK_STORE_BATCH_SIZE);
        let mut batch_data: Vec<Vec<u8>> = Vec::with_capacity(CHUNK_STORE_BATCH_SIZE);

        while let Ok(req) = self.rx.recv() {
            batch.push(req.checksum);
            batch_data.push(req.data);

            // Try to collect more requests without blocking
            while batch.len() < CHUNK_STORE_BATCH_SIZE {
                match self.rx.try_recv() {
                    Ok(req) => {
                        batch.push(req.checksum);
                        batch_data.push(req.data);
                    }
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => break,
                }
            }

            // Process the batch
            let chunks: Vec<(CheckSum, Vec<u8>)> =
                batch.drain(..).zip(batch_data.drain(..)).collect();

            if let Err(e) = self.chunk_db.add_chunks(chunks) {
                warn!(err = debug(e), "Failed to store chunk batch into chunkdb.");
            }
        }

        // Process any remaining requests in the batch
        if !batch.is_empty() {
            let chunks: Vec<(CheckSum, Vec<u8>)> =
                batch.drain(..).zip(batch_data.drain(..)).collect();

            if let Err(e) = self.chunk_db.add_chunks(chunks) {
                warn!(err = debug(e), "Failed to store chunk batch into chunkdb.");
            }
        }
    }
}

pub struct NydusImage {
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    sb: Arc<RafsSuper>,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    blobs: HashMap<u32, Arc<BlobInfo>>,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    readers: Mutex<HashMap<String, Arc<dyn Backend>>>,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    backend: Arc<GeneralBackend>,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    dedup_db: Option<(Arc<ChunkDB>, Arc<IndexDB>)>,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    local_chunk_client: Option<Arc<LocalChunkClient>>,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    store_tx: Option<SyncSender<ChunkStoreRequest>>,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    store_worker: Mutex<Option<thread::JoinHandle<()>>>,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    store_dropped_count: AtomicU64,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    store_warn_limiter: RateLimitedLog,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    cache_dir: String,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    i_uid: u32,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    i_gid: u32,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    i_time: u64,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    metrics: FsReadMetrics,
}

impl NydusImage {
    pub fn new<P: AsRef<Path>>(
        bootstrap: P,
        backend_cfg: P,
        cache_dir: &str,
        dedup_db: Option<(Arc<ChunkDB>, Arc<IndexDB>)>,
        local_chunk_client: Option<Arc<LocalChunkClient>>,
    ) -> anyhow::Result<Self> {
        if !backend_cfg.as_ref().is_file() {
            return Err(io::Error::new(ErrorKind::InvalidInput, "invalid backend cfg").into());
        }
        let mut config = ConfigV2::default();
        config.internal.blob_accessible.store(true, Relaxed);
        let t = std::time::Instant::now();
        let backend = Arc::new(GeneralBackend::new(backend_cfg)?);
        info!("GeneralBackend::new took {:?}", t.elapsed());
        config.backend = Some(backend.backend_config().clone());
        let t = std::time::Instant::now();
        let (sb, _reader) = RafsSuper::load_from_file(bootstrap, Arc::new(config), false)?;
        info!("RafsSuper::load_from_file took {:?}", t.elapsed());
        if sb.meta.is_v6() {
            return Err(io::Error::new(
                ErrorKind::Unsupported,
                "RAFS v6 is not supported by distill-fs yet; build RAFS v5 images",
            )
            .into());
        }
        let mut blobs = HashMap::new();
        for blob in sb.superblock.get_blob_infos() {
            blobs.insert(blob.blob_index(), blob);
        }
        let i_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let i_uid = unsafe { libc::geteuid() } as u32;
        let i_gid = unsafe { libc::getegid() } as u32;

        let (store_tx, store_worker) = if let Some((chunk_db, _)) = &dedup_db {
            let (tx, rx) = mpsc::sync_channel(CHUNK_STORE_REQ_BUF_SIZE);
            let worker = ChunkStoreWorker::new(chunk_db.clone(), rx);
            let handle = thread::spawn(move || worker.run());
            (Some(tx), Mutex::new(Some(handle)))
        } else {
            (None, Mutex::new(None))
        };

        Ok(Self {
            sb: Arc::new(sb),
            blobs,
            readers: Mutex::new(HashMap::new()),
            backend,
            dedup_db,
            local_chunk_client,
            store_tx,
            store_worker,
            store_dropped_count: AtomicU64::new(0),
            store_warn_limiter: RateLimitedLog::new(5),
            cache_dir: cache_dir.to_string(),
            i_uid,
            i_gid,
            i_time,
            metrics: FsReadMetrics::new("nydus"),
        })
    }

    #[cfg(target_os = "linux")]
    fn root_ino(&self) -> u64 {
        self.sb.superblock.root_ino()
    }

    #[cfg(target_os = "linux")]
    fn to_rafs_ino(&self, ino: u64) -> u64 {
        if ino == ROOT_ID {
            self.root_ino()
        } else {
            ino
        }
    }

    #[cfg(target_os = "linux")]
    fn get_blob(&self, index: u32) -> io::Result<Arc<BlobInfo>> {
        self.blobs
            .get(&index)
            .cloned()
            .ok_or_else(|| io::Error::new(ErrorKind::NotFound, "blob index not found"))
    }

    #[cfg(target_os = "linux")]
    fn get_blob_reader(&self, blob: &BlobInfo) -> io::Result<Arc<dyn Backend>> {
        let blob_id = blob.blob_id();
        let mut readers = self.readers.lock().unwrap();
        if let Some(reader) = readers.get(&blob_id) {
            return Ok(reader.clone());
        }

        let reader = self.backend.get_reader(&blob_id).inspect_err(|e| {
            error!(
                blob_id = blob_id,
                err = debug(e),
                "Failed to get blob reader."
            );
        })?;
        if self.cache_dir.is_empty() {
            let direct_reader = Arc::new(reader);
            readers.insert(blob_id, direct_reader.clone());
            Ok(direct_reader)
        } else {
            let cache_file = format!("{}/{}", self.cache_dir, blob_id);
            let reader_with_cache =
                Arc::new(Cache::new(reader, &cache_file).map_err(new_std_io_error)?);
            let final_reader = if let Some((chunk_db, index_db)) = &self.dedup_db {
                let dedup = DedupReader::new(
                    reader_with_cache,
                    chunk_db.clone(),
                    index_db.clone(),
                    &blob_id,
                    self.local_chunk_client.clone(),
                )
                .map_err(new_std_io_error)?;
                Arc::new(dedup) as Arc<dyn Backend>
            } else {
                reader_with_cache as Arc<dyn Backend>
            };
            readers.insert(blob_id, final_reader.clone());
            Ok(final_reader)
        }
    }

    #[cfg(target_os = "linux")]
    fn checksum_from_chunk(
        &self,
        chunk: &dyn BlobChunkInfo,
        blob: &BlobInfo,
    ) -> io::Result<CheckSum> {
        let method = match blob.digester() {
            DigestAlgorithm::Blake3 => CheckSumMethod::Blake3,
            DigestAlgorithm::Sha256 => CheckSumMethod::Sha256,
        };
        CheckSum::new(chunk.chunk_id().as_ref(), method)
    }

    #[cfg(target_os = "linux")]
    fn read_blob_range(
        &self,
        reader: &dyn Backend,
        offset: u64,
        size: usize,
    ) -> io::Result<Vec<u8>> {
        let mut buf = vec![0_u8; size];
        let mut read = 0;
        while read < size {
            let n = reader.fetch((offset as usize) + read, &mut buf[read..])?;
            if n == 0 {
                return Err(io::Error::new(ErrorKind::UnexpectedEof, "short read"));
            }
            read += n;
        }
        Ok(buf)
    }

    #[cfg(target_os = "linux")]
    fn read_chunk_data(&self, chunk: &dyn BlobChunkInfo, blob: &BlobInfo) -> io::Result<Vec<u8>> {
        if chunk.is_encrypted() {
            return Err(io::Error::new(
                ErrorKind::Unsupported,
                "encrypted chunk is not supported",
            ));
        }
        if chunk.is_batch() {
            return Err(io::Error::new(
                ErrorKind::Unsupported,
                "batch chunk is not supported",
            ));
        }
        let compressed_size = chunk.compressed_size() as usize;
        let uncompressed_size = chunk.uncompressed_size() as usize;
        if uncompressed_size == 0 {
            return Ok(Vec::new());
        }
        if compressed_size == 0 {
            return Ok(vec![0_u8; uncompressed_size]);
        }

        let reader = self.get_blob_reader(blob)?;
        let mut compressed =
            self.read_blob_range(reader.as_ref(), chunk.compressed_offset(), compressed_size)?;

        if !chunk.is_compressed() {
            if compressed.len() < uncompressed_size {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "compressed data is shorter than expected",
                ));
            }
            compressed.truncate(uncompressed_size);
            return Ok(compressed);
        }

        let mut data = vec![0_u8; uncompressed_size];
        let sz = compress::decompress(&compressed, &mut data, blob.compressor()).map_err(|e| {
            io::Error::new(ErrorKind::InvalidData, format!("decompress failed: {}", e))
        })?;
        if sz != uncompressed_size {
            data.truncate(sz);
        }
        Ok(data)
    }

    #[cfg(target_os = "linux")]
    fn store_chunk_bg(&self, cs: CheckSum, data: Vec<u8>) {
        if let Some(tx) = &self.store_tx {
            let req = ChunkStoreRequest { checksum: cs, data };
            if let Err(_e) = tx.try_send(req) {
                let dropped = self.store_dropped_count.fetch_add(1, Ordering::Relaxed) + 1;
                rate_limited_log!(self.store_warn_limiter, {
                    warn!(
                        dropped_count = dropped,
                        queue_size = CHUNK_STORE_REQ_BUF_SIZE,
                        "Chunk store queue full, dropping requests."
                    );
                    self.store_dropped_count.store(0, Ordering::Relaxed);
                });
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn read_from_chunkdb(
        &self,
        cs: &CheckSum,
        start: usize,
        len: usize,
        w: &mut dyn ZeroCopyWriter,
    ) -> io::Result<Option<usize>> {
        if let Some((chunk_db, _)) = &self.dedup_db {
            match chunk_db.with_chunk_range(cs, start, len, |slice| w.write(slice)) {
                Ok(Some(n)) => Ok(Some(n)),
                Ok(None) => Ok(None),
                Err(e) => {
                    warn!(err = debug(e), "Failed to read chunk from chunkdb.");
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }

    #[cfg(target_os = "linux")]
    fn get_inode_entry(&self, inode: &dyn RafsInode) -> Entry {
        let mut entry = inode.get_entry();
        let ino = inode.ino();
        if ino == self.root_ino() {
            entry.inode = ROOT_ID;
            entry.attr.st_ino = ROOT_ID;
        } else {
            entry.inode = ino;
            entry.attr.st_ino = ino;
        }
        if !self.sb.meta.explicit_uidgid() {
            entry.attr.st_uid = self.i_uid;
            entry.attr.st_gid = self.i_gid;
        }
        if entry.attr.st_mtime == 0 {
            entry.attr.st_atime = self.i_time as i64;
            entry.attr.st_ctime = self.i_time as i64;
            entry.attr.st_mtime = self.i_time as i64;
        }
        entry.attr_timeout = self.sb.meta.attr_timeout;
        entry.entry_timeout = self.sb.meta.entry_timeout;
        entry
    }

    #[cfg(target_os = "linux")]
    fn get_inode_attr(&self, ino: u64) -> io::Result<Attr> {
        let rafs_ino = self.to_rafs_ino(ino);
        let inode = self.sb.get_extended_inode(rafs_ino, false)?;
        let mut attr = inode.get_attr();
        if rafs_ino == self.root_ino() {
            attr.ino = ROOT_ID;
        }
        if !self.sb.meta.explicit_uidgid() {
            attr.uid = self.i_uid;
            attr.gid = self.i_gid;
        }
        if attr.mtime == 0 {
            attr.atime = self.i_time;
            attr.ctime = self.i_time;
            attr.mtime = self.i_time;
        }
        Ok(attr)
    }

    #[cfg(target_os = "linux")]
    fn do_readdir(
        &self,
        ino: u64,
        size: u32,
        offset: u64,
        add_entry: &mut dyn FnMut(DirEntry) -> io::Result<usize>,
    ) -> io::Result<()> {
        if size == 0 {
            return Ok(());
        }
        let parent = self.sb.get_inode(self.to_rafs_ino(ino), false)?;
        if !parent.is_dir() {
            return Err(io::Error::from_raw_os_error(libc::ENOTDIR));
        }

        let mut handler = |_inode, name: OsString, ino, offset| match add_entry(DirEntry {
            ino,
            offset,
            type_: 0,
            name: name.as_os_str().as_bytes(),
        }) {
            Ok(0) => Ok(RafsInodeWalkAction::Break),
            Ok(_) => Ok(RafsInodeWalkAction::Continue),
            Err(e) => Err(e),
        };

        parent.walk_children_inodes(offset, &mut handler)?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn xattr_supported(&self) -> bool {
        self.sb.meta.has_xattr()
    }

    #[cfg(target_os = "linux")]
    fn check_read_range(&self, inode: &dyn RafsInode, size: u32, offset: u64) -> io::Result<u64> {
        if offset.checked_add(size as u64).is_none() {
            return Err(io::Error::new(ErrorKind::InvalidInput, "invalid range"));
        }
        if !inode.is_reg() {
            return Err(io::Error::from_raw_os_error(libc::EISDIR));
        }
        let inode_size = inode.size();
        if size == 0 || offset >= inode_size {
            return Ok(0);
        }
        Ok(std::cmp::min(size as u64, inode_size - offset))
    }

    #[allow(clippy::too_many_arguments)]
    #[cfg(target_os = "linux")]
    fn read_chunk_into_writer(
        &self,
        inode: &dyn RafsInodeExt,
        chunk_idx: u64,
        offset: u64,
        chunk_size: u64,
        remaining: &mut usize,
        total: &mut usize,
        w: &mut dyn ZeroCopyWriter,
    ) -> io::Result<()> {
        let chunk_info = inode.get_chunk_info(chunk_idx as u32)?;
        let blob = self.get_blob(chunk_info.blob_index())?;
        let chunk_base = chunk_idx * chunk_size;
        let chunk_offset = if offset > chunk_base {
            (offset - chunk_base) as usize
        } else {
            0
        };
        let chunk_uncompressed = chunk_info.uncompressed_size() as usize;
        if chunk_offset >= chunk_uncompressed {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "invalid chunk offset",
            ));
        }
        let wanted = (*remaining).min(chunk_uncompressed - chunk_offset);
        let cs = self.checksum_from_chunk(chunk_info.as_ref(), &blob)?;

        if let Some(n) = self.read_from_chunkdb(&cs, chunk_offset, wanted, w)? {
            *total += n;
            *remaining -= n;
            return Ok(());
        }

        let data = self
            .read_chunk_data(chunk_info.as_ref(), &blob)
            .inspect_err(|e| {
                error!(err = debug(e), "Failed to read chunk data.");
            })?;
        let end = chunk_offset + wanted;
        if end > data.len() {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "chunk data is shorter than expected",
            ));
        }
        let n = w.write(&data[chunk_offset..end]).inspect_err(|e| {
            error!(
                err = debug(e),
                "Failed to write chunk data to FUSE response."
            );
        })?;
        *total += n;
        *remaining -= n;
        if data.len() == chunk_uncompressed {
            self.store_chunk_bg(cs, data);
        }
        Ok(())
    }
}

#[cfg(target_os = "linux")]
impl fuse_backend_rs::api::filesystem::FileSystem for NydusImage {
    type Inode = u64;
    type Handle = u64;

    fn init(&self, capable: FsOptions) -> io::Result<FsOptions> {
        let mut opts = FsOptions::empty();
        opts.insert(FsOptions::ASYNC_READ);
        opts.insert(FsOptions::ASYNC_DIO);
        opts.insert(FsOptions::BIG_WRITES);
        opts.insert(FsOptions::MAX_PAGES);
        if capable.contains(FsOptions::ZERO_MESSAGE_OPEN) {
            opts.insert(FsOptions::ZERO_MESSAGE_OPEN);
        }
        if capable.contains(FsOptions::ZERO_MESSAGE_OPENDIR) {
            opts.insert(FsOptions::ZERO_MESSAGE_OPENDIR);
        }
        Ok(opts)
    }

    fn lookup(&self, _ctx: &Context, parent: Self::Inode, name: &CStr) -> io::Result<Entry> {
        let target = OsStr::from_bytes(name.to_bytes());
        let rafs_parent = self.to_rafs_ino(parent);
        let parent_inode = self.sb.get_inode(rafs_parent, false)?;
        if !parent_inode.is_dir() {
            return Err(io::Error::from_raw_os_error(libc::ENOTDIR));
        }

        if target == DOT || (rafs_parent == self.root_ino() && target == DOTDOT) {
            return Ok(self.get_inode_entry(parent_inode.as_ref()));
        }
        if target == DOTDOT {
            let parent = self.sb.get_extended_inode(parent_inode.ino(), false)?;
            let ino = parent.parent();
            let inode = self.sb.get_inode(ino, false)?;
            return Ok(self.get_inode_entry(inode.as_ref()));
        }

        match parent_inode.get_child_by_name(target) {
            Ok(inode) => Ok(self.get_inode_entry(inode.as_inode())),
            Err(_) => Err(io::Error::from_raw_os_error(libc::ENOENT)),
        }
    }

    fn getattr(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Option<Self::Handle>,
    ) -> io::Result<(stat64, Duration)> {
        let attr = self.get_inode_attr(inode)?;
        Ok((attr.into(), self.sb.meta.attr_timeout))
    }

    fn readlink(&self, _ctx: &Context, ino: u64) -> io::Result<Vec<u8>> {
        let inode = self.sb.get_inode(self.to_rafs_ino(ino), false)?;
        let target = inode.get_symlink()?;
        Ok(target.as_bytes().to_vec())
    }

    fn open(
        &self,
        _ctx: &Context,
        _inode: Self::Inode,
        _flags: u32,
        _fuse_flags: u32,
    ) -> io::Result<(Option<Self::Handle>, OpenOptions, Option<u32>)> {
        Ok((None, OpenOptions::KEEP_CACHE, None))
    }

    fn read(
        &self,
        _ctx: &Context,
        ino: Self::Inode,
        _handle: Self::Handle,
        w: &mut dyn ZeroCopyWriter,
        size: u32,
        offset: u64,
        _lock_owner: Option<u64>,
        _flags: u32,
    ) -> io::Result<usize> {
        let begin = Instant::now();
        let result = (|| -> io::Result<usize> {
            let inode = self.sb.get_extended_inode(self.to_rafs_ino(ino), false)?;
            let read_size = self.check_read_range(inode.as_inode(), size, offset)?;
            if read_size == 0 {
                return Ok(0);
            }
            let chunk_size = self.sb.meta.chunk_size as u64;
            let start_chunk = offset / chunk_size;
            let end_chunk = (offset + read_size - 1) / chunk_size;
            let mut total = 0_usize;
            let mut remaining = read_size as usize;
            for chunk_idx in start_chunk..=end_chunk {
                self.read_chunk_into_writer(
                    inode.as_ref(),
                    chunk_idx,
                    offset,
                    chunk_size,
                    &mut remaining,
                    &mut total,
                    w,
                )?;
                if remaining == 0 {
                    break;
                }
            }
            Ok(total)
        })();
        let (status, bytes) = match &result {
            Ok(n) => ("ok", *n),
            Err(_) => ("error", 0),
        };
        self.metrics
            .record_read(status, begin.elapsed().as_secs_f64() * 1000.0, bytes);
        result
    }

    fn release(
        &self,
        _ctx: &Context,
        _inode: Self::Inode,
        _flags: u32,
        _handle: Self::Handle,
        _flush: bool,
        _flock_release: bool,
        _lock_owner: Option<u64>,
    ) -> io::Result<()> {
        Ok(())
    }

    fn statfs(&self, _ctx: &Context, _inode: Self::Inode) -> io::Result<statvfs64> {
        let mut st: statvfs64 = unsafe { std::mem::zeroed() };
        st.f_namemax = 255;
        st.f_bsize = 512;
        st.f_fsid = self.sb.meta.magic as u64;
        #[cfg(target_os = "macos")]
        {
            st.f_files = self.sb.meta.inodes_count as u32;
        }
        #[cfg(target_os = "linux")]
        {
            st.f_files = self.sb.meta.inodes_count;
        }
        Ok(st)
    }

    fn getxattr(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        name: &CStr,
        size: u32,
    ) -> io::Result<GetxattrReply> {
        if !self.xattr_supported() {
            return Err(io::Error::from_raw_os_error(libc::ENOSYS));
        }
        let name = OsStr::from_bytes(name.to_bytes());
        let inode = self.sb.get_inode(self.to_rafs_ino(inode), false)?;
        let value = inode.get_xattr(name)?;
        match value {
            Some(value) => match size {
                0 => Ok(GetxattrReply::Count((value.len() + 1) as u32)),
                x if x < value.len() as u32 => Err(io::Error::from_raw_os_error(libc::ERANGE)),
                _ => Ok(GetxattrReply::Value(value)),
            },
            None => Err(io::Error::from_raw_os_error(libc::ENODATA)),
        }
    }

    fn listxattr(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        size: u32,
    ) -> io::Result<ListxattrReply> {
        if !self.xattr_supported() {
            return Err(io::Error::from_raw_os_error(libc::ENOSYS));
        }
        let inode = self.sb.get_inode(self.to_rafs_ino(inode), false)?;
        let mut count = 0;
        let mut buf = Vec::new();
        for mut name in inode.get_xattrs()? {
            count += name.len() + 1;
            if size != 0 {
                buf.append(&mut name);
                buf.push(0);
            }
        }
        match size {
            0 => Ok(ListxattrReply::Count(count as u32)),
            x if x < count as u32 => Err(io::Error::from_raw_os_error(libc::ERANGE)),
            _ => Ok(ListxattrReply::Names(buf)),
        }
    }

    fn readdir(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Self::Handle,
        size: u32,
        offset: u64,
        add_entry: &mut dyn FnMut(DirEntry) -> io::Result<usize>,
    ) -> io::Result<()> {
        self.do_readdir(inode, size, offset, add_entry)
    }

    fn readdirplus(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Self::Handle,
        size: u32,
        offset: u64,
        add_entry: &mut dyn FnMut(DirEntry, Entry) -> io::Result<usize>,
    ) -> io::Result<()> {
        self.do_readdir(inode, size, offset, &mut |dir_entry| {
            let inode = self.sb.get_inode(dir_entry.ino, false)?;
            add_entry(dir_entry, self.get_inode_entry(inode.as_ref()))
        })
    }
}

impl Drop for NydusImage {
    fn drop(&mut self) {
        // Drop the sender to signal the worker thread to exit
        drop(self.store_tx.take());

        // Wait for the worker thread to finish
        if let Ok(mut guard) = self.store_worker.lock() {
            if let Some(handle) = guard.take() {
                let _ = handle.join();
            }
        }
    }
}
