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

use crate::backend::{Backend, BackendEx};
use crate::utils::new_std_io_error;
use nydus_api::BackendConfigV2;
use nydus_storage::backend::{BlobBackend, BlobReader};
use nydus_storage::factory::BlobFactory;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io::{self, ErrorKind};
use std::path::Path;
use std::sync::Arc;

pub struct BackendReader {
    name: String,
    size: u64,
    reader: Arc<dyn BlobReader>,
}

impl BackendReader {
    fn new(name: &str, reader: Arc<dyn BlobReader>) -> io::Result<Self> {
        let size = reader.blob_size().map_err(new_std_io_error)?;
        Ok(Self {
            name: name.to_string(),
            size,
            reader,
        })
    }
}

impl Debug for BackendReader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "backend_reader:{}({})", self.name, self.size)
    }
}

impl Backend for BackendReader {
    fn size(&self) -> u64 {
        self.size
    }

    fn fetch(&self, off: usize, data: &mut [u8]) -> io::Result<usize> {
        self.reader.read(data, off as u64).map_err(new_std_io_error)
    }
}

impl BackendEx for BackendReader {
    fn invalidate_chunk(&self, _chunk_id: usize) -> io::Result<()> {
        Ok(())
    }
}

pub struct GeneralBackend {
    backend_cfg: BackendConfigV2,
    backend: Arc<dyn BlobBackend + Send + Sync>,
}

impl GeneralBackend {
    pub fn new<P: AsRef<Path>>(cfg_path: P) -> io::Result<Self> {
        let file = File::open(cfg_path)?;
        let cfg: BackendConfigV2 = serde_json::from_reader(file).map_err(new_std_io_error)?;
        if !cfg.validate() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "Invalid backend config",
            ));
        }
        let backend = BlobFactory::new_backend(&cfg, "distill_fs").map_err(new_std_io_error)?;
        Ok(Self {
            backend_cfg: cfg,
            backend,
        })
    }

    pub fn get_reader(&self, object: &str) -> io::Result<BackendReader> {
        let reader = self.backend.get_reader(object).map_err(new_std_io_error)?;
        BackendReader::new(object, reader)
    }

    pub fn backend_config(&self) -> &BackendConfigV2 {
        &self.backend_cfg
    }
}

impl Debug for GeneralBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "general_backend:{}", self.backend_cfg.backend_type)
    }
}
