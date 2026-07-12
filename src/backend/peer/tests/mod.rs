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

use super::*;
use crate::backend::chunkdb::CheckSumMethod;
use async_trait::async_trait;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::net::TcpStream as StdTcpStream;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::sync::{Arc, Mutex};
use std::thread;
use tempfile::TempDir;

mod circuit_breaker;
mod metrics;
mod pool;
mod protocol;
mod refresh;
mod selection;

fn wait_until<F>(cond: F)
where
    F: FnMut() -> bool,
{
    wait_until_with_timeout(Duration::from_secs(3), cond);
}

fn wait_until_with_timeout<F>(timeout: Duration, mut cond: F)
where
    F: FnMut() -> bool,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("condition not met before timeout");
}

fn short_pool_config() -> ConnectionPoolConfig {
    ConnectionPoolConfig {
        min_idle: 1,
        max_size: 8,
        idle_ttl: Duration::from_millis(40),
    }
}

#[derive(Debug, Default)]
struct TestChunkIndex {
    owners: Vec<SocketAddr>,
    registered: Arc<Mutex<Vec<CheckSum>>>,
    unregistered: Arc<Mutex<Vec<CheckSum>>>,
    refresh_result: Option<usize>,
    refresh_spread_over: Arc<Mutex<Vec<Duration>>>,
}

impl TestChunkIndex {
    fn registered(&self) -> Vec<CheckSum> {
        self.registered.lock().unwrap().clone()
    }

    fn unregistered(&self) -> Vec<CheckSum> {
        self.unregistered.lock().unwrap().clone()
    }

    fn refresh_spreads(&self) -> Vec<Duration> {
        self.refresh_spread_over.lock().unwrap().clone()
    }
}

#[async_trait]
impl ChunkIndex for TestChunkIndex {
    async fn lookup_owners(&self, _cs: &CheckSum) -> anyhow::Result<Vec<SocketAddr>> {
        Ok(self.owners.clone())
    }

    async fn register(&self, cs: &CheckSum) -> anyhow::Result<()> {
        self.registered.lock().unwrap().push(*cs);
        Ok(())
    }

    async fn register_batch(&self, checksums: &[CheckSum]) -> anyhow::Result<()> {
        self.registered.lock().unwrap().extend_from_slice(checksums);
        Ok(())
    }

    async fn unregister(&self, cs: &CheckSum) -> anyhow::Result<()> {
        self.unregistered.lock().unwrap().push(*cs);
        Ok(())
    }

    async fn refresh_registered(&self, spread_over: Duration) -> anyhow::Result<Option<usize>> {
        self.refresh_spread_over.lock().unwrap().push(spread_over);
        Ok(self.refresh_result)
    }
}
