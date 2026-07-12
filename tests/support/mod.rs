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

#![allow(dead_code)]

use async_trait::async_trait;
use distill_fs::backend::chunkdb::CheckSum;
#[cfg(feature = "redis-integration-tests")]
use distill_fs::backend::chunkdb::CheckSumMethod;
use distill_fs::backend::peer::ChunkIndex;
#[cfg(feature = "redis-integration-tests")]
use redis::Commands;
use std::net::SocketAddr;
#[cfg(feature = "redis-integration-tests")]
use std::process;
#[cfg(feature = "redis-integration-tests")]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
#[cfg(feature = "redis-integration-tests")]
use std::sync::{MutexGuard, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

pub fn wait_until<F>(cond: F)
where
    F: FnMut() -> bool,
{
    wait_until_with_timeout(Duration::from_secs(3), cond);
}

pub fn wait_until_with_timeout<F>(timeout: Duration, mut cond: F)
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

#[cfg(feature = "redis-integration-tests")]
pub fn checksum_for(label: &str) -> CheckSum {
    CheckSum::from_data(label.as_bytes(), CheckSumMethod::Blake3)
}

#[derive(Debug, Default)]
pub struct TestChunkIndex {
    pub owners: Vec<SocketAddr>,
    registered: Arc<Mutex<Vec<CheckSum>>>,
    unregistered: Arc<Mutex<Vec<CheckSum>>>,
}

impl TestChunkIndex {
    pub fn with_owners(owners: Vec<SocketAddr>) -> Self {
        Self {
            owners,
            ..Default::default()
        }
    }

    pub fn registered(&self) -> Vec<CheckSum> {
        self.registered.lock().unwrap().clone()
    }

    pub fn unregistered(&self) -> Vec<CheckSum> {
        self.unregistered.lock().unwrap().clone()
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
}

#[cfg(feature = "redis-integration-tests")]
static REDIS_TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
#[cfg(feature = "redis-integration-tests")]
static REDIS_TEST_ID: AtomicUsize = AtomicUsize::new(1);
#[cfg(feature = "redis-integration-tests")]
fn redis_test_url() -> String {
    let url = std::env::var("DISTILL_FS_TEST_REDIS_URL")
        .expect("DISTILL_FS_TEST_REDIS_URL must point at a disposable non-zero Redis database");
    let url = url.trim();
    assert!(
        !url.is_empty(),
        "DISTILL_FS_TEST_REDIS_URL must not be empty"
    );

    let path = url
        .split_once("://")
        .map(|(_, rest)| rest)
        .and_then(|rest| rest.split_once('/').map(|(_, path)| path))
        .and_then(|path| path.split(['?', '#']).next())
        .filter(|path| !path.is_empty())
        .expect("Redis test URL must include an explicit database number");
    let database: u32 = path
        .parse()
        .expect("Redis test URL database must be a number");
    assert_ne!(
        database, 0,
        "Redis integration tests refuse to run against database zero"
    );

    url.to_string()
}

#[cfg(feature = "redis-integration-tests")]
fn try_flush_redis(url: &str) -> redis::RedisResult<()> {
    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_connection().unwrap();
    redis::cmd("FLUSHDB").query::<()>(&mut conn)
}

#[cfg(feature = "redis-integration-tests")]
pub struct RedisTestGuard {
    url: String,
    _guard: MutexGuard<'static, ()>,
}

#[cfg(feature = "redis-integration-tests")]
impl RedisTestGuard {
    pub fn acquire() -> Self {
        let url = redis_test_url();
        let guard = REDIS_TEST_MUTEX
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        try_flush_redis(&url).unwrap();
        Self { url, _guard: guard }
    }

    pub fn url(&self) -> &str {
        &self.url
    }
}

#[cfg(feature = "redis-integration-tests")]
impl Drop for RedisTestGuard {
    fn drop(&mut self) {
        let _ = try_flush_redis(&self.url);
    }
}

#[cfg(feature = "redis-integration-tests")]
pub fn unique_node_id(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        process::id(),
        REDIS_TEST_ID.fetch_add(1, Ordering::Relaxed)
    )
}

#[cfg(feature = "redis-integration-tests")]
pub fn wait_for_next_epoch_second() {
    let start = distill_fs::utils::now_epoch_secs();
    wait_until(|| distill_fs::utils::now_epoch_secs() > start);
}

#[cfg(feature = "redis-integration-tests")]
pub fn redis_set_string(url: &str, key: &str, value: &str, ttl_secs: u64) {
    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_connection().unwrap();
    let _: () = conn.set_ex(key, value, ttl_secs).unwrap();
}

#[cfg(feature = "redis-integration-tests")]
pub fn chunk_index_key(checksum: &CheckSum) -> String {
    format!("distill-fs:chunk-owner:{checksum}")
}

#[cfg(feature = "redis-integration-tests")]
pub fn redis_remove_owner(url: &str, checksum: &CheckSum, owner: SocketAddr) {
    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_connection().unwrap();
    let _: usize = conn
        .zrem(chunk_index_key(checksum), owner.to_string())
        .unwrap();
}
