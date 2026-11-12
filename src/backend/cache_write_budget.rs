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

use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

const LOCK_FILE_NAME: &str = ".cache-write-budget";
const SLOT_RETRY_DELAY: Duration = Duration::from_millis(50);

/// All daemons sharing a ChunkDB use the same number of equal-size slots.
/// Each daemon holds a slot only while it has pending cache writes.
pub(crate) struct CacheWriteBudget {
    path: PathBuf,
    slot_count: usize,
    slot_bytes: usize,
    state: Mutex<SlotState>,
}

#[derive(Default)]
struct SlotState {
    slot: Weak<Slot>,
    retry_after: Option<Instant>,
}

struct Slot {
    // Closing this open file description releases the slot, including on OOM.
    _file: File,
    used_bytes: AtomicUsize,
}

pub(crate) struct CacheWritePermit {
    slot: Arc<Slot>,
    bytes: usize,
}

impl Drop for CacheWritePermit {
    fn drop(&mut self) {
        self.slot
            .used_bytes
            .fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

impl CacheWriteBudget {
    pub(crate) fn new(
        chunk_db_dir: &Path,
        slot_count: usize,
        slot_bytes: usize,
    ) -> io::Result<Self> {
        if slot_count == 0
            || slot_bytes == 0
            || libc::off_t::try_from(slot_count).is_err()
            || slot_count.checked_mul(slot_bytes).is_none()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid cache write budget",
            ));
        }

        let path = chunk_db_dir.join(LOCK_FILE_NAME);
        // Keep the file at a stable path so all daemons lock the same inode.
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path)?;

        Ok(Self {
            path,
            slot_count,
            slot_bytes,
            state: Mutex::default(),
        })
    }

    pub(crate) fn try_reserve(&self, bytes: usize) -> Option<CacheWritePermit> {
        if bytes == 0 || bytes > self.slot_bytes {
            return None;
        }

        let mut state = self.state.try_lock().ok()?;
        let slot = if let Some(slot) = state.slot.upgrade() {
            slot
        } else {
            if state
                .retry_after
                .is_some_and(|retry| Instant::now() < retry)
            {
                return None;
            }
            // Avoid scanning all slots on every cache miss while they are busy.
            let Some(slot) = self.try_lock_slot() else {
                state.retry_after = Some(Instant::now() + SLOT_RETRY_DELAY);
                return None;
            };
            let slot = Arc::new(slot);
            state.slot = Arc::downgrade(&slot);
            state.retry_after = None;
            slot
        };

        slot.used_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(bytes)
                    .filter(|total| *total <= self.slot_bytes)
            })
            .ok()?;
        Some(CacheWritePermit { slot, bytes })
    }

    fn try_lock_slot(&self) -> Option<Slot> {
        // A distinct open file description is required for each slot holder.
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&self.path)
            .ok()?;
        let start = std::process::id() as usize % self.slot_count;
        // The fields are integer values, and zero is required for l_pid.
        let mut lock: libc::flock = unsafe { std::mem::zeroed() };
        lock.l_type = libc::F_WRLCK as _;
        lock.l_whence = libc::SEEK_SET as _;
        lock.l_len = 1;
        for index in (start..self.slot_count).chain(0..start) {
            lock.l_start = index as libc::off_t;
            // file and lock remain valid for the duration of the call.
            let result =
                unsafe { libc::fcntl(file.as_raw_fd(), libc::F_OFD_SETLK, &lock as *const _) };
            if result == 0 {
                return Some(Slot {
                    _file: file,
                    used_bytes: AtomicUsize::new(0),
                });
            }
            if io::Error::last_os_error().raw_os_error() != Some(libc::EAGAIN) {
                break;
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::{Child, Command, Stdio};
    use std::thread;

    fn wait_for_slot(budget: &CacheWriteBudget, bytes: usize) -> CacheWritePermit {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if let Some(permit) = budget.try_reserve(bytes) {
                return permit;
            }
            assert!(Instant::now() < deadline, "slot did not become available");
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn reservations_hold_the_slot_until_the_last_drop() {
        let dir = tempfile::tempdir().unwrap();
        let budget = CacheWriteBudget::new(dir.path(), 1, 8).unwrap();
        let other = CacheWriteBudget::new(dir.path(), 1, 8).unwrap();
        assert!(budget.try_reserve(9).is_none());

        let first = budget.try_reserve(3).unwrap();
        let second = budget.try_reserve(5).unwrap();
        assert!(budget.try_reserve(1).is_none());
        assert!(other.try_reserve(1).is_none());

        drop(first);
        assert!(budget.try_reserve(4).is_none());
        drop(budget);
        assert!(other.try_reserve(1).is_none());
        drop(second);
        let _permit = wait_for_slot(&other, 8);
    }

    #[test]
    fn defers_rescanning_after_slot_acquisition_fails() {
        let dir = tempfile::tempdir().unwrap();
        let budget = CacheWriteBudget::new(dir.path(), 1, 8).unwrap();
        let other = CacheWriteBudget::new(dir.path(), 1, 8).unwrap();
        let permit = budget.try_reserve(8).unwrap();
        assert!(other.try_reserve(8).is_none());
        let mut state = other.state.lock().unwrap();
        assert!(state.retry_after.is_some());
        state.retry_after = Some(Instant::now() + Duration::from_secs(10));
        drop(state);

        drop(permit);
        assert!(other.try_reserve(8).is_none());
        other.state.lock().unwrap().retry_after = Some(Instant::now());
        // A concurrent test's child can inherit the OFD until exec closes it.
        let _permit = wait_for_slot(&other, 8);
    }

    struct ChildGuard(Child);

    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[test]
    fn shares_budget_across_processes_and_recovers_after_kill() {
        const CHILD_DIR: &str = "DISTILL_FS_CACHE_WRITE_BUDGET_TEST_CHILD_DIR";
        if let Some(dir) = std::env::var_os(CHILD_DIR) {
            let dir = PathBuf::from(dir);
            let budget = CacheWriteBudget::new(&dir, 2, 8).unwrap();
            let _permit = budget.try_reserve(8).unwrap();
            File::create(dir.join("ready")).unwrap();
            // Hold the slot until the parent kills this process or exits.
            let _ = io::stdin().read_line(&mut String::new());
            return;
        }

        let dir = tempfile::tempdir().unwrap();
        let mut child = ChildGuard(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "backend::cache_write_budget::tests::shares_budget_across_processes_and_recovers_after_kill",
                ])
                .env(CHILD_DIR, dir.path())
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .spawn()
                .unwrap(),
        );

        let deadline = Instant::now() + Duration::from_secs(10);
        while !dir.path().join("ready").is_file() {
            assert!(child.0.try_wait().unwrap().is_none(), "child exited early");
            assert!(Instant::now() < deadline, "child did not reserve a slot");
            thread::sleep(Duration::from_millis(10));
        }

        let budget = CacheWriteBudget::new(dir.path(), 2, 8).unwrap();
        let other = CacheWriteBudget::new(dir.path(), 2, 8).unwrap();
        let _permit = budget.try_reserve(8).unwrap();
        assert!(budget.try_reserve(1).is_none());
        assert!(other.try_reserve(1).is_none());

        child.0.kill().unwrap();
        child.0.wait().unwrap();
        let _recovered = wait_for_slot(&other, 8);
    }
}
