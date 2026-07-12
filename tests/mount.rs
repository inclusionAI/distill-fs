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

#![cfg(target_os = "linux")]

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use fuse_backend_rs::api::filesystem::{Context, FileSystem, ZeroCopyWriter};

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();
}

struct VecWriter {
    data: Vec<u8>,
}

impl Write for VecWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.data.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl ZeroCopyWriter for VecWriter {
    fn available_bytes(&self) -> usize {
        usize::MAX
    }

    fn write_from(
        &mut self,
        _f: &mut dyn fuse_backend_rs::common::file_traits::FileReadWriteVolatile,
        _count: usize,
        _off: u64,
    ) -> io::Result<usize> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "write_from is not used by this test",
        ))
    }
}

struct MountGuard {
    child: Child,
    mountpoint: PathBuf,
}

impl MountGuard {
    fn spawn(args: &[&str], mountpoint: &Path) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_distill_fs"))
            .args(args)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        Self {
            child,
            mountpoint: mountpoint.to_path_buf(),
        }
    }

    fn wait_for_file(&mut self, path: &Path, expected: &[u8]) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(data) = fs::read(path) {
                assert_eq!(data, expected);
                return;
            }
            if let Some(status) = self.child.try_wait().unwrap() {
                panic!("distill_fs mount exited early with {status}");
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for mounted file {}",
                path.display()
            );
            thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for MountGuard {
    fn drop(&mut self) {
        let mountpoint = self.mountpoint.to_str().unwrap();
        let unmounted = [
            ("fusermount3", vec!["-u", mountpoint]),
            ("fusermount", vec!["-u", mountpoint]),
            ("umount", vec![mountpoint]),
        ]
        .into_iter()
        .any(|(program, args)| {
            Command::new(program)
                .args(args)
                .status()
                .is_ok_and(|status| status.success())
        });

        if !unmounted {
            eprintln!("warning: failed to unmount {}", self.mountpoint.display());
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
#[ignore = "requires Linux FUSE mount privileges"]
fn raw_mount_reads_file() {
    init_tracing();
    let temp = tempfile::TempDir::new().unwrap();
    let image = temp.path().join("source.raw");
    let mountpoint = temp.path().join("mnt");
    let expected = b"distill-fs raw mount\n";
    fs::write(&image, expected).unwrap();
    fs::create_dir(&mountpoint).unwrap();

    let mut mount = MountGuard::spawn(
        &[
            "mount",
            "--src",
            "local",
            "--name",
            "rootfs.raw",
            "--cache-file",
            image.to_str().unwrap(),
            "--mountpoint",
            mountpoint.to_str().unwrap(),
        ],
        &mountpoint,
    );
    mount.wait_for_file(&mountpoint.join("rootfs.raw"), expected);
}

fn build_nydus_fixture(
    nydus_image: &str,
    temp: &tempfile::TempDir,
    expected: &[u8],
) -> (PathBuf, PathBuf) {
    let source = temp.path().join("source");
    let artifacts = temp.path().join("artifacts");
    let bootstrap = artifacts.join("bootstrap");
    let output_json = temp.path().join("build.json");
    let config = temp.path().join("backend.json");
    fs::create_dir(&source).unwrap();
    fs::create_dir(&artifacts).unwrap();
    fs::write(source.join("hello.txt"), expected).unwrap();

    let status = Command::new(nydus_image)
        .args([
            "create",
            "--fs-version",
            "5",
            "--bootstrap",
            bootstrap.to_str().unwrap(),
            "--blob-dir",
            artifacts.to_str().unwrap(),
            "--output-json",
            output_json.to_str().unwrap(),
            source.to_str().unwrap(),
        ])
        .status()
        .unwrap();
    assert!(status.success(), "nydus-image create failed");

    fs::write(
        &config,
        format!(
            r#"{{"type":"localfs","localfs":{{"dir":{}}}}}"#,
            serde_json::to_string(artifacts.to_str().unwrap()).unwrap()
        ),
    )
    .unwrap();

    (bootstrap, config)
}

#[test]
#[ignore = "requires nydus-image v2.4.0"]
fn nydus_direct_reads_file() {
    init_tracing();
    let nydus_image = std::env::var("DISTILL_FS_NYDUS_IMAGE").expect("set DISTILL_FS_NYDUS_IMAGE");
    let temp = tempfile::TempDir::new().unwrap();
    let expected = b"distill-fs nydus mount\n";
    let (bootstrap, config) = build_nydus_fixture(&nydus_image, &temp, expected);
    let image =
        distill_fs::image::nydus::NydusImage::new(&bootstrap, &config, "", None, None).unwrap();

    let ctx = Context::new();
    let name = std::ffi::CString::new("hello.txt").unwrap();
    let entry = image
        .lookup(&ctx, fuse_backend_rs::abi::fuse_abi::ROOT_ID, &name)
        .unwrap();
    let mut writer = VecWriter { data: Vec::new() };
    let n = image
        .read(
            &ctx,
            entry.inode,
            0,
            &mut writer,
            expected.len() as u32,
            0,
            None,
            0,
        )
        .unwrap();
    assert_eq!(n, expected.len());
    assert_eq!(writer.data, expected);
}

#[test]
#[ignore = "requires Linux FUSE privileges and nydus-image v2.4.0"]
fn nydus_mount_reads_file() {
    init_tracing();
    let nydus_image = std::env::var("DISTILL_FS_NYDUS_IMAGE").expect("set DISTILL_FS_NYDUS_IMAGE");
    let temp = tempfile::TempDir::new().unwrap();
    let mountpoint = temp.path().join("mnt");
    let expected = b"distill-fs nydus mount\n";
    fs::create_dir(&mountpoint).unwrap();
    let (bootstrap, config) = build_nydus_fixture(&nydus_image, &temp, expected);

    let mut mount = MountGuard::spawn(
        &[
            "mount",
            "--src",
            "nydus",
            "--name",
            "test-image",
            "--bootstrap",
            bootstrap.to_str().unwrap(),
            "--cfg",
            config.to_str().unwrap(),
            "--mountpoint",
            mountpoint.to_str().unwrap(),
        ],
        &mountpoint,
    );
    mount.wait_for_file(&mountpoint.join("hello.txt"), expected);
}
