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

use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn cli_help_smoke() {
    let output = Command::new(env!("CARGO_BIN_EXE_distill_fs"))
        .arg("--help")
        .output()
        .unwrap();

    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("dedup, on_demand, readonly fs"));
}

#[test]
fn cli_subcommand_help_smoke() {
    let output = Command::new(env!("CARGO_BIN_EXE_distill_fs"))
        .args(["serve-chunk", "--help"])
        .output()
        .unwrap();

    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("--listen-port"));
}

#[test]
fn gc_chunk_help_mentions_chunk_server_sock() {
    let output = Command::new(env!("CARGO_BIN_EXE_distill_fs"))
        .args(["gc-chunk", "--help"])
        .output()
        .unwrap();

    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("--chunk-server-sock"));
}

#[test]
fn serve_chunk_starts_without_external_services() {
    let temp = tempfile::TempDir::new().unwrap();
    let socket = temp.path().join("chunkserver.sock");
    let mut child = Command::new(env!("CARGO_BIN_EXE_distill_fs"))
        .args([
            "serve-chunk",
            "--chunk-db-dir",
            temp.path().to_str().unwrap(),
            "--listen-port",
            "0",
            "--chunk-server-sock",
            socket.to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    while !socket.exists() && Instant::now() < deadline {
        if let Some(status) = child.try_wait().unwrap() {
            let stderr = child
                .stderr
                .take()
                .map(|mut stderr| {
                    use std::io::Read;
                    let mut output = String::new();
                    stderr.read_to_string(&mut output).unwrap();
                    output
                })
                .unwrap_or_default();
            panic!("serve-chunk exited early with {status}: {stderr}");
        }
        thread::sleep(Duration::from_millis(20));
    }

    assert!(
        socket.exists(),
        "serve-chunk did not create its Unix socket"
    );
    child.kill().unwrap();
    child.wait().unwrap();
}

#[test]
fn gc_chunk_dry_run_succeeds_without_chunk_server() {
    let temp = tempfile::TempDir::new().unwrap();
    let db = distill_fs::backend::chunkdb::ChunkDB::new(temp.path()).unwrap();
    let data = b"keep-me".to_vec();
    let checksum = distill_fs::backend::chunkdb::CheckSum::from_data(
        &data,
        distill_fs::backend::chunkdb::CheckSumMethod::Blake3,
    );
    db.add_chunk(&checksum, data).unwrap();
    drop(db);

    let output = Command::new(env!("CARGO_BIN_EXE_distill_fs"))
        .args([
            "gc-chunk",
            "--chunk-db-dir",
            temp.path().to_str().unwrap(),
            "--dry-run",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "gc-chunk --dry-run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let db = distill_fs::backend::chunkdb::ChunkDB::new(temp.path()).unwrap();
    assert_eq!(db.get_chunk(&checksum).unwrap(), Some(b"keep-me".to_vec()));
}

#[test]
fn cli_version_matches_package_version() {
    let output = Command::new(env!("CARGO_BIN_EXE_distill_fs"))
        .arg("--version")
        .output()
        .unwrap();

    assert!(output.status.success());
    let version = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        version.trim(),
        concat!("distill_fs ", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn cli_stats_chunk_outputs_json_to_stdout() {
    let temp = tempfile::TempDir::new().unwrap();
    // stats-chunk on an empty (non-initialized) directory may fail,
    // but on an initialized one it should output JSON to stdout.
    // Initialize LMDB by opening ChunkDB.
    drop(distill_fs::backend::chunkdb::ChunkDB::new(temp.path()).unwrap());

    let output = Command::new(env!("CARGO_BIN_EXE_distill_fs"))
        .args([
            "stats-chunk",
            "--chunk-db-dir",
            temp.path().to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("stdout should be valid JSON");
    assert!(parsed["storage"]["total_size_bytes"].as_u64().is_some());
    assert!(parsed["readers"]["max"].as_u64().is_some());
}
