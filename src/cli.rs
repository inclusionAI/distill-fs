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

use crate::backend::cache::Cache;
use crate::backend::chunkdb::{ChunkDB, ChunkIndexControl, GcWorker};
use crate::backend::general::GeneralBackend;
use crate::backend::indexdb::IndexDB;
use crate::backend::peer::{
    build_discovery, default_chunk_server_socket, default_node_id, ChunkIndex, ChunkServer,
    LocalChunkClient, PeerClient, PeerRuntime, RedisChunkIndex, SyncChunkIndexControl,
};
use crate::backend::{Backend, BackendEx};
use crate::fs::mount_fs;
use crate::image::nydus::NydusImage;
use crate::image::raw::RawImage;
use crate::log_rotation::RotatingFileWriter;
use clap::{Args, Parser, Subcommand, ValueEnum};
use daemonize::Daemonize;
use opentelemetry::global;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::Resource;
use std::io;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::info;

const DEFAULT_CHUNK_INDEX_TTL_SECS: u64 = 12 * 60 * 60;
const DEFAULT_TOKIO_WORKER_THREADS: usize = 8;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum Source {
    Local,
    Oss,
    Nydus,
}

impl Source {
    fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Oss => "oss",
            Self::Nydus => "nydus",
        }
    }
}

#[derive(Args)]
struct FsOptions {
    /// In production, we daemonize the process to ensure it keeps running
    /// even if the controlling process (e.g., a shell or deployment script)
    /// restarts or is upgraded. We also configure a custom log path and a PID file.
    #[arg(long, default_value_t = false)]
    daemon: bool,
    #[arg(long, default_value = "")]
    pid_file: String,
    #[arg(long, default_value = "")]
    log_file: String,
    /// for oss: object name,
    /// for local: local file name
    #[arg(long)]
    name: String,
    /// FUSE mountpoint dir
    #[arg(long)]
    mountpoint: String,
    /// for oss: local cached file path,
    /// for local: local file path
    #[arg(long, default_value = "")]
    cache_file: String,
    /// for nydus: cache directory for blob files
    #[arg(long, default_value = "")]
    cache_dir: String,
    /// for oss: oss config file path,
    /// for local: useless
    /// for nydus: backend config file path
    #[arg(long, default_value = "")]
    cfg: String,
    #[arg(long, value_enum)]
    src: Source,
    #[arg(long, default_value_t = 4)]
    fuse_worker_num: u32,
    #[arg(long, default_value = "")]
    chunk_db_dir: String,
    #[arg(long, default_value = "")]
    image_meta_dir: String,
    /// for nydus: bootstrap file path
    #[arg(long, default_value = "")]
    bootstrap: String,
    #[arg(long, default_value = "")]
    chunk_server_sock: String,
    #[arg(long, default_value_t = 1000)]
    chunk_server_timeout_ms: u64,
    #[arg(long, default_value_t = DEFAULT_TOKIO_WORKER_THREADS)]
    tokio_worker_threads: usize,
    #[arg(long, default_value = "")]
    otel_endpoint: String,
}

#[derive(Args)]
struct ServeChunkOptions {
    #[arg(long, default_value_t = false)]
    daemon: bool,
    #[arg(long, default_value = "")]
    pid_file: String,
    #[arg(long, default_value = "")]
    log_file: String,
    #[arg(long)]
    chunk_db_dir: String,
    #[arg(long, default_value_t = 9876)]
    listen_port: u16,
    #[arg(long, default_value = "")]
    peer_addrs: String,
    #[arg(long, default_value = "")]
    peer_discovery: String,
    #[arg(long, default_value = "")]
    advertise_addr: String,
    #[arg(long, default_value = "")]
    node_id: String,
    #[arg(long, default_value = "")]
    chunk_server_sock: String,
    #[arg(long, default_value = "")]
    chunk_index_url: String,
    #[arg(long, default_value_t = DEFAULT_CHUNK_INDEX_TTL_SECS)]
    chunk_index_ttl: u64,
    #[arg(long, default_value_t = DEFAULT_TOKIO_WORKER_THREADS)]
    tokio_worker_threads: usize,
    #[arg(long, default_value = "")]
    otel_endpoint: String,
}

#[derive(Args)]
struct GcOptions {
    #[arg(long)]
    chunk_db_dir: String,
    #[arg(long, default_value = "")]
    chunk_server_sock: String,
    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

#[derive(Args)]
struct StatsOptions {
    #[arg(long)]
    chunk_db_dir: String,
}

#[derive(Subcommand)]
enum Commands {
    Mount(Box<FsOptions>),
    ServeChunk(Box<ServeChunkOptions>),
    GcChunk(GcOptions),
    StatsChunk(StatsOptions),
}

fn build_local_chunk_client(opts: &FsOptions) -> Option<Arc<LocalChunkClient>> {
    let socket_path = resolve_chunk_server_socket(&opts.chunk_db_dir, &opts.chunk_server_sock)?;
    let runtime = PeerRuntime::new_with_worker_threads(opts.tokio_worker_threads).ok()?;
    let client = LocalChunkClient::new(
        runtime,
        socket_path,
        Duration::from_millis(opts.chunk_server_timeout_ms),
    );
    client.start_health_checker();
    Some(Arc::new(client))
}

fn resolve_chunk_server_socket(chunk_db_dir: &str, chunk_server_sock: &str) -> Option<PathBuf> {
    if !chunk_server_sock.is_empty() {
        Some(PathBuf::from(chunk_server_sock))
    } else if !chunk_db_dir.is_empty() {
        Some(default_chunk_server_socket(chunk_db_dir))
    } else {
        None
    }
}

fn build_gc_local_chunk_client(
    chunk_db_dir: &str,
    chunk_server_sock: &str,
) -> Option<Arc<dyn ChunkIndexControl>> {
    let runtime = PeerRuntime::new().ok()?;
    let socket_path = resolve_chunk_server_socket(chunk_db_dir, chunk_server_sock)?;
    Some(Arc::new(LocalChunkClient::new(
        runtime,
        socket_path,
        Duration::from_millis(1000),
    )))
}

fn init_metrics(
    otel_endpoint: &str,
    service_name: &str,
    node_id: &str,
    extra_attrs: Vec<KeyValue>,
) -> anyhow::Result<Option<SdkMeterProvider>> {
    if otel_endpoint.is_empty() {
        return Ok(None);
    }

    let provider = opentelemetry_otlp::new_pipeline()
        .metrics(opentelemetry_sdk::runtime::TokioCurrentThread)
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(otel_endpoint),
        )
        .with_resource(build_metrics_resource(service_name, node_id, extra_attrs))
        .build()?;

    global::set_meter_provider(provider.clone());
    Ok(Some(provider))
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::{Key, Value};

    fn resource_value(resource: &Resource, key: &str) -> Option<Value> {
        resource.get(Key::new(key.to_string()))
    }

    #[test]
    fn build_metrics_resource_includes_mount_attrs_and_instance_id() {
        let resource = build_metrics_resource(
            "distill-fs-mount",
            "node-a",
            vec![
                KeyValue::new("distill_fs.mountpoint", "/mnt/distill"),
                KeyValue::new("distill_fs.src", "oss"),
                KeyValue::new("service.instance.id", "mount:oss:/mnt/distill"),
            ],
        );

        assert_eq!(
            resource_value(&resource, "service.name"),
            Some(Value::from("distill-fs-mount"))
        );
        assert_eq!(
            resource_value(&resource, "node.id"),
            Some(Value::from("node-a"))
        );
        assert_eq!(
            resource_value(&resource, "distill_fs.mountpoint"),
            Some(Value::from("/mnt/distill"))
        );
        assert_eq!(
            resource_value(&resource, "distill_fs.src"),
            Some(Value::from("oss"))
        );
        assert_eq!(
            resource_value(&resource, "service.instance.id"),
            Some(Value::from("mount:oss:/mnt/distill"))
        );
    }

    #[test]
    fn build_metrics_resource_excludes_mount_attrs_for_chunkserver() {
        let resource = build_metrics_resource(
            "distill-fs-chunkserver",
            "node-b",
            vec![KeyValue::new(
                "service.instance.id",
                "chunkserver:node-b@10.0.0.1:9876",
            )],
        );

        assert_eq!(
            resource_value(&resource, "service.instance.id"),
            Some(Value::from("chunkserver:node-b@10.0.0.1:9876"))
        );
        assert_eq!(resource_value(&resource, "distill_fs.mountpoint"), None);
        assert_eq!(resource_value(&resource, "distill_fs.src"), None);
    }

    #[test]
    fn resolve_chunk_server_socket_prefers_explicit_path() {
        let socket = resolve_chunk_server_socket("/tmp/chunkdb", "/tmp/custom.sock");

        assert_eq!(socket, Some(PathBuf::from("/tmp/custom.sock")));
    }

    #[test]
    fn resolve_chunk_server_socket_defaults_to_chunkdb_path() {
        let socket = resolve_chunk_server_socket("/tmp/chunkdb", "");

        assert_eq!(socket, Some(default_chunk_server_socket("/tmp/chunkdb")));
    }
}

fn build_metrics_resource(
    service_name: &str,
    node_id: &str,
    extra_attrs: Vec<KeyValue>,
) -> Resource {
    let mut attrs = vec![
        KeyValue::new("service.name", service_name.to_string()),
        KeyValue::new("node.id", node_id.to_string()),
    ];
    attrs.extend(extra_attrs);
    Resource::new(attrs)
}

fn init_logging(log_level: tracing::Level, log_file: &str) -> anyhow::Result<()> {
    if !log_file.is_empty() {
        let file_writer = RotatingFileWriter::new(log_file)?;
        let (non_blocking, guard) = tracing_appender::non_blocking(file_writer);
        Box::leak(Box::new(guard));

        tracing_subscriber::fmt()
            .with_max_level(log_level)
            .with_writer(non_blocking)
            .with_ansi(false)
            .init();
    } else {
        tracing_subscriber::fmt().with_max_level(log_level).init();
    }
    Ok(())
}

#[derive(Parser)]
#[command(name = "distill_fs")]
#[command(version)]
#[command(about = "dedup, on_demand, readonly fs", long_about = None)]
struct Cli {
    #[arg(short = 'd', long, default_value_t = false)]
    verbose: bool,
    #[command(subcommand)]
    commands: Commands,
}

fn setup_chunk_and_index_db(
    chunk_db_dir: &str,
    image_meta_dir: &str,
    index_ctl: Option<Arc<dyn ChunkIndexControl>>,
) -> anyhow::Result<Option<(Arc<ChunkDB>, Arc<IndexDB>)>> {
    let conf_chunk_db = !chunk_db_dir.is_empty();
    let conf_image_meta = !image_meta_dir.is_empty();
    if conf_image_meta != conf_chunk_db {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "Invalid chunk_db_dir & image_meta_dir config",
        )
        .into());
    }
    if conf_chunk_db {
        let chunk_db = Arc::new(ChunkDB::new_with_index_ctl(chunk_db_dir, index_ctl)?);
        let index_db = Arc::new(IndexDB::open(image_meta_dir)?);
        Ok(Some((chunk_db, index_db)))
    } else {
        Ok(None)
    }
}

fn prepare_oss(opts: &FsOptions) -> anyhow::Result<RawImage> {
    let backend = GeneralBackend::new(&opts.cfg)?;
    let oss_file = backend.get_reader(&opts.name)?;
    let size = oss_file.size();
    if size == 0 {
        return Err(io::Error::new(ErrorKind::InvalidInput, "Invalid oss file").into());
    }
    let cached_oss = Cache::new(oss_file, &opts.cache_file)?;
    let b = Arc::new(cached_oss);
    let local_chunk_client = build_local_chunk_client(opts);
    let dedup_db = setup_chunk_and_index_db(
        &opts.chunk_db_dir,
        &opts.image_meta_dir,
        local_chunk_client
            .as_ref()
            .map(|client| client.clone() as Arc<dyn ChunkIndexControl>),
    )?;
    RawImage::new(
        &opts.name,
        b as Arc<dyn BackendEx>,
        dedup_db,
        local_chunk_client,
    )
}

fn prepare_local(opts: &FsOptions) -> anyhow::Result<RawImage> {
    let local_file = Cache::from_raw_file(&opts.cache_file)?;
    let size = local_file.size();
    if size == 0 {
        return Err(io::Error::new(ErrorKind::InvalidInput, "invalid local file").into());
    }
    let b = Arc::new(local_file);
    let local_chunk_client = build_local_chunk_client(opts);
    let dedup_db = setup_chunk_and_index_db(
        &opts.chunk_db_dir,
        &opts.image_meta_dir,
        local_chunk_client
            .as_ref()
            .map(|client| client.clone() as Arc<dyn ChunkIndexControl>),
    )?;
    RawImage::new(
        &opts.name,
        b as Arc<dyn BackendEx>,
        dedup_db,
        local_chunk_client,
    )
}

pub fn run() -> anyhow::Result<()> {
    let cli: Cli = Cli::parse();
    let log_level = if cli.verbose {
        tracing::Level::DEBUG
    } else {
        tracing::Level::INFO
    };

    match &cli.commands {
        Commands::Mount(fs_opts) => {
            if fs_opts.daemon {
                Daemonize::new().pid_file(&fs_opts.pid_file).start()?;
            }
            init_logging(log_level, &fs_opts.log_file)?;
            let src = fs_opts.src.as_str();
            let t = Instant::now();
            let metrics_provider = init_metrics(
                &fs_opts.otel_endpoint,
                "distill-fs-daemon",
                "mount",
                vec![
                    KeyValue::new("distill_fs.mountpoint", fs_opts.mountpoint.clone()),
                    KeyValue::new("distill_fs.src", src),
                    KeyValue::new(
                        "service.instance.id",
                        format!("mount:{src}:{}", fs_opts.mountpoint),
                    ),
                ],
            )?;
            info!("init_metrics took {:?}", t.elapsed());

            let result = match fs_opts.src {
                Source::Local => {
                    let image = prepare_local(fs_opts)?;
                    mount_fs(image, &fs_opts.mountpoint, fs_opts.fuse_worker_num)
                }
                Source::Oss => {
                    let image = prepare_oss(fs_opts)?;
                    mount_fs(image, &fs_opts.mountpoint, fs_opts.fuse_worker_num)
                }
                Source::Nydus => {
                    let t = std::time::Instant::now();
                    let local_chunk_client = build_local_chunk_client(fs_opts);
                    info!("build_local_chunk_client took {:?}", t.elapsed());

                    let t = std::time::Instant::now();
                    let dedup_db = setup_chunk_and_index_db(
                        &fs_opts.chunk_db_dir,
                        &fs_opts.image_meta_dir,
                        local_chunk_client
                            .as_ref()
                            .map(|client| client.clone() as Arc<dyn ChunkIndexControl>),
                    )?;
                    info!("setup_chunk_and_index_db took {:?}", t.elapsed());

                    let t = std::time::Instant::now();
                    let fs = NydusImage::new(
                        &fs_opts.bootstrap,
                        &fs_opts.cfg,
                        &fs_opts.cache_dir,
                        dedup_db,
                        local_chunk_client,
                    )?;
                    info!("NydusImage::new took {:?}", t.elapsed());

                    mount_fs(fs, &fs_opts.mountpoint, fs_opts.fuse_worker_num)
                }
            };
            if let Some(provider) = metrics_provider {
                let _ = provider.shutdown();
            }
            result
        }
        Commands::ServeChunk(opts) => {
            if opts.daemon {
                Daemonize::new().pid_file(&opts.pid_file).start()?;
            }
            init_logging(log_level, &opts.log_file)?;
            let runtime = PeerRuntime::new_with_worker_threads(opts.tokio_worker_threads)?;
            let listen_addr: std::net::SocketAddr =
                format!("0.0.0.0:{}", opts.listen_port).parse()?;
            let advertise_addr: std::net::SocketAddr = if opts.advertise_addr.is_empty() {
                format!("127.0.0.1:{}", opts.listen_port).parse()?
            } else {
                opts.advertise_addr.parse()?
            };
            let node_id = if opts.node_id.is_empty() {
                default_node_id()
            } else {
                opts.node_id.clone()
            };
            let metrics_provider = init_metrics(
                &opts.otel_endpoint,
                "distill-fs-chunkserver",
                &node_id,
                vec![KeyValue::new(
                    "service.instance.id",
                    format!("chunkserver:{node_id}@{advertise_addr}"),
                )],
            )?;
            let socket_path = if !opts.chunk_server_sock.is_empty() {
                PathBuf::from(&opts.chunk_server_sock)
            } else {
                default_chunk_server_socket(&opts.chunk_db_dir)
            };
            let discovery = build_discovery(
                runtime.clone(),
                &opts.peer_discovery,
                &opts.peer_addrs,
                advertise_addr,
                &node_id,
            )?;
            let chunk_index: Option<Arc<dyn ChunkIndex>> = if opts.chunk_index_url.is_empty() {
                None
            } else {
                Some(Arc::new(RedisChunkIndex::new(
                    &opts.chunk_index_url,
                    advertise_addr,
                    &node_id,
                    opts.chunk_index_ttl,
                )?))
            };
            let index_ctl = chunk_index.as_ref().map(|chunk_index| {
                Arc::new(SyncChunkIndexControl::new(
                    runtime.clone(),
                    chunk_index.clone(),
                )) as Arc<dyn ChunkIndexControl>
            });
            let chunk_db = Arc::new(ChunkDB::new_with_index_ctl(&opts.chunk_db_dir, index_ctl)?);
            let peer_client =
                if discovery.get_peers().is_empty() && opts.peer_discovery.trim().is_empty() {
                    None
                } else {
                    let mut client =
                        PeerClient::new(runtime.clone(), discovery).with_local_addr(advertise_addr);
                    if let Some(chunk_index) = chunk_index {
                        client = client.with_chunk_index(chunk_index);
                    }
                    Some(Arc::new(client))
                };
            let server =
                ChunkServer::new(runtime, chunk_db, listen_addr, &socket_path, peer_client)?;
            let result = server.run();
            if let Some(provider) = metrics_provider {
                let _ = provider.shutdown();
            }
            result
        }
        Commands::GcChunk(gc_opts) => {
            tracing_subscriber::fmt().with_max_level(log_level).init();

            let worker = GcWorker::new_with_local_client(
                &gc_opts.chunk_db_dir,
                build_gc_local_chunk_client(&gc_opts.chunk_db_dir, &gc_opts.chunk_server_sock),
            )?;
            worker.run(gc_opts.dry_run)
        }
        Commands::StatsChunk(stats_opts) => {
            // Write logs to stderr so only JSON goes to stdout.
            tracing_subscriber::fmt()
                .with_max_level(log_level)
                .with_writer(std::io::stderr)
                .init();

            let chunk_db = ChunkDB::new(&stats_opts.chunk_db_dir)?;
            let stats = chunk_db.get_stats()?;
            println!("{}", serde_json::to_string_pretty(&stats)?);
            Ok(())
        }
    }
}
