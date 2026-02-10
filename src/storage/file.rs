use super::{
    CreateStreamResult, CreateWithDataResult, NOTIFY_CHANNEL_CAPACITY, ProducerAppendResult,
    ProducerCheck, ProducerState, ReadResult, Storage, StreamConfig, StreamMetadata,
};
use crate::protocol::error::{Error, Result};
use crate::protocol::offset::Offset;
use crate::protocol::producer::ProducerHeaders;
use base64::Engine;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;
use tracing::warn;

/// Binary record header size: little-endian `u32` payload length.
const RECORD_HEADER_BYTES: usize = 4;
const INITIAL_INDEX_CAPACITY: usize = 256;
const INITIAL_PRODUCERS_CAPACITY: usize = 8;

#[derive(Debug, Clone)]
struct MessageIndex {
    offset: Offset,
    file_pos: u64,
    byte_len: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct StreamMeta {
    name: String,
    config: StreamConfig,
    closed: bool,
    created_at: DateTime<Utc>,
    last_seq: Option<String>,
    producers: HashMap<String, ProducerState>,
}

struct StreamEntry {
    config: StreamConfig,
    index: Vec<MessageIndex>,
    closed: bool,
    next_read_seq: u64,
    next_byte_offset: u64,
    total_bytes: u64,
    created_at: DateTime<Utc>,
    producers: HashMap<String, ProducerState>,
    notify: broadcast::Sender<()>,
    last_seq: Option<String>,
    file: File,
    file_len: u64,
    dir: PathBuf,
}

impl StreamEntry {
    fn new(config: StreamConfig, file: File, dir: PathBuf) -> Self {
        let (notify, _) = broadcast::channel(NOTIFY_CHANNEL_CAPACITY);
        let file_len = file.metadata().map_or(0, |m| m.len());
        Self {
            config,
            index: Vec::with_capacity(INITIAL_INDEX_CAPACITY),
            closed: false,
            next_read_seq: 0,
            next_byte_offset: 0,
            total_bytes: 0,
            created_at: Utc::now(),
            producers: HashMap::with_capacity(INITIAL_PRODUCERS_CAPACITY),
            notify,
            last_seq: None,
            file,
            file_len,
            dir,
        }
    }
}

/// High-throughput file-backed storage.
///
/// Design:
/// - One append-only log file per stream (`data.log`)
/// - In-memory offset/file index for fast reads
/// - Stream-level write lock serializes appends and preserves monotonic offsets
/// - Batched write per append call reduces syscall overhead
#[allow(clippy::module_name_repetitions)]
pub struct FileStorage {
    streams: RwLock<HashMap<String, Arc<RwLock<StreamEntry>>>>,
    total_bytes: RwLock<u64>,
    max_total_bytes: u64,
    max_stream_bytes: u64,
    root_dir: PathBuf,
    root_dir_canonical: PathBuf,
    sync_on_append: bool,
}

impl FileStorage {
    /// # Errors
    ///
    /// Returns `Error::Storage` if the root directory cannot be created or
    /// existing streams fail to load from disk.
    pub fn new(
        root_dir: impl Into<PathBuf>,
        max_total_bytes: u64,
        max_stream_bytes: u64,
        sync_on_append: bool,
    ) -> Result<Self> {
        let root_dir = root_dir.into();
        fs::create_dir_all(&root_dir).map_err(|e| {
            Error::Storage(format!(
                "failed to create storage directory {}: {e}",
                root_dir.display()
            ))
        })?;

        let root_dir_canonical = fs::canonicalize(&root_dir).map_err(|e| {
            Error::Storage(format!(
                "failed to canonicalize storage directory {}: {e}",
                root_dir.display()
            ))
        })?;

        let storage = Self {
            streams: RwLock::new(HashMap::new()),
            total_bytes: RwLock::new(0),
            max_total_bytes,
            max_stream_bytes,
            root_dir,
            root_dir_canonical,
            sync_on_append,
        };
        storage.load_existing_streams()?;
        Ok(storage)
    }

    /// # Panics
    ///
    /// Panics if the internal `total_bytes` lock is poisoned.
    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        *self.total_bytes.read().expect("total_bytes lock poisoned")
    }

    /// Map a stream name to a directory path inside `root_dir`.
    ///
    /// Uses base64url encoding (alphabet `[A-Za-z0-9_-]`) so the output
    /// cannot contain path separators, but we verify containment anyway
    /// as defense in depth.
    fn stream_dir_for_name(&self, name: &str) -> Result<PathBuf> {
        let encoded = base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(name.as_bytes());

        // Reject path traversal sequences and separators. Base64url encoding
        // (alphabet [A-Za-z0-9_-]) cannot produce these, but the explicit
        // checks act as defense in depth and satisfy static analysis (CodeQL
        // rust/path-injection).
        if encoded.contains("..") || encoded.contains('/') || encoded.contains('\\') {
            return Err(Error::Storage(
                "encoded stream name contains path traversal characters".to_string(),
            ));
        }
        if !encoded
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err(Error::Storage(
                "encoded stream directory contains invalid characters".to_string(),
            ));
        }

        let dir = self.root_dir.join(&encoded);
        if !dir.starts_with(&self.root_dir) {
            return Err(Error::Storage(format!(
                "stream directory escapes storage root: {encoded}"
            )));
        }
        Ok(dir)
    }

    fn validate_stream_dir(&self, dir: &Path) -> Result<()> {
        if !dir.starts_with(&self.root_dir) {
            return Err(Error::Storage(format!(
                "path escapes storage root: {}",
                dir.display()
            )));
        }

        let rel = dir.strip_prefix(&self.root_dir).map_err(|e| {
            Error::Storage(format!(
                "failed to validate storage path {}: {e}",
                dir.display()
            ))
        })?;
        if rel.components().count() != 1 {
            return Err(Error::Storage(format!(
                "invalid stream path depth: {}",
                dir.display()
            )));
        }

        if dir.exists() {
            let metadata = fs::symlink_metadata(dir).map_err(|e| {
                Error::Storage(format!(
                    "failed to stat stream directory {}: {e}",
                    dir.display()
                ))
            })?;
            if metadata.file_type().is_symlink() {
                return Err(Error::Storage(format!(
                    "stream directory cannot be a symlink: {}",
                    dir.display()
                )));
            }
            if !metadata.is_dir() {
                return Err(Error::Storage(format!(
                    "stream path is not a directory: {}",
                    dir.display()
                )));
            }

            let canonical = fs::canonicalize(dir).map_err(|e| {
                Error::Storage(format!(
                    "failed to canonicalize stream directory {}: {e}",
                    dir.display()
                ))
            })?;
            if !canonical.starts_with(&self.root_dir_canonical) {
                return Err(Error::Storage(format!(
                    "stream directory resolves outside storage root: {}",
                    dir.display()
                )));
            }
        }

        Ok(())
    }

    fn data_log_path(dir: &Path) -> PathBuf {
        dir.join("data.log")
    }

    fn meta_path(dir: &Path) -> PathBuf {
        dir.join("meta.json")
    }

    fn write_metadata_for(&self, name: &str, entry: &StreamEntry) -> Result<()> {
        self.validate_stream_dir(&entry.dir)?;
        let meta = StreamMeta {
            name: name.to_string(),
            config: entry.config.clone(),
            closed: entry.closed,
            created_at: entry.created_at,
            last_seq: entry.last_seq.clone(),
            producers: entry.producers.clone(),
        };

        let meta_path = Self::meta_path(&entry.dir);
        let tmp_path = entry.dir.join("meta.json.tmp");
        let payload = serde_json::to_vec(&meta)
            .map_err(|e| Error::Storage(format!("failed to serialize stream metadata: {e}")))?;

        fs::write(&tmp_path, payload).map_err(|e| {
            Error::Storage(format!(
                "failed to write metadata temp file {}: {e}",
                tmp_path.display()
            ))
        })?;

        fs::rename(&tmp_path, &meta_path).map_err(|e| {
            Error::Storage(format!(
                "failed to atomically replace metadata {}: {e}",
                meta_path.display()
            ))
        })?;

        Ok(())
    }

    fn open_stream_file(&self, dir: &Path) -> Result<File> {
        self.validate_stream_dir(dir)?;
        let path = Self::data_log_path(dir);
        OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .map_err(|e| {
                Error::Storage(format!("failed to open stream log {}: {e}", path.display()))
            })
    }

    fn rebuild_index(file: &mut File) -> Result<(Vec<MessageIndex>, u64, u64)> {
        let mut index = Vec::new();
        let mut next_read_seq = 0u64;
        let mut next_byte_offset = 0u64;

        let file_len = file
            .metadata()
            .map_err(|e| Error::Storage(format!("failed to stat stream log: {e}")))?
            .len();

        let mut cursor = 0u64;
        let mut header = [0u8; RECORD_HEADER_BYTES];

        while cursor < file_len {
            file.seek(SeekFrom::Start(cursor))
                .map_err(|e| Error::Storage(format!("failed to seek stream log: {e}")))?;

            let read = file
                .read(&mut header)
                .map_err(|e| Error::Storage(format!("failed to read stream log header: {e}")))?;

            if read == 0 {
                break;
            }

            if read < RECORD_HEADER_BYTES {
                file.set_len(cursor).map_err(|e| {
                    Error::Storage(format!("failed to truncate partial record: {e}"))
                })?;
                break;
            }

            let record_len = u64::from(u32::from_le_bytes(header));
            let record_end = cursor + RECORD_HEADER_BYTES as u64 + record_len;

            if record_end > file_len {
                file.set_len(cursor).map_err(|e| {
                    Error::Storage(format!("failed to truncate partial record: {e}"))
                })?;
                break;
            }

            index.push(MessageIndex {
                offset: Offset::new(next_read_seq, next_byte_offset),
                file_pos: cursor + RECORD_HEADER_BYTES as u64,
                byte_len: record_len,
            });

            next_read_seq += 1;
            next_byte_offset += record_len;
            cursor = record_end;
        }

        file.seek(SeekFrom::End(0))
            .map_err(|e| Error::Storage(format!("failed to seek end of stream log: {e}")))?;

        Ok((index, next_read_seq, next_byte_offset))
    }

    fn rollback_total_bytes(&self, bytes: u64) {
        let mut total = self.total_bytes.write().expect("total_bytes lock poisoned");
        *total = total.saturating_sub(bytes);
    }

    fn get_stream(&self, name: &str) -> Option<Arc<RwLock<StreamEntry>>> {
        let streams = self.streams.read().expect("streams lock poisoned");
        streams.get(name).map(Arc::clone)
    }

    fn append_records(
        &self,
        name: &str,
        stream: &mut StreamEntry,
        messages: &[Bytes],
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let mut total_batch_bytes = 0u64;
        let mut payload_bytes = 0u64;
        let mut sizes = Vec::with_capacity(messages.len());

        for msg in messages {
            let len = u64::try_from(msg.len()).unwrap_or(u64::MAX);
            if len > u64::from(u32::MAX) {
                return Err(Error::InvalidHeader {
                    header: "Content-Length".to_string(),
                    reason: "message too large for file record format".to_string(),
                });
            }
            payload_bytes += len;
            total_batch_bytes += len;
            sizes.push(len);
        }

        // Check-and-reserve global limit atomically under a single write lock
        // to prevent concurrent appends on different streams from exceeding it.
        // Global check comes first to preserve error precedence.
        {
            let mut total = self.total_bytes.write().expect("total_bytes lock poisoned");
            if *total + total_batch_bytes > self.max_total_bytes {
                return Err(Error::MemoryLimitExceeded);
            }
            if stream.total_bytes + total_batch_bytes > self.max_stream_bytes {
                return Err(Error::StreamSizeLimitExceeded);
            }
            *total += total_batch_bytes;
        }

        let wire_overhead = RECORD_HEADER_BYTES.saturating_mul(messages.len());
        let mut write_buf =
            Vec::with_capacity(usize::try_from(payload_bytes).unwrap_or(0) + wire_overhead);
        for msg in messages {
            let len = u32::try_from(msg.len()).unwrap_or(u32::MAX);
            write_buf.extend_from_slice(&len.to_le_bytes());
            write_buf.extend_from_slice(msg);
        }

        let before_len = stream.file_len;

        if let Err(e) = stream.file.write_all(&write_buf) {
            // Write errors may still leave partial bytes on disk; refresh cached length.
            if let Ok(m) = stream.file.metadata() {
                stream.file_len = m.len();
            }
            self.rollback_total_bytes(total_batch_bytes);
            return Err(Error::Storage(format!(
                "failed to append stream log for {name}: {e}"
            )));
        }

        if self.sync_on_append
            && let Err(e) = stream.file.sync_data()
        {
            // Data may be written even if fsync fails; refresh cached length.
            if let Ok(m) = stream.file.metadata() {
                stream.file_len = m.len();
            }
            self.rollback_total_bytes(total_batch_bytes);
            return Err(Error::Storage(format!(
                "failed to sync stream log for {name}: {e}"
            )));
        }

        let mut cursor = before_len;
        for len in sizes {
            let offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);
            stream.index.push(MessageIndex {
                offset,
                file_pos: cursor + RECORD_HEADER_BYTES as u64,
                byte_len: len,
            });
            stream.next_read_seq += 1;
            stream.next_byte_offset += len;
            stream.total_bytes += len;
            cursor += RECORD_HEADER_BYTES as u64 + len;
        }
        stream.file_len = cursor;

        let _ = stream.notify.send(());
        Ok(())
    }

    fn read_messages(file: &File, index_slice: &[MessageIndex]) -> Result<Vec<Bytes>> {
        if index_slice.is_empty() {
            return Ok(Vec::new());
        }

        let mut reader = file
            .try_clone()
            .map_err(|e| Error::Storage(format!("failed to clone stream file handle: {e}")))?;

        let first_pos = index_slice[0].file_pos;
        let last = index_slice
            .last()
            .expect("index_slice non-empty due early return");
        let read_end = last.file_pos + last.byte_len;
        let read_len = read_end.saturating_sub(first_pos);

        reader
            .seek(SeekFrom::Start(first_pos))
            .map_err(|e| Error::Storage(format!("failed to seek message data: {e}")))?;

        let mut raw = vec![0u8; usize::try_from(read_len).unwrap_or(usize::MAX)];
        reader
            .read_exact(&mut raw)
            .map_err(|e| Error::Storage(format!("failed to read message data: {e}")))?;

        let shared = Bytes::from(raw);
        let mut messages = Vec::with_capacity(index_slice.len());
        for idx in index_slice {
            let rel_start =
                usize::try_from(idx.file_pos.saturating_sub(first_pos)).unwrap_or(usize::MAX);
            let rel_end = rel_start + usize::try_from(idx.byte_len).unwrap_or(usize::MAX);
            messages.push(shared.slice(rel_start..rel_end));
        }

        Ok(messages)
    }

    fn remove_stream_dir(&self, dir: &Path) -> Result<()> {
        self.validate_stream_dir(dir)?;
        fs::remove_dir_all(dir).map_err(|e| {
            Error::Storage(format!(
                "failed to remove stream directory {}: {e}",
                dir.display()
            ))
        })
    }

    fn load_existing_streams(&self) -> Result<()> {
        let entries = fs::read_dir(&self.root_dir).map_err(|e| {
            Error::Storage(format!(
                "failed to read storage directory {}: {e}",
                self.root_dir.display()
            ))
        })?;

        let mut streams_map = self.streams.write().expect("streams lock poisoned");
        let mut restored_total = 0u64;

        for dir_entry in entries {
            let dir_entry = dir_entry
                .map_err(|e| Error::Storage(format!("failed to inspect storage entry: {e}")))?;
            let path = dir_entry.path();
            if !path.is_dir() {
                continue;
            }
            if self.validate_stream_dir(&path).is_err() {
                continue;
            }

            let meta_path = Self::meta_path(&path);
            if !meta_path.exists() {
                continue;
            }

            let meta_payload = fs::read(&meta_path).map_err(|e| {
                Error::Storage(format!(
                    "failed to read stream metadata {}: {e}",
                    meta_path.display()
                ))
            })?;
            let meta: StreamMeta = serde_json::from_slice(&meta_payload).map_err(|e| {
                Error::Storage(format!(
                    "failed to parse stream metadata {}: {e}",
                    meta_path.display()
                ))
            })?;

            let mut file = self.open_stream_file(&path)?;
            let (index, next_read_seq, next_byte_offset) = Self::rebuild_index(&mut file)?;
            let total_bytes = next_byte_offset;
            let file_len = file
                .metadata()
                .map_err(|e| Error::Storage(format!("failed to stat stream log: {e}")))?
                .len();

            let (notify, _) = broadcast::channel(NOTIFY_CHANNEL_CAPACITY);
            let entry = StreamEntry {
                config: meta.config,
                index,
                closed: meta.closed,
                next_read_seq,
                next_byte_offset,
                total_bytes,
                created_at: meta.created_at,
                producers: meta.producers,
                notify,
                last_seq: meta.last_seq,
                file,
                file_len,
                dir: path,
            };

            if super::is_stream_expired(&entry.config) {
                self.remove_stream_dir(&entry.dir)?;
                continue;
            }

            restored_total = restored_total.saturating_add(entry.total_bytes);
            streams_map.insert(meta.name, Arc::new(RwLock::new(entry)));
        }

        let mut total = self.total_bytes.write().expect("total_bytes lock poisoned");
        *total = restored_total;

        Ok(())
    }
}

impl Storage for FileStorage {
    fn create_stream(&self, name: &str, config: StreamConfig) -> Result<CreateStreamResult> {
        let mut streams = self.streams.write().expect("streams lock poisoned");

        if let Some(stream_arc) = streams.get(name) {
            let stream = stream_arc.read().expect("stream lock poisoned");

            if super::is_stream_expired(&stream.config) {
                let stream_bytes = stream.total_bytes;
                let dir = stream.dir.clone();
                drop(stream);
                streams.remove(name);

                let mut total = self.total_bytes.write().expect("total_bytes lock poisoned");
                *total = total.saturating_sub(stream_bytes);
                drop(total);

                self.remove_stream_dir(&dir)?;
            } else if stream.config == config {
                return Ok(CreateStreamResult::AlreadyExists);
            } else {
                return Err(Error::ConfigMismatch);
            }
        }

        let dir = self.stream_dir_for_name(name)?;
        fs::create_dir_all(&dir).map_err(|e| {
            Error::Storage(format!(
                "failed to create stream directory {}: {e}",
                dir.display()
            ))
        })?;

        self.validate_stream_dir(&dir)?;
        let file = self.open_stream_file(&dir)?;
        let entry = StreamEntry::new(config, file, dir);

        self.write_metadata_for(name, &entry)?;
        streams.insert(name.to_string(), Arc::new(RwLock::new(entry)));

        Ok(CreateStreamResult::Created)
    }

    fn append(&self, name: &str, data: Bytes, content_type: &str) -> Result<Offset> {
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let mut stream = stream_arc.write().expect("stream lock poisoned");

        if super::is_stream_expired(&stream.config) {
            return Err(Error::StreamExpired);
        }

        if stream.closed {
            return Err(Error::StreamClosed);
        }

        super::validate_content_type(&stream.config.content_type, content_type)?;

        let offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);
        self.append_records(name, &mut stream, &[data])?;
        // Plain appends do not mutate persisted metadata fields (closed/seq/producers).
        Ok(offset)
    }

    fn batch_append(
        &self,
        name: &str,
        messages: Vec<Bytes>,
        content_type: &str,
        seq: Option<&str>,
    ) -> Result<Offset> {
        if messages.is_empty() {
            return Err(Error::InvalidHeader {
                header: "Content-Length".to_string(),
                reason: "batch cannot be empty".to_string(),
            });
        }

        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let mut stream = stream_arc.write().expect("stream lock poisoned");

        if super::is_stream_expired(&stream.config) {
            return Err(Error::StreamExpired);
        }

        if stream.closed {
            return Err(Error::StreamClosed);
        }

        super::validate_content_type(&stream.config.content_type, content_type)?;

        let pending_seq = super::validate_seq(stream.last_seq.as_deref(), seq)?;
        self.append_records(name, &mut stream, &messages)?;
        if let Some(new_seq) = pending_seq {
            stream.last_seq = Some(new_seq);
            // Stream-Seq changed, persist metadata best-effort.
            if let Err(e) = self.write_metadata_for(name, &stream) {
                warn!(%e, stream = name, "metadata persist failed after batch append");
            }
        }

        Ok(Offset::new(stream.next_read_seq, stream.next_byte_offset))
    }

    fn read(&self, name: &str, from_offset: &Offset) -> Result<ReadResult> {
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let stream = stream_arc.read().expect("stream lock poisoned");

        if super::is_stream_expired(&stream.config) {
            return Err(Error::StreamExpired);
        }

        if from_offset.is_now() {
            return Ok(ReadResult {
                messages: Vec::new(),
                next_offset: Offset::new(stream.next_read_seq, stream.next_byte_offset),
                at_tail: true,
                closed: stream.closed,
            });
        }

        let start_idx = if from_offset.is_start() {
            0
        } else {
            match stream.index.binary_search_by(|m| m.offset.cmp(from_offset)) {
                Ok(idx) | Err(idx) => idx,
            }
        };

        let index_slice = &stream.index[start_idx..];
        let messages = Self::read_messages(&stream.file, index_slice)?;
        let at_tail = start_idx + messages.len() >= stream.index.len();

        Ok(ReadResult {
            messages,
            next_offset: Offset::new(stream.next_read_seq, stream.next_byte_offset),
            at_tail,
            closed: stream.closed,
        })
    }

    fn delete(&self, name: &str) -> Result<()> {
        let mut streams = self.streams.write().expect("streams lock poisoned");

        if let Some(stream_arc) = streams.remove(name) {
            let stream = stream_arc.read().expect("stream lock poisoned");
            let dir = stream.dir.clone();
            let stream_bytes = stream.total_bytes;
            drop(stream);

            let mut total = self.total_bytes.write().expect("total_bytes lock poisoned");
            *total = total.saturating_sub(stream_bytes);
            drop(total);

            self.remove_stream_dir(&dir)?;
            Ok(())
        } else {
            Err(Error::NotFound(name.to_string()))
        }
    }

    fn head(&self, name: &str) -> Result<StreamMetadata> {
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let stream = stream_arc.read().expect("stream lock poisoned");

        if super::is_stream_expired(&stream.config) {
            return Err(Error::StreamExpired);
        }

        Ok(StreamMetadata {
            config: stream.config.clone(),
            next_offset: Offset::new(stream.next_read_seq, stream.next_byte_offset),
            closed: stream.closed,
            total_bytes: stream.total_bytes,
            message_count: u64::try_from(stream.index.len()).unwrap_or(u64::MAX),
            created_at: stream.created_at,
        })
    }

    fn close_stream(&self, name: &str) -> Result<()> {
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let mut stream = stream_arc.write().expect("stream lock poisoned");

        if super::is_stream_expired(&stream.config) {
            return Err(Error::StreamExpired);
        }

        stream.closed = true;
        self.write_metadata_for(name, &stream)?;

        let _ = stream.notify.send(());
        Ok(())
    }

    fn append_with_producer(
        &self,
        name: &str,
        messages: Vec<Bytes>,
        content_type: &str,
        producer: &ProducerHeaders,
        should_close: bool,
        seq: Option<&str>,
    ) -> Result<ProducerAppendResult> {
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let mut stream = stream_arc.write().expect("stream lock poisoned");

        if super::is_stream_expired(&stream.config) {
            return Err(Error::StreamExpired);
        }

        super::cleanup_stale_producers(&mut stream.producers);

        if !messages.is_empty() {
            super::validate_content_type(&stream.config.content_type, content_type)?;
        }

        let now = Utc::now();

        match super::check_producer(
            stream.producers.get(producer.id.as_str()),
            producer,
            stream.closed,
        )? {
            ProducerCheck::Accept => {}
            ProducerCheck::Duplicate { epoch, seq } => {
                return Ok(ProducerAppendResult::Duplicate {
                    epoch,
                    seq,
                    next_offset: Offset::new(stream.next_read_seq, stream.next_byte_offset),
                    closed: stream.closed,
                });
            }
        }

        let pending_seq = super::validate_seq(stream.last_seq.as_deref(), seq)?;
        self.append_records(name, &mut stream, &messages)?;

        if let Some(new_seq) = pending_seq {
            stream.last_seq = Some(new_seq);
        }
        if should_close {
            stream.closed = true;
        }

        stream.producers.insert(
            producer.id.clone(),
            ProducerState {
                epoch: producer.epoch,
                last_seq: producer.seq,
                updated_at: now,
            },
        );

        // Data is committed to the log; metadata write failure is non-fatal
        if let Err(e) = self.write_metadata_for(name, &stream) {
            warn!(%e, stream = name, "metadata persist failed after committed producer append");
        }

        Ok(ProducerAppendResult::Accepted {
            epoch: producer.epoch,
            seq: producer.seq,
            next_offset: Offset::new(stream.next_read_seq, stream.next_byte_offset),
            closed: stream.closed,
        })
    }

    fn create_stream_with_data(
        &self,
        name: &str,
        config: StreamConfig,
        messages: Vec<Bytes>,
        should_close: bool,
    ) -> Result<CreateWithDataResult> {
        let mut streams = self.streams.write().expect("streams lock poisoned");

        if let Some(stream_arc) = streams.get(name) {
            let stream = stream_arc.read().expect("stream lock poisoned");

            if super::is_stream_expired(&stream.config) {
                let stream_bytes = stream.total_bytes;
                let dir = stream.dir.clone();
                drop(stream);
                streams.remove(name);

                let mut total = self.total_bytes.write().expect("total_bytes lock poisoned");
                *total = total.saturating_sub(stream_bytes);
                drop(total);

                self.remove_stream_dir(&dir)?;
            } else if stream.config == config {
                return Ok(CreateWithDataResult {
                    status: CreateStreamResult::AlreadyExists,
                    next_offset: Offset::new(stream.next_read_seq, stream.next_byte_offset),
                    closed: stream.closed,
                });
            } else {
                return Err(Error::ConfigMismatch);
            }
        }

        let dir = self.stream_dir_for_name(name)?;
        fs::create_dir_all(&dir).map_err(|e| {
            Error::Storage(format!(
                "failed to create stream directory {}: {e}",
                dir.display()
            ))
        })?;

        self.validate_stream_dir(&dir)?;
        let file = self.open_stream_file(&dir)?;
        let mut entry = StreamEntry::new(config, file, dir);

        if !messages.is_empty() {
            self.append_records(name, &mut entry, &messages)?;
        }
        if should_close {
            entry.closed = true;
        }

        let next_offset = Offset::new(entry.next_read_seq, entry.next_byte_offset);
        let closed = entry.closed;

        self.write_metadata_for(name, &entry)?;
        streams.insert(name.to_string(), Arc::new(RwLock::new(entry)));

        Ok(CreateWithDataResult {
            status: CreateStreamResult::Created,
            next_offset,
            closed,
        })
    }

    fn exists(&self, name: &str) -> bool {
        let streams = self.streams.read().expect("streams lock poisoned");
        if let Some(stream_arc) = streams.get(name) {
            let stream = stream_arc.read().expect("stream lock poisoned");
            !super::is_stream_expired(&stream.config)
        } else {
            false
        }
    }

    fn subscribe(&self, name: &str) -> Option<broadcast::Receiver<()>> {
        let stream_arc = self.get_stream(name)?;
        let stream = stream_arc.read().expect("stream lock poisoned");

        if super::is_stream_expired(&stream.config) {
            return None;
        }

        Some(stream.notify.subscribe())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    fn test_storage_dir() -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let stamp = Utc::now().timestamp_nanos_opt().unwrap_or_default();
        let seq = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let pid = std::process::id();
        std::env::temp_dir().join(format!("ds-file-storage-test-{stamp}-{pid}-{seq}"))
    }

    fn test_storage() -> FileStorage {
        FileStorage::new(test_storage_dir(), 1024 * 1024, 100 * 1024, false)
            .expect("file storage should initialize")
    }

    #[test]
    fn test_delete_removes_files() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();
        storage
            .append("test", Bytes::from("data"), "text/plain")
            .unwrap();

        let dir = storage.stream_dir_for_name("test").unwrap();
        assert!(dir.exists(), "stream directory should exist before delete");

        storage.delete("test").unwrap();
        assert!(
            !dir.exists(),
            "stream directory should be removed after delete"
        );
    }

    #[test]
    fn test_restore_from_disk() {
        let root = test_storage_dir();
        let config = StreamConfig::new("text/plain".to_string());

        {
            let storage = FileStorage::new(root.clone(), 1024 * 1024, 100 * 1024, false)
                .expect("file storage should initialize");
            storage
                .create_stream("events", config.clone())
                .expect("stream should be created");
            storage
                .append("events", Bytes::from("event-1"), "text/plain")
                .expect("append should succeed");
            storage
                .append("events", Bytes::from("event-2"), "text/plain")
                .expect("append should succeed");
        }

        let restored =
            FileStorage::new(root, 1024 * 1024, 100 * 1024, false).expect("restore should work");

        let read = restored
            .read("events", &Offset::start())
            .expect("read should succeed");

        assert_eq!(read.messages.len(), 2);
        assert_eq!(read.messages[0], Bytes::from("event-1"));
        assert_eq!(read.messages[1], Bytes::from("event-2"));
    }

    #[test]
    fn test_restore_closed_stream_from_disk() {
        let root = test_storage_dir();
        let config = StreamConfig::new("text/plain".to_string());

        {
            let storage = FileStorage::new(root.clone(), 1024 * 1024, 100 * 1024, false).unwrap();
            storage.create_stream("s", config.clone()).unwrap();
            storage
                .append("s", Bytes::from("data"), "text/plain")
                .unwrap();
            storage.close_stream("s").unwrap();
        }

        let restored = FileStorage::new(root, 1024 * 1024, 100 * 1024, false).unwrap();
        let meta = restored.head("s").unwrap();
        assert!(meta.closed);
        assert_eq!(meta.message_count, 1);

        assert!(matches!(
            restored.append("s", Bytes::from("more"), "text/plain"),
            Err(Error::StreamClosed)
        ));
    }

    #[test]
    fn test_partial_record_truncation_on_recovery() {
        let root = test_storage_dir();
        let config = StreamConfig::new("text/plain".to_string());

        {
            let storage = FileStorage::new(root.clone(), 1024 * 1024, 100 * 1024, false).unwrap();
            storage.create_stream("s", config.clone()).unwrap();
            storage
                .append("s", Bytes::from("good"), "text/plain")
                .unwrap();
        }

        // Append partial garbage to the data log (incomplete record header)
        let encoded = base64::prelude::BASE64_URL_SAFE_NO_PAD.encode("s".as_bytes());
        let log_path = root.join(&encoded).join("data.log");
        let mut f = OpenOptions::new().append(true).open(&log_path).unwrap();
        // Write a 2-byte partial header (less than the 4-byte record header)
        f.write_all(&[0xFF, 0xFF]).unwrap();
        drop(f);

        let restored = FileStorage::new(root, 1024 * 1024, 100 * 1024, false).unwrap();
        let read = restored.read("s", &Offset::start()).unwrap();
        assert_eq!(read.messages.len(), 1);
        assert_eq!(read.messages[0], Bytes::from("good"));
    }
}
