use super::{
    CreateStreamResult, CreateWithDataResult, Message, NOTIFY_CHANNEL_CAPACITY,
    ProducerAppendResult, ProducerCheck, ProducerState, ReadResult, Storage, StreamConfig,
    StreamMetadata,
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
    dir: PathBuf,
}

impl StreamEntry {
    fn new(config: StreamConfig, file: File, dir: PathBuf) -> Self {
        let (notify, _) = broadcast::channel(NOTIFY_CHANNEL_CAPACITY);
        Self {
            config,
            index: Vec::new(),
            closed: false,
            next_read_seq: 0,
            next_byte_offset: 0,
            total_bytes: 0,
            created_at: Utc::now(),
            producers: HashMap::new(),
            notify,
            last_seq: None,
            file,
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

        let before_len = match stream.file.metadata() {
            Ok(m) => m.len(),
            Err(e) => {
                self.rollback_total_bytes(total_batch_bytes);
                return Err(Error::Storage(format!(
                    "failed to stat stream log before append: {e}"
                )));
            }
        };

        if let Err(e) = stream.file.write_all(&write_buf) {
            self.rollback_total_bytes(total_batch_bytes);
            return Err(Error::Storage(format!(
                "failed to append stream log for {name}: {e}"
            )));
        }

        if self.sync_on_append
            && let Err(e) = stream.file.sync_data()
        {
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

        let _ = stream.notify.send(());
        Ok(())
    }

    fn read_messages(file: &File, index_slice: &[MessageIndex]) -> Result<Vec<Message>> {
        let mut messages = Vec::with_capacity(index_slice.len());
        let mut reader = file
            .try_clone()
            .map_err(|e| Error::Storage(format!("failed to clone stream file handle: {e}")))?;

        for idx in index_slice {
            reader
                .seek(SeekFrom::Start(idx.file_pos))
                .map_err(|e| Error::Storage(format!("failed to seek message data: {e}")))?;

            let mut buf = vec![0u8; usize::try_from(idx.byte_len).unwrap_or(usize::MAX)];
            reader
                .read_exact(&mut buf)
                .map_err(|e| Error::Storage(format!("failed to read message data: {e}")))?;

            messages.push(Message::new(idx.offset.clone(), Bytes::from(buf)));
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
        // Data is committed to the log; metadata write failure is non-fatal
        if let Err(e) = self.write_metadata_for(name, &stream) {
            warn!(%e, stream = name, "metadata persist failed after committed append");
        }
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
        }
        // Data is committed to the log; metadata write failure is non-fatal
        if let Err(e) = self.write_metadata_for(name, &stream) {
            warn!(%e, stream = name, "metadata persist failed after committed batch append");
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

    fn test_storage_dir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let stamp = Utc::now().timestamp_nanos_opt().unwrap_or_default();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        std::env::temp_dir().join(format!("ds-file-storage-test-{stamp}-{pid}-{seq}"))
    }

    fn test_storage() -> FileStorage {
        FileStorage::new(test_storage_dir(), 1024 * 1024, 100 * 1024, false)
            .expect("file storage should initialize")
    }

    fn producer(id: &str, epoch: u64, seq: u64) -> ProducerHeaders {
        ProducerHeaders {
            id: id.to_string(),
            epoch,
            seq,
        }
    }

    #[test]
    fn test_create_stream() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());

        let result = storage.create_stream("test", config.clone()).unwrap();
        assert_eq!(result, CreateStreamResult::Created);
        assert!(storage.exists("test"));

        let result = storage.create_stream("test", config).unwrap();
        assert_eq!(result, CreateStreamResult::AlreadyExists);

        let different_config = StreamConfig::new("application/json".to_string());
        assert!(matches!(
            storage.create_stream("test", different_config),
            Err(Error::ConfigMismatch)
        ));
    }

    #[test]
    fn test_append_and_read() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();

        let data1 = Bytes::from("hello");
        let offset1 = storage.append("test", data1.clone(), "text/plain").unwrap();

        let data2 = Bytes::from("world");
        let offset2 = storage.append("test", data2.clone(), "text/plain").unwrap();

        assert!(offset1 < offset2);

        let result = storage.read("test", &Offset::start()).unwrap();
        assert_eq!(result.messages.len(), 2);
        assert_eq!(result.messages[0].data, data1);
        assert_eq!(result.messages[1].data, data2);
        assert!(result.at_tail);
    }

    #[test]
    fn test_offset_monotonicity() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();

        let mut offsets = Vec::new();
        for i in 0..10 {
            let data = Bytes::from(format!("message {i}"));
            let offset = storage.append("test", data, "text/plain").unwrap();
            offsets.push(offset);
        }

        for i in 1..offsets.len() {
            assert!(offsets[i - 1] < offsets[i], "Offset monotonicity violated");
        }
    }

    #[test]
    fn test_read_from_offset() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();

        let offset1 = storage
            .append("test", Bytes::from("msg1"), "text/plain")
            .unwrap();
        let offset2 = storage
            .append("test", Bytes::from("msg2"), "text/plain")
            .unwrap();
        let _offset3 = storage
            .append("test", Bytes::from("msg3"), "text/plain")
            .unwrap();

        let result = storage.read("test", &offset2).unwrap();
        assert_eq!(result.messages.len(), 2);
        assert_eq!(result.messages[0].data, Bytes::from("msg2"));
        assert_eq!(result.messages[1].data, Bytes::from("msg3"));

        let result = storage.read("test", &offset1).unwrap();
        assert_eq!(result.messages.len(), 3);
    }

    #[test]
    fn test_read_sentinels() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();

        storage
            .append("test", Bytes::from("msg1"), "text/plain")
            .unwrap();

        let result = storage.read("test", &Offset::now()).unwrap();
        assert_eq!(result.messages.len(), 0);
        assert!(result.at_tail);

        let result = storage.read("test", &Offset::start()).unwrap();
        assert_eq!(result.messages.len(), 1);
    }

    #[test]
    fn test_stream_closed() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();

        storage.close_stream("test").unwrap();

        assert!(matches!(
            storage.append("test", Bytes::from("data"), "text/plain"),
            Err(Error::StreamClosed)
        ));

        let result = storage.read("test", &Offset::start()).unwrap();
        assert!(result.closed);
    }

    #[test]
    fn test_content_type_mismatch() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();

        assert!(matches!(
            storage.append("test", Bytes::from("data"), "application/json"),
            Err(Error::ContentTypeMismatch { .. })
        ));

        // Case-insensitive match should work
        storage
            .append("test", Bytes::from("data"), "TEXT/PLAIN")
            .unwrap();
    }

    #[test]
    fn test_memory_limits() {
        let root = test_storage_dir();
        let storage = FileStorage::new(root, 100, 50, false).unwrap();
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test1", config.clone()).unwrap();
        storage.create_stream("test2", config).unwrap();

        storage
            .append("test1", Bytes::from(vec![0u8; 50]), "text/plain")
            .unwrap();

        assert!(matches!(
            storage.append("test1", Bytes::from(vec![0u8; 10]), "text/plain"),
            Err(Error::StreamSizeLimitExceeded)
        ));

        storage
            .append("test2", Bytes::from(vec![0u8; 40]), "text/plain")
            .unwrap();

        assert!(matches!(
            storage.append("test2", Bytes::from(vec![0u8; 20]), "text/plain"),
            Err(Error::MemoryLimitExceeded)
        ));
    }

    #[test]
    fn test_batch_append_does_not_advance_stream_seq_on_failed_commit() {
        let root = test_storage_dir();
        let storage = FileStorage::new(root, 1024, 8, false).unwrap();
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();

        let oversized = vec![Bytes::from(vec![0_u8; 9])];
        let err = storage.batch_append("test", oversized, "text/plain", Some("s1"));
        assert!(matches!(err, Err(Error::StreamSizeLimitExceeded)));

        let retry = vec![Bytes::from("ok")];
        let result = storage.batch_append("test", retry, "text/plain", Some("s1"));
        assert!(result.is_ok(), "retry with same seq should be accepted");
    }

    #[test]
    fn test_producer_append_does_not_advance_stream_seq_on_failed_commit() {
        let root = test_storage_dir();
        let storage = FileStorage::new(root, 1024, 8, false).unwrap();
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();

        let oversized = vec![Bytes::from(vec![0_u8; 9])];
        let err = storage.append_with_producer(
            "test",
            oversized,
            "text/plain",
            &producer("p1", 0, 0),
            false,
            Some("s1"),
        );
        assert!(matches!(err, Err(Error::StreamSizeLimitExceeded)));

        let retry = storage.append_with_producer(
            "test",
            vec![Bytes::from("ok")],
            "text/plain",
            &producer("p1", 0, 0),
            false,
            Some("s1"),
        );
        assert!(
            matches!(retry, Ok(ProducerAppendResult::Accepted { .. })),
            "retry with same producer seq and stream seq should succeed"
        );
    }

    #[test]
    fn test_create_with_data_rolls_back_on_memory_limit() {
        let root = test_storage_dir();
        let storage = FileStorage::new(root, 1024, 8, false).unwrap();
        let config = StreamConfig::new("text/plain".to_string());
        let oversized = vec![Bytes::from(vec![0_u8; 9])];

        let err = storage.create_stream_with_data("test", config, oversized, false);
        assert!(matches!(err, Err(Error::StreamSizeLimitExceeded)));
        assert!(
            !storage.exists("test"),
            "stream must not exist after failed create"
        );
    }

    #[test]
    fn test_create_with_data_closed_is_atomic() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string()).with_created_closed(true);

        let result = storage
            .create_stream_with_data("test", config, vec![], true)
            .unwrap();
        assert_eq!(result.status, CreateStreamResult::Created);
        assert!(result.closed, "stream must be closed in result snapshot");

        let meta = storage.head("test").unwrap();
        assert!(meta.closed, "stream must be closed via head()");

        assert!(matches!(
            storage.append("test", Bytes::from("data"), "text/plain"),
            Err(Error::StreamClosed)
        ));
    }

    #[test]
    fn test_create_with_data_appends_and_closes() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string()).with_created_closed(true);
        let messages = vec![Bytes::from("hello"), Bytes::from("world")];

        let result = storage
            .create_stream_with_data("test", config, messages, true)
            .unwrap();
        assert_eq!(result.status, CreateStreamResult::Created);
        assert!(result.closed);

        let meta = storage.head("test").unwrap();
        assert_eq!(meta.message_count, 2);
        assert!(meta.closed);
    }

    #[test]
    fn test_create_with_data_idempotent_ignores_body() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());

        let r1 = storage
            .create_stream_with_data("test", config.clone(), vec![Bytes::from("a")], false)
            .unwrap();
        assert_eq!(r1.status, CreateStreamResult::Created);

        let r2 = storage
            .create_stream_with_data("test", config, vec![Bytes::from("b")], false)
            .unwrap();
        assert_eq!(r2.status, CreateStreamResult::AlreadyExists);

        let meta = storage.head("test").unwrap();
        assert_eq!(meta.message_count, 1);
    }

    #[test]
    fn test_delete() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();

        storage
            .append("test", Bytes::from(vec![0u8; 100]), "text/plain")
            .unwrap();

        let bytes_before = storage.total_bytes();
        assert!(bytes_before >= 100);

        storage.delete("test").unwrap();
        assert!(!storage.exists("test"));

        let bytes_after = storage.total_bytes();
        assert_eq!(bytes_after, 0);

        assert!(matches!(storage.delete("test"), Err(Error::NotFound(_))));
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
    fn test_head() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();

        storage
            .append("test", Bytes::from(vec![0u8; 50]), "text/plain")
            .unwrap();
        storage
            .append("test", Bytes::from(vec![0u8; 30]), "text/plain")
            .unwrap();

        let metadata = storage.head("test").unwrap();
        assert_eq!(metadata.config.content_type, "text/plain");
        assert_eq!(metadata.message_count, 2);
        assert_eq!(metadata.total_bytes, 80);
        assert!(!metadata.closed);

        storage.close_stream("test").unwrap();
        let metadata = storage.head("test").unwrap();
        assert!(metadata.closed);
    }

    #[test]
    fn test_producer_basic_append() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();

        let result = storage
            .append_with_producer(
                "test",
                vec![Bytes::from("hello")],
                "text/plain",
                &producer("p1", 0, 0),
                false,
                None,
            )
            .unwrap();

        assert!(matches!(
            result,
            ProducerAppendResult::Accepted {
                epoch: 0,
                seq: 0,
                ..
            }
        ));
    }

    #[test]
    fn test_producer_sequential_appends() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();

        for s in 0..3 {
            let result = storage
                .append_with_producer(
                    "test",
                    vec![Bytes::from(format!("msg{s}"))],
                    "text/plain",
                    &producer("p1", 0, s),
                    false,
                    None,
                )
                .unwrap();
            assert!(matches!(
                result,
                ProducerAppendResult::Accepted { epoch: 0, seq, .. } if seq == s
            ));
        }

        let metadata = storage.head("test").unwrap();
        assert_eq!(metadata.message_count, 3);
    }

    #[test]
    fn test_producer_duplicate_detection() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();

        storage
            .append_with_producer(
                "test",
                vec![Bytes::from("hello")],
                "text/plain",
                &producer("p1", 0, 0),
                false,
                None,
            )
            .unwrap();

        let result = storage
            .append_with_producer(
                "test",
                vec![Bytes::from("hello")],
                "text/plain",
                &producer("p1", 0, 0),
                false,
                None,
            )
            .unwrap();

        assert!(matches!(
            result,
            ProducerAppendResult::Duplicate {
                epoch: 0,
                seq: 0,
                ..
            }
        ));

        let metadata = storage.head("test").unwrap();
        assert_eq!(metadata.message_count, 1);
    }

    #[test]
    fn test_producer_sequence_gap() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();

        storage
            .append_with_producer(
                "test",
                vec![Bytes::from("msg0")],
                "text/plain",
                &producer("p1", 0, 0),
                false,
                None,
            )
            .unwrap();

        let err = storage
            .append_with_producer(
                "test",
                vec![Bytes::from("msg2")],
                "text/plain",
                &producer("p1", 0, 5),
                false,
                None,
            )
            .unwrap_err();

        assert!(matches!(
            err,
            Error::SequenceGap {
                expected: 1,
                actual: 5
            }
        ));
    }

    #[test]
    fn test_producer_epoch_fencing() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();

        storage
            .append_with_producer(
                "test",
                vec![Bytes::from("msg")],
                "text/plain",
                &producer("p1", 1, 0),
                false,
                None,
            )
            .unwrap();

        let err = storage
            .append_with_producer(
                "test",
                vec![Bytes::from("zombie")],
                "text/plain",
                &producer("p1", 0, 0),
                false,
                None,
            )
            .unwrap_err();

        assert!(matches!(
            err,
            Error::EpochFenced {
                current: 1,
                received: 0
            }
        ));
    }

    #[test]
    fn test_producer_epoch_bump_resets_seq() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();

        for seq in 0..3 {
            storage
                .append_with_producer(
                    "test",
                    vec![Bytes::from(format!("e0s{seq}"))],
                    "text/plain",
                    &producer("p1", 0, seq),
                    false,
                    None,
                )
                .unwrap();
        }

        let result = storage
            .append_with_producer(
                "test",
                vec![Bytes::from("e1s0")],
                "text/plain",
                &producer("p1", 1, 0),
                false,
                None,
            )
            .unwrap();

        assert!(matches!(
            result,
            ProducerAppendResult::Accepted {
                epoch: 1,
                seq: 0,
                ..
            }
        ));
    }

    #[test]
    fn test_producer_epoch_bump_with_nonzero_seq() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();

        storage
            .append_with_producer(
                "test",
                vec![Bytes::from("msg")],
                "text/plain",
                &producer("p1", 0, 0),
                false,
                None,
            )
            .unwrap();

        let err = storage
            .append_with_producer(
                "test",
                vec![Bytes::from("bad")],
                "text/plain",
                &producer("p1", 1, 5),
                false,
                None,
            )
            .unwrap_err();

        assert!(matches!(err, Error::InvalidProducerState(_)));
    }

    #[test]
    fn test_producer_close_with_append() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();

        let result = storage
            .append_with_producer(
                "test",
                vec![Bytes::from("final")],
                "text/plain",
                &producer("p1", 0, 0),
                true,
                None,
            )
            .unwrap();

        assert!(matches!(
            result,
            ProducerAppendResult::Accepted {
                epoch: 0,
                seq: 0,
                closed: true,
                ..
            }
        ));

        let metadata = storage.head("test").unwrap();
        assert!(metadata.closed);
        assert_eq!(metadata.message_count, 1);
    }

    #[test]
    fn test_producer_multiple_producers() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();

        storage
            .append_with_producer(
                "test",
                vec![Bytes::from("a0")],
                "text/plain",
                &producer("a", 0, 0),
                false,
                None,
            )
            .unwrap();

        storage
            .append_with_producer(
                "test",
                vec![Bytes::from("b0")],
                "text/plain",
                &producer("b", 0, 0),
                false,
                None,
            )
            .unwrap();

        storage
            .append_with_producer(
                "test",
                vec![Bytes::from("a1")],
                "text/plain",
                &producer("a", 0, 1),
                false,
                None,
            )
            .unwrap();

        let metadata = storage.head("test").unwrap();
        assert_eq!(metadata.message_count, 3);
    }

    #[test]
    fn test_producer_closed_stream_returns_closed_not_gap() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();

        storage
            .append_with_producer(
                "test",
                vec![Bytes::from("final")],
                "text/plain",
                &producer("p1", 0, 0),
                true,
                None,
            )
            .unwrap();

        let err = storage
            .append_with_producer(
                "test",
                vec![Bytes::from("more")],
                "text/plain",
                &producer("p1", 0, 5),
                false,
                None,
            )
            .unwrap_err();

        assert!(
            matches!(err, Error::StreamClosed),
            "Closed stream should return StreamClosed even with gap seq, got: {err:?}"
        );
    }

    #[test]
    fn test_producer_new_with_nonzero_seq_is_gap() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();

        let err = storage
            .append_with_producer(
                "test",
                vec![Bytes::from("bad")],
                "text/plain",
                &producer("p1", 0, 3),
                false,
                None,
            )
            .unwrap_err();

        assert!(matches!(
            err,
            Error::SequenceGap {
                expected: 0,
                actual: 3
            }
        ));
    }

    #[test]
    fn test_not_found() {
        let storage = test_storage();

        assert!(matches!(
            storage.append("nonexistent", Bytes::from("data"), "text/plain"),
            Err(Error::NotFound(_))
        ));

        assert!(matches!(
            storage.read("nonexistent", &Offset::start()),
            Err(Error::NotFound(_))
        ));

        assert!(matches!(
            storage.head("nonexistent"),
            Err(Error::NotFound(_))
        ));

        assert!(matches!(
            storage.close_stream("nonexistent"),
            Err(Error::NotFound(_))
        ));
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
        assert_eq!(read.messages[0].data, Bytes::from("event-1"));
        assert_eq!(read.messages[1].data, Bytes::from("event-2"));
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
        assert_eq!(read.messages[0].data, Bytes::from("good"));
    }

    #[test]
    fn test_concurrent_appends_monotonicity() {
        use std::sync::Arc;
        use std::thread;

        let storage = Arc::new(test_storage());
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();

        let mut handles = vec![];
        for thread_id in 0..5 {
            let storage_clone = Arc::clone(&storage);
            let handle = thread::spawn(move || {
                let mut offsets = Vec::new();
                for i in 0..20 {
                    let data = Bytes::from(format!("thread {thread_id} msg {i}"));
                    let offset = storage_clone.append("test", data, "text/plain").unwrap();
                    offsets.push(offset);
                }
                offsets
            });
            handles.push(handle);
        }

        let mut all_offsets = Vec::new();
        for handle in handles {
            let offsets = handle.join().unwrap();
            all_offsets.extend(offsets);
        }

        all_offsets.sort();
        for i in 1..all_offsets.len() {
            assert_ne!(
                all_offsets[i - 1],
                all_offsets[i],
                "Duplicate offset detected"
            );
        }

        let metadata = storage.head("test").unwrap();
        assert_eq!(metadata.message_count, 100);
    }
}
