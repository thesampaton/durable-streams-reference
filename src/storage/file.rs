use super::{
    CreateStreamResult, CreateWithDataResult, Message, ProducerAppendResult, ReadResult, Storage,
    StreamConfig, StreamMetadata,
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

/// Duration after which stale producer state is cleaned up (7 days).
const PRODUCER_STATE_TTL_SECS: i64 = 7 * 24 * 60 * 60;

/// Broadcast channel capacity for long-poll/SSE notifications.
const NOTIFY_CHANNEL_CAPACITY: usize = 16;

/// Binary record header size: little-endian `u32` payload length.
const RECORD_HEADER_BYTES: u64 = 4;

#[derive(Debug, Clone)]
struct MessageIndex {
    offset: Offset,
    file_pos: u64,
    byte_len: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProducerState {
    epoch: u64,
    last_seq: u64,
    updated_at: DateTime<Utc>,
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

    fn cleanup_stale_producers(&mut self) {
        let cutoff = Utc::now()
            - chrono::TimeDelta::try_seconds(PRODUCER_STATE_TTL_SECS)
                .expect("7 days fits in TimeDelta");
        self.producers.retain(|_, state| state.updated_at > cutoff);
    }
}

/// High-throughput file-backed storage.
///
/// Design:
/// - One append-only log file per stream (`data.log`)
/// - In-memory offset/file index for fast reads
/// - Stream-level write lock serializes appends and preserves monotonic offsets
/// - Batched write per append call reduces syscall overhead
pub struct FileStorage {
    streams: RwLock<HashMap<String, Arc<RwLock<StreamEntry>>>>,
    total_bytes: RwLock<u64>,
    max_total_bytes: u64,
    max_stream_bytes: u64,
    root_dir: PathBuf,
    sync_on_append: bool,
}

impl FileStorage {
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

        let storage = Self {
            streams: RwLock::new(HashMap::new()),
            total_bytes: RwLock::new(0),
            max_total_bytes,
            max_stream_bytes,
            root_dir,
            sync_on_append,
        };
        storage.load_existing_streams()?;
        Ok(storage)
    }

    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        *self.total_bytes.read().expect("total_bytes lock poisoned")
    }

    fn stream_dir_for_name(&self, name: &str) -> PathBuf {
        let encoded = base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(name.as_bytes());
        self.root_dir.join(encoded)
    }

    fn data_log_path(dir: &Path) -> PathBuf {
        dir.join("data.log")
    }

    fn meta_path(dir: &Path) -> PathBuf {
        dir.join("meta.json")
    }

    fn write_metadata_for(name: &str, entry: &StreamEntry) -> Result<()> {
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

    fn open_stream_file(dir: &Path) -> Result<File> {
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
        let mut header = [0u8; RECORD_HEADER_BYTES as usize];

        while cursor < file_len {
            file.seek(SeekFrom::Start(cursor))
                .map_err(|e| Error::Storage(format!("failed to seek stream log: {e}")))?;

            let read = file
                .read(&mut header)
                .map_err(|e| Error::Storage(format!("failed to read stream log header: {e}")))?;

            if read == 0 {
                break;
            }

            if read < RECORD_HEADER_BYTES as usize {
                file.set_len(cursor).map_err(|e| {
                    Error::Storage(format!("failed to truncate partial record: {e}"))
                })?;
                break;
            }

            let record_len = u64::from(u32::from_le_bytes(header));
            let record_end = cursor + RECORD_HEADER_BYTES + record_len;

            if record_end > file_len {
                file.set_len(cursor).map_err(|e| {
                    Error::Storage(format!("failed to truncate partial record: {e}"))
                })?;
                break;
            }

            index.push(MessageIndex {
                offset: Offset::new(next_read_seq, next_byte_offset),
                file_pos: cursor + RECORD_HEADER_BYTES,
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

    fn is_expired(entry: &StreamEntry) -> bool {
        if let Some(expires_at) = entry.config.expires_at {
            Utc::now() >= expires_at
        } else {
            false
        }
    }

    fn get_stream(&self, name: &str) -> Option<Arc<RwLock<StreamEntry>>> {
        let streams = self.streams.read().expect("streams lock poisoned");
        streams.get(name).map(Arc::clone)
    }

    fn validate_seq(stream: &StreamEntry, seq: Option<&str>) -> Result<Option<String>> {
        if let Some(new_seq) = seq {
            if let Some(ref last) = stream.last_seq
                && new_seq <= last.as_str()
            {
                return Err(Error::SeqOrderingViolation {
                    last: last.clone(),
                    received: new_seq.to_string(),
                });
            }
            return Ok(Some(new_seq.to_string()));
        }
        Ok(None)
    }

    fn append_records(
        &self,
        name: &str,
        stream: &mut StreamEntry,
        messages: Vec<Bytes>,
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let mut total_batch_bytes = 0u64;
        let mut payload_bytes = 0u64;
        let mut sizes = Vec::with_capacity(messages.len());

        for msg in &messages {
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

        let current_total = *self.total_bytes.read().expect("total_bytes lock poisoned");
        if current_total + total_batch_bytes > self.max_total_bytes {
            return Err(Error::MemoryLimitExceeded);
        }
        if stream.total_bytes + total_batch_bytes > self.max_stream_bytes {
            return Err(Error::StreamSizeLimitExceeded);
        }

        let mut write_buf = Vec::with_capacity(
            usize::try_from(
                payload_bytes + (RECORD_HEADER_BYTES * u64::try_from(messages.len()).unwrap_or(0)),
            )
            .unwrap_or(0),
        );
        for msg in &messages {
            let len = u32::try_from(msg.len()).unwrap_or(u32::MAX);
            write_buf.extend_from_slice(&len.to_le_bytes());
            write_buf.extend_from_slice(msg);
        }

        let before_len = stream
            .file
            .metadata()
            .map_err(|e| Error::Storage(format!("failed to stat stream log before append: {e}")))?
            .len();

        stream
            .file
            .write_all(&write_buf)
            .map_err(|e| Error::Storage(format!("failed to append stream log for {name}: {e}")))?;

        if self.sync_on_append {
            stream.file.sync_data().map_err(|e| {
                Error::Storage(format!("failed to sync stream log for {name}: {e}"))
            })?;
        }

        let mut cursor = before_len;
        for len in sizes {
            let offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);
            stream.index.push(MessageIndex {
                offset,
                file_pos: cursor + RECORD_HEADER_BYTES,
                byte_len: len,
            });
            stream.next_read_seq += 1;
            stream.next_byte_offset += len;
            stream.total_bytes += len;
            cursor += RECORD_HEADER_BYTES + len;
        }

        let mut total = self.total_bytes.write().expect("total_bytes lock poisoned");
        *total += total_batch_bytes;

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

    fn remove_stream_dir(dir: &Path) -> Result<()> {
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

            let mut file = Self::open_stream_file(&path)?;
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

            if Self::is_expired(&entry) {
                Self::remove_stream_dir(&entry.dir)?;
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

            if Self::is_expired(&stream) {
                let stream_bytes = stream.total_bytes;
                let dir = stream.dir.clone();
                drop(stream);
                streams.remove(name);

                let mut total = self.total_bytes.write().expect("total_bytes lock poisoned");
                *total = total.saturating_sub(stream_bytes);
                drop(total);

                Self::remove_stream_dir(&dir)?;
            } else if stream.config == config {
                return Ok(CreateStreamResult::AlreadyExists);
            } else {
                return Err(Error::ConfigMismatch);
            }
        }

        let dir = self.stream_dir_for_name(name);
        fs::create_dir_all(&dir).map_err(|e| {
            Error::Storage(format!(
                "failed to create stream directory {}: {e}",
                dir.display()
            ))
        })?;

        let file = Self::open_stream_file(&dir)?;
        let entry = StreamEntry::new(config, file, dir);

        Self::write_metadata_for(name, &entry)?;
        streams.insert(name.to_string(), Arc::new(RwLock::new(entry)));

        Ok(CreateStreamResult::Created)
    }

    fn append(&self, name: &str, data: Bytes, content_type: &str) -> Result<Offset> {
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let mut stream = stream_arc.write().expect("stream lock poisoned");

        if Self::is_expired(&stream) {
            return Err(Error::StreamExpired);
        }

        if stream.closed {
            return Err(Error::StreamClosed);
        }

        let normalized_ct = content_type.to_lowercase();
        let expected_ct = stream.config.content_type.to_lowercase();
        if normalized_ct != expected_ct {
            return Err(Error::ContentTypeMismatch {
                expected: stream.config.content_type.clone(),
                actual: content_type.to_string(),
            });
        }

        let offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);
        self.append_records(name, &mut stream, vec![data])?;
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

        if Self::is_expired(&stream) {
            return Err(Error::StreamExpired);
        }

        if stream.closed {
            return Err(Error::StreamClosed);
        }

        let normalized_ct = content_type.to_lowercase();
        let expected_ct = stream.config.content_type.to_lowercase();
        if normalized_ct != expected_ct {
            return Err(Error::ContentTypeMismatch {
                expected: stream.config.content_type.clone(),
                actual: content_type.to_string(),
            });
        }

        let pending_seq = Self::validate_seq(&stream, seq)?;
        self.append_records(name, &mut stream, messages)?;
        if let Some(new_seq) = pending_seq {
            stream.last_seq = Some(new_seq);
        }

        Ok(Offset::new(stream.next_read_seq, stream.next_byte_offset))
    }

    fn read(&self, name: &str, from_offset: &Offset) -> Result<ReadResult> {
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let stream = stream_arc.read().expect("stream lock poisoned");

        if Self::is_expired(&stream) {
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

            Self::remove_stream_dir(&dir)?;
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

        if Self::is_expired(&stream) {
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

        if Self::is_expired(&stream) {
            return Err(Error::StreamExpired);
        }

        stream.closed = true;
        Self::write_metadata_for(name, &stream)?;

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

        if Self::is_expired(&stream) {
            return Err(Error::StreamExpired);
        }

        stream.cleanup_stale_producers();

        if !messages.is_empty() {
            let normalized_ct = content_type.to_lowercase();
            let expected_ct = stream.config.content_type.to_lowercase();
            if normalized_ct != expected_ct {
                return Err(Error::ContentTypeMismatch {
                    expected: stream.config.content_type.clone(),
                    actual: content_type.to_string(),
                });
            }
        }

        let now = Utc::now();

        if let Some(state) = stream.producers.get(producer.id.as_str()) {
            if producer.epoch < state.epoch {
                return Err(Error::EpochFenced {
                    current: state.epoch,
                    received: producer.epoch,
                });
            }

            if producer.epoch == state.epoch && producer.seq <= state.last_seq {
                return Ok(ProducerAppendResult::Duplicate {
                    epoch: state.epoch,
                    seq: state.last_seq,
                    next_offset: Offset::new(stream.next_read_seq, stream.next_byte_offset),
                    closed: stream.closed,
                });
            }

            if stream.closed {
                return Err(Error::StreamClosed);
            }

            if producer.epoch > state.epoch {
                if producer.seq != 0 {
                    return Err(Error::InvalidProducerState(
                        "new epoch must start at seq 0".to_string(),
                    ));
                }
            } else if producer.seq > state.last_seq + 1 {
                return Err(Error::SequenceGap {
                    expected: state.last_seq + 1,
                    actual: producer.seq,
                });
            }
        } else {
            if stream.closed {
                return Err(Error::StreamClosed);
            }
            if producer.seq != 0 {
                return Err(Error::SequenceGap {
                    expected: 0,
                    actual: producer.seq,
                });
            }
        }

        let pending_seq = Self::validate_seq(&stream, seq)?;
        self.append_records(name, &mut stream, messages)?;

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

        Self::write_metadata_for(name, &stream)?;

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

            if Self::is_expired(&stream) {
                let stream_bytes = stream.total_bytes;
                let dir = stream.dir.clone();
                drop(stream);
                streams.remove(name);

                let mut total = self.total_bytes.write().expect("total_bytes lock poisoned");
                *total = total.saturating_sub(stream_bytes);
                drop(total);

                Self::remove_stream_dir(&dir)?;
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

        let dir = self.stream_dir_for_name(name);
        fs::create_dir_all(&dir).map_err(|e| {
            Error::Storage(format!(
                "failed to create stream directory {}: {e}",
                dir.display()
            ))
        })?;

        let file = Self::open_stream_file(&dir)?;
        let mut entry = StreamEntry::new(config, file, dir);

        if !messages.is_empty() {
            self.append_records(name, &mut entry, messages)?;
        }
        if should_close {
            entry.closed = true;
        }

        let next_offset = Offset::new(entry.next_read_seq, entry.next_byte_offset);
        let closed = entry.closed;

        Self::write_metadata_for(name, &entry)?;
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
            !Self::is_expired(&stream)
        } else {
            false
        }
    }

    fn subscribe(&self, name: &str) -> Option<broadcast::Receiver<()>> {
        let stream_arc = self.get_stream(name)?;
        let stream = stream_arc.read().expect("stream lock poisoned");

        if Self::is_expired(&stream) {
            return None;
        }

        Some(stream.notify.subscribe())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_storage_dir() -> PathBuf {
        let stamp = Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_default()
            .to_string();
        std::env::temp_dir().join(format!("ds-file-storage-test-{stamp}"))
    }

    fn test_storage() -> FileStorage {
        FileStorage::new(test_storage_dir(), 1024 * 1024, 100 * 1024, false)
            .expect("file storage should initialize")
    }

    #[test]
    fn test_append_and_read_roundtrip() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());

        storage
            .create_stream("test", config)
            .expect("stream should be created");

        let first = storage
            .append("test", Bytes::from("hello"), "text/plain")
            .expect("append should succeed");
        let second = storage
            .append("test", Bytes::from("world"), "text/plain")
            .expect("append should succeed");

        assert!(first < second);

        let read = storage
            .read("test", &Offset::start())
            .expect("read should succeed");
        assert_eq!(read.messages.len(), 2);
        assert_eq!(read.messages[0].data, Bytes::from("hello"));
        assert_eq!(read.messages[1].data, Bytes::from("world"));
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
}
