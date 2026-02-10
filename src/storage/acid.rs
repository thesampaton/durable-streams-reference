use super::{
    CreateStreamResult, CreateWithDataResult, NOTIFY_CHANNEL_CAPACITY, ProducerAppendResult,
    ProducerCheck, ProducerState, ReadResult, Storage, StreamConfig, StreamMetadata,
};
use crate::protocol::error::{Error, Result};
use crate::protocol::offset::Offset;
use crate::protocol::producer::ProducerHeaders;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use redb::{Database, Durability, ReadableDatabase, ReadableTable, Table, TableDefinition};
use seahash::hash;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::broadcast;

const STREAMS: TableDefinition<&str, &[u8]> = TableDefinition::new("streams");
const MESSAGES: TableDefinition<(&str, u64, u64), &[u8]> = TableDefinition::new("messages");

const LAYOUT_FORMAT_VERSION: u32 = 1;
const HASH_POLICY: &str = "seahash-v1";

#[derive(Debug, Serialize, Deserialize)]
struct LayoutManifest {
    format_version: u32,
    shard_count: usize,
    hash_policy: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredStreamMeta {
    config: StreamConfig,
    closed: bool,
    next_read_seq: u64,
    next_byte_offset: u64,
    total_bytes: u64,
    created_at: DateTime<Utc>,
    last_seq: Option<String>,
    producers: HashMap<String, ProducerState>,
}

#[derive(Debug)]
struct AcidShard {
    db: Database,
}

#[allow(clippy::module_name_repetitions)]
pub struct AcidStorage {
    shards: Vec<AcidShard>,
    shard_count: usize,
    total_bytes: AtomicU64,
    max_total_bytes: u64,
    max_stream_bytes: u64,
    notifiers: RwLock<HashMap<String, broadcast::Sender<()>>>,
}

impl AcidStorage {
    /// # Errors
    ///
    /// Returns `Error::Storage` if storage layout validation fails, shard
    /// databases cannot be opened, or on-disk recovery cannot complete.
    pub fn new(
        root_dir: impl Into<PathBuf>,
        shard_count: usize,
        max_total_bytes: u64,
        max_stream_bytes: u64,
    ) -> Result<Self> {
        Self::validate_shard_count(shard_count)?;

        let root_dir = root_dir.into();
        let acid_dir = Self::acid_dir(&root_dir);
        fs::create_dir_all(&acid_dir).map_err(|e| {
            Self::storage_err(
                format!(
                    "failed to create acid storage directory {}",
                    acid_dir.display()
                ),
                e,
            )
        })?;

        Self::load_or_create_layout(&acid_dir, shard_count)?;

        let mut shards = Vec::with_capacity(shard_count);
        for idx in 0..shard_count {
            let shard_path = acid_dir.join(format!("shard_{idx:02x}.redb"));
            let db = Database::create(&shard_path).map_err(|e| {
                Self::storage_err(
                    format!("failed to open shard database {}", shard_path.display()),
                    e,
                )
            })?;
            Self::ensure_schema(&db)?;
            shards.push(AcidShard { db });
        }

        let storage = Self {
            shards,
            shard_count,
            total_bytes: AtomicU64::new(0),
            max_total_bytes,
            max_stream_bytes,
            notifiers: RwLock::new(HashMap::new()),
        };

        let total_bytes = storage.rebuild_state_from_disk()?;
        storage.total_bytes.store(total_bytes, Ordering::Release);

        Ok(storage)
    }

    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Acquire)
    }

    fn validate_shard_count(shard_count: usize) -> Result<()> {
        if !(1..=256).contains(&shard_count) {
            return Err(Error::Storage(format!(
                "acid shard count must be in range 1..=256, got {shard_count}"
            )));
        }
        if !shard_count.is_power_of_two() {
            return Err(Error::Storage(format!(
                "acid shard count must be a power of two, got {shard_count}"
            )));
        }
        Ok(())
    }

    fn storage_err(context: impl Into<String>, err: impl std::fmt::Display) -> Error {
        Error::Storage(format!("{}: {err}", context.into()))
    }

    fn acid_dir(root_dir: &Path) -> PathBuf {
        root_dir.join("acid")
    }

    fn layout_path(acid_dir: &Path) -> PathBuf {
        acid_dir.join("layout.json")
    }

    fn load_or_create_layout(acid_dir: &Path, shard_count: usize) -> Result<()> {
        let layout_path = Self::layout_path(acid_dir);
        if layout_path.exists() {
            let payload = fs::read(&layout_path).map_err(|e| {
                Self::storage_err(
                    format!("failed to read acid layout file {}", layout_path.display()),
                    e,
                )
            })?;
            let manifest: LayoutManifest = serde_json::from_slice(&payload).map_err(|e| {
                Self::storage_err(
                    format!("failed to parse acid layout file {}", layout_path.display()),
                    e,
                )
            })?;

            if manifest.format_version != LAYOUT_FORMAT_VERSION {
                return Err(Error::Storage(format!(
                    "acid layout mismatch: format_version={}, expected={}",
                    manifest.format_version, LAYOUT_FORMAT_VERSION
                )));
            }
            if manifest.shard_count != shard_count {
                return Err(Error::Storage(format!(
                    "acid layout mismatch: shard_count={}, expected={shard_count}",
                    manifest.shard_count
                )));
            }
            if manifest.hash_policy != HASH_POLICY {
                return Err(Error::Storage(format!(
                    "acid layout mismatch: hash_policy='{}', expected='{}'",
                    manifest.hash_policy, HASH_POLICY
                )));
            }
            return Ok(());
        }

        let manifest = LayoutManifest {
            format_version: LAYOUT_FORMAT_VERSION,
            shard_count,
            hash_policy: HASH_POLICY.to_string(),
        };
        let payload = serde_json::to_vec_pretty(&manifest)
            .map_err(|e| Self::storage_err("failed to serialize acid layout manifest", e))?;

        let tmp_path = acid_dir.join("layout.json.tmp");
        fs::write(&tmp_path, payload).map_err(|e| {
            Self::storage_err(
                format!("failed to write temp layout file {}", tmp_path.display()),
                e,
            )
        })?;
        fs::rename(&tmp_path, &layout_path).map_err(|e| {
            Self::storage_err(
                format!("failed to write layout file {}", layout_path.display()),
                e,
            )
        })?;

        Ok(())
    }

    #[must_use]
    fn shard_index(&self, name: &str) -> usize {
        let hash_u64 = hash(name.as_bytes());
        let hash_usize = usize::try_from(hash_u64).unwrap_or_else(|_| {
            let masked = hash_u64 & u64::from(u32::MAX);
            usize::try_from(masked).expect("masked hash value must fit in usize")
        });
        hash_usize & (self.shard_count - 1)
    }

    fn shard(&self, name: &str) -> &AcidShard {
        &self.shards[self.shard_index(name)]
    }

    fn reserve_total_bytes(&self, bytes: u64) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }

        if self
            .total_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_add(bytes)
                    .filter(|next| *next <= self.max_total_bytes)
            })
            .is_err()
        {
            return Err(Error::MemoryLimitExceeded);
        }
        Ok(())
    }

    fn rollback_total_bytes(&self, bytes: u64) {
        self.saturating_sub_total_bytes(bytes);
    }

    fn saturating_sub_total_bytes(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }

        self.total_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(bytes))
            })
            .ok();
    }

    fn read_stream_meta<T>(streams: &T, name: &str) -> Result<Option<StoredStreamMeta>>
    where
        T: ReadableTable<&'static str, &'static [u8]>,
    {
        let payload = streams
            .get(name)
            .map_err(|e| Self::storage_err("failed to read stream metadata", e))?;

        if let Some(payload) = payload {
            let meta = serde_json::from_slice(payload.value())
                .map_err(|e| Self::storage_err("failed to parse stream metadata", e))?;
            Ok(Some(meta))
        } else {
            Ok(None)
        }
    }

    fn write_stream_meta(
        streams: &mut Table<'_, &'static str, &'static [u8]>,
        name: &str,
        meta: &StoredStreamMeta,
    ) -> Result<()> {
        let payload = serde_json::to_vec(meta)
            .map_err(|e| Self::storage_err("failed to serialize stream metadata", e))?;
        streams
            .insert(name, payload.as_slice())
            .map_err(|e| Self::storage_err("failed to write stream metadata", e))?;
        Ok(())
    }

    fn delete_stream_messages(
        messages: &mut Table<'_, (&'static str, u64, u64), &'static [u8]>,
        name: &str,
    ) -> Result<()> {
        // redb table iterators cannot be safely mutated in-place; we must collect
        // keys first, then remove in a second pass.
        let mut keys = Vec::new();
        let iter = messages
            .range((name, 0_u64, 0_u64)..=(name, u64::MAX, u64::MAX))
            .map_err(|e| Self::storage_err("failed to iterate stream messages", e))?;

        for item in iter {
            let (key, _) = item.map_err(|e| Self::storage_err("failed to read message key", e))?;
            let (_, read_seq, byte_offset) = key.value();
            keys.push((read_seq, byte_offset));
        }

        for (read_seq, byte_offset) in keys {
            messages
                .remove((name, read_seq, byte_offset))
                .map_err(|e| Self::storage_err("failed to delete message", e))?;
        }

        Ok(())
    }

    fn notifier_sender(&self, name: &str) -> broadcast::Sender<()> {
        if let Some(sender) = self
            .notifiers
            .read()
            .expect("notifiers lock poisoned")
            .get(name)
        {
            return sender.clone();
        }

        let mut guard = self.notifiers.write().expect("notifiers lock poisoned");
        guard
            .entry(name.to_string())
            .or_insert_with(|| {
                let (sender, _) = broadcast::channel(NOTIFY_CHANNEL_CAPACITY);
                sender
            })
            .clone()
    }

    fn notify_stream(&self, name: &str) {
        if let Some(sender) = self
            .notifiers
            .read()
            .expect("notifiers lock poisoned")
            .get(name)
        {
            let _ = sender.send(());
        }
    }

    fn drop_notifier(&self, name: &str) {
        self.notifiers
            .write()
            .expect("notifiers lock poisoned")
            .remove(name);
    }

    fn new_stream_meta(config: StreamConfig) -> StoredStreamMeta {
        StoredStreamMeta {
            config,
            closed: false,
            next_read_seq: 0,
            next_byte_offset: 0,
            total_bytes: 0,
            created_at: Utc::now(),
            last_seq: None,
            producers: HashMap::new(),
        }
    }

    fn batch_bytes(messages: &[Bytes]) -> u64 {
        messages
            .iter()
            .map(|m| u64::try_from(m.len()).unwrap_or(u64::MAX))
            .sum()
    }

    fn begin_write_txn(db: &Database) -> Result<redb::WriteTransaction> {
        let mut txn = db
            .begin_write()
            .map_err(|e| Self::storage_err("failed to begin write transaction", e))?;
        txn.set_durability(Durability::Immediate)
            .map_err(|e| Self::storage_err("failed to set write durability", e))?;
        Ok(txn)
    }

    fn ensure_schema(db: &Database) -> Result<()> {
        let txn = Self::begin_write_txn(db)?;
        let streams = txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to initialize streams table", e))?;
        let messages = txn
            .open_table(MESSAGES)
            .map_err(|e| Self::storage_err("failed to initialize messages table", e))?;
        drop(messages);
        drop(streams);
        txn.commit()
            .map_err(|e| Self::storage_err("failed to commit schema initialization", e))?;
        Ok(())
    }

    fn rebuild_state_from_disk(&self) -> Result<u64> {
        let mut total = 0_u64;
        for shard in &self.shards {
            total = total.saturating_add(self.rebuild_shard(shard)?);
        }
        Ok(total)
    }

    fn rebuild_shard(&self, shard: &AcidShard) -> Result<u64> {
        let read_txn = shard
            .db
            .begin_read()
            .map_err(|e| Self::storage_err("failed to begin read transaction", e))?;
        let streams = read_txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to open streams table", e))?;

        let mut live_bytes = 0_u64;
        let mut expired_names = Vec::new();

        {
            let iter = streams
                .iter()
                .map_err(|e| Self::storage_err("failed to iterate stream metadata", e))?;
            for item in iter {
                let (key, value) =
                    item.map_err(|e| Self::storage_err("failed to read stream metadata", e))?;
                let stream_name = key.value().to_string();
                let meta: StoredStreamMeta = serde_json::from_slice(value.value())
                    .map_err(|e| Self::storage_err("failed to parse stream metadata", e))?;
                if super::is_stream_expired(&meta.config) {
                    expired_names.push(stream_name);
                } else {
                    live_bytes = live_bytes.saturating_add(meta.total_bytes);
                }
            }
        }

        drop(streams);
        drop(read_txn);

        if expired_names.is_empty() {
            return Ok(live_bytes);
        }

        let txn = Self::begin_write_txn(&shard.db)?;
        let mut streams = txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to open streams table", e))?;
        let mut messages = txn
            .open_table(MESSAGES)
            .map_err(|e| Self::storage_err("failed to open messages table", e))?;

        for name in &expired_names {
            Self::delete_stream_messages(&mut messages, name)?;
            streams
                .remove(name.as_str())
                .map_err(|e| Self::storage_err("failed to remove expired stream", e))?;
            self.drop_notifier(name);
        }

        drop(messages);
        drop(streams);
        txn.commit()
            .map_err(|e| Self::storage_err("failed to commit startup cleanup", e))?;

        Ok(live_bytes)
    }
}

impl Storage for AcidStorage {
    fn create_stream(&self, name: &str, config: StreamConfig) -> Result<CreateStreamResult> {
        let shard = self.shard(name);
        let txn = Self::begin_write_txn(&shard.db)?;
        let mut streams = txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to open streams table", e))?;
        let mut messages = txn
            .open_table(MESSAGES)
            .map_err(|e| Self::storage_err("failed to open messages table", e))?;

        let mut removed_expired_bytes = 0_u64;

        if let Some(existing) = Self::read_stream_meta(&streams, name)? {
            if super::is_stream_expired(&existing.config) {
                removed_expired_bytes = existing.total_bytes;
                Self::delete_stream_messages(&mut messages, name)?;
                streams
                    .remove(name)
                    .map_err(|e| Self::storage_err("failed to remove expired stream", e))?;
            } else if existing.config == config {
                return Ok(CreateStreamResult::AlreadyExists);
            } else {
                return Err(Error::ConfigMismatch);
            }
        }

        let meta = Self::new_stream_meta(config);
        Self::write_stream_meta(&mut streams, name, &meta)?;

        drop(messages);
        drop(streams);
        txn.commit()
            .map_err(|e| Self::storage_err("failed to commit create stream", e))?;

        if removed_expired_bytes > 0 {
            self.saturating_sub_total_bytes(removed_expired_bytes);
            self.drop_notifier(name);
        }

        Ok(CreateStreamResult::Created)
    }

    fn append(&self, name: &str, data: Bytes, content_type: &str) -> Result<Offset> {
        let message_bytes = u64::try_from(data.len()).unwrap_or(u64::MAX);
        self.reserve_total_bytes(message_bytes)?;

        let result = (|| {
            let shard = self.shard(name);
            let txn = Self::begin_write_txn(&shard.db)?;
            let mut streams = txn
                .open_table(STREAMS)
                .map_err(|e| Self::storage_err("failed to open streams table", e))?;
            let mut messages = txn
                .open_table(MESSAGES)
                .map_err(|e| Self::storage_err("failed to open messages table", e))?;

            let mut meta = Self::read_stream_meta(&streams, name)?
                .ok_or_else(|| Error::NotFound(name.to_string()))?;

            if super::is_stream_expired(&meta.config) {
                return Err(Error::StreamExpired);
            }
            if meta.closed {
                return Err(Error::StreamClosed);
            }

            super::validate_content_type(&meta.config.content_type, content_type)?;

            if meta.total_bytes + message_bytes > self.max_stream_bytes {
                return Err(Error::StreamSizeLimitExceeded);
            }

            let offset = Offset::new(meta.next_read_seq, meta.next_byte_offset);
            messages
                .insert(
                    (name, meta.next_read_seq, meta.next_byte_offset),
                    data.as_ref(),
                )
                .map_err(|e| Self::storage_err("failed to append message", e))?;

            meta.next_read_seq += 1;
            meta.next_byte_offset += message_bytes;
            meta.total_bytes += message_bytes;

            Self::write_stream_meta(&mut streams, name, &meta)?;

            drop(messages);
            drop(streams);
            txn.commit()
                .map_err(|e| Self::storage_err("failed to commit append", e))?;

            Ok(offset)
        })();

        if result.is_err() {
            self.rollback_total_bytes(message_bytes);
            return result;
        }

        self.notify_stream(name);
        result
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

        let batch_bytes = Self::batch_bytes(&messages);
        self.reserve_total_bytes(batch_bytes)?;

        let result = (|| {
            let shard = self.shard(name);
            let txn = Self::begin_write_txn(&shard.db)?;
            let mut streams = txn
                .open_table(STREAMS)
                .map_err(|e| Self::storage_err("failed to open streams table", e))?;
            let mut message_table = txn
                .open_table(MESSAGES)
                .map_err(|e| Self::storage_err("failed to open messages table", e))?;

            let mut meta = Self::read_stream_meta(&streams, name)?
                .ok_or_else(|| Error::NotFound(name.to_string()))?;

            if super::is_stream_expired(&meta.config) {
                return Err(Error::StreamExpired);
            }
            if meta.closed {
                return Err(Error::StreamClosed);
            }

            super::validate_content_type(&meta.config.content_type, content_type)?;
            let pending_seq = super::validate_seq(meta.last_seq.as_deref(), seq)?;

            if meta.total_bytes + batch_bytes > self.max_stream_bytes {
                return Err(Error::StreamSizeLimitExceeded);
            }

            for data in &messages {
                let len = u64::try_from(data.len()).unwrap_or(u64::MAX);
                message_table
                    .insert(
                        (name, meta.next_read_seq, meta.next_byte_offset),
                        data.as_ref(),
                    )
                    .map_err(|e| Self::storage_err("failed to append batch message", e))?;
                meta.next_read_seq += 1;
                meta.next_byte_offset += len;
                meta.total_bytes += len;
            }

            if let Some(new_seq) = pending_seq {
                meta.last_seq = Some(new_seq);
            }

            let next_offset = Offset::new(meta.next_read_seq, meta.next_byte_offset);
            Self::write_stream_meta(&mut streams, name, &meta)?;

            drop(message_table);
            drop(streams);
            txn.commit()
                .map_err(|e| Self::storage_err("failed to commit batch append", e))?;

            Ok(next_offset)
        })();

        if result.is_err() {
            self.rollback_total_bytes(batch_bytes);
            return result;
        }

        self.notify_stream(name);
        result
    }

    fn read(&self, name: &str, from_offset: &Offset) -> Result<ReadResult> {
        let shard = self.shard(name);
        let txn = shard
            .db
            .begin_read()
            .map_err(|e| Self::storage_err("failed to begin read transaction", e))?;

        let streams = txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to open streams table", e))?;
        let message_table = txn
            .open_table(MESSAGES)
            .map_err(|e| Self::storage_err("failed to open messages table", e))?;

        let meta = Self::read_stream_meta(&streams, name)?
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        if super::is_stream_expired(&meta.config) {
            return Err(Error::StreamExpired);
        }

        if from_offset.is_now() {
            return Ok(ReadResult {
                messages: Vec::new(),
                next_offset: Offset::new(meta.next_read_seq, meta.next_byte_offset),
                at_tail: true,
                closed: meta.closed,
            });
        }

        let (start_read_seq, start_byte_offset) = if from_offset.is_start() {
            (0_u64, 0_u64)
        } else {
            from_offset.parse_components().ok_or_else(|| {
                Error::InvalidOffset("non-concrete offset in read range".to_string())
            })?
        };

        let iter = message_table
            .range((name, start_read_seq, start_byte_offset)..=(name, u64::MAX, u64::MAX))
            .map_err(|e| Self::storage_err("failed to read stream range", e))?;

        let mut messages = Vec::new();
        for item in iter {
            let (_, value) =
                item.map_err(|e| Self::storage_err("failed to read stream message", e))?;
            messages.push(Bytes::copy_from_slice(value.value()));
        }

        Ok(ReadResult {
            messages,
            next_offset: Offset::new(meta.next_read_seq, meta.next_byte_offset),
            at_tail: true,
            closed: meta.closed,
        })
    }

    fn delete(&self, name: &str) -> Result<()> {
        let shard = self.shard(name);
        let txn = Self::begin_write_txn(&shard.db)?;
        let mut streams = txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to open streams table", e))?;
        let mut messages = txn
            .open_table(MESSAGES)
            .map_err(|e| Self::storage_err("failed to open messages table", e))?;

        let meta = Self::read_stream_meta(&streams, name)?
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        Self::delete_stream_messages(&mut messages, name)?;
        streams
            .remove(name)
            .map_err(|e| Self::storage_err("failed to remove stream metadata", e))?;

        drop(messages);
        drop(streams);
        txn.commit()
            .map_err(|e| Self::storage_err("failed to commit delete", e))?;

        self.saturating_sub_total_bytes(meta.total_bytes);
        self.drop_notifier(name);
        Ok(())
    }

    fn head(&self, name: &str) -> Result<StreamMetadata> {
        let shard = self.shard(name);
        let txn = shard
            .db
            .begin_read()
            .map_err(|e| Self::storage_err("failed to begin read transaction", e))?;

        let streams = txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to open streams table", e))?;

        let meta = Self::read_stream_meta(&streams, name)?
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        if super::is_stream_expired(&meta.config) {
            return Err(Error::StreamExpired);
        }

        Ok(StreamMetadata {
            config: meta.config,
            next_offset: Offset::new(meta.next_read_seq, meta.next_byte_offset),
            closed: meta.closed,
            total_bytes: meta.total_bytes,
            message_count: meta.next_read_seq,
            created_at: meta.created_at,
        })
    }

    fn close_stream(&self, name: &str) -> Result<()> {
        let shard = self.shard(name);
        let txn = Self::begin_write_txn(&shard.db)?;
        let mut streams = txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to open streams table", e))?;

        let mut meta = Self::read_stream_meta(&streams, name)?
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        if super::is_stream_expired(&meta.config) {
            return Err(Error::StreamExpired);
        }

        meta.closed = true;
        Self::write_stream_meta(&mut streams, name, &meta)?;

        drop(streams);
        txn.commit()
            .map_err(|e| Self::storage_err("failed to commit close stream", e))?;

        self.notify_stream(name);
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
        let batch_bytes = Self::batch_bytes(&messages);
        self.reserve_total_bytes(batch_bytes)?;

        let result = (|| {
            let shard = self.shard(name);
            let txn = Self::begin_write_txn(&shard.db)?;
            let mut streams = txn
                .open_table(STREAMS)
                .map_err(|e| Self::storage_err("failed to open streams table", e))?;
            let mut message_table = txn
                .open_table(MESSAGES)
                .map_err(|e| Self::storage_err("failed to open messages table", e))?;

            let mut meta = Self::read_stream_meta(&streams, name)?
                .ok_or_else(|| Error::NotFound(name.to_string()))?;

            if super::is_stream_expired(&meta.config) {
                return Err(Error::StreamExpired);
            }

            super::cleanup_stale_producers(&mut meta.producers);

            if !messages.is_empty() {
                super::validate_content_type(&meta.config.content_type, content_type)?;
            }

            match super::check_producer(
                meta.producers.get(producer.id.as_str()),
                producer,
                meta.closed,
            )? {
                ProducerCheck::Accept => {}
                ProducerCheck::Duplicate { epoch, seq } => {
                    return Ok(ProducerAppendResult::Duplicate {
                        epoch,
                        seq,
                        next_offset: Offset::new(meta.next_read_seq, meta.next_byte_offset),
                        closed: meta.closed,
                    });
                }
            }

            let pending_seq = super::validate_seq(meta.last_seq.as_deref(), seq)?;

            if meta.total_bytes + batch_bytes > self.max_stream_bytes {
                return Err(Error::StreamSizeLimitExceeded);
            }

            for data in &messages {
                let len = u64::try_from(data.len()).unwrap_or(u64::MAX);
                message_table
                    .insert(
                        (name, meta.next_read_seq, meta.next_byte_offset),
                        data.as_ref(),
                    )
                    .map_err(|e| Self::storage_err("failed to append producer message", e))?;
                meta.next_read_seq += 1;
                meta.next_byte_offset += len;
                meta.total_bytes += len;
            }

            if let Some(new_seq) = pending_seq {
                meta.last_seq = Some(new_seq);
            }
            if should_close {
                meta.closed = true;
            }

            meta.producers.insert(
                producer.id.clone(),
                ProducerState {
                    epoch: producer.epoch,
                    last_seq: producer.seq,
                    updated_at: Utc::now(),
                },
            );

            let next_offset = Offset::new(meta.next_read_seq, meta.next_byte_offset);
            let closed = meta.closed;

            Self::write_stream_meta(&mut streams, name, &meta)?;
            drop(message_table);
            drop(streams);
            txn.commit()
                .map_err(|e| Self::storage_err("failed to commit producer append", e))?;

            Ok(ProducerAppendResult::Accepted {
                epoch: producer.epoch,
                seq: producer.seq,
                next_offset,
                closed,
            })
        })();

        if result.is_err() || matches!(result, Ok(ProducerAppendResult::Duplicate { .. })) {
            self.rollback_total_bytes(batch_bytes);
        }

        if result.is_ok() && (!messages.is_empty() || should_close) {
            self.notify_stream(name);
        }

        result
    }

    fn create_stream_with_data(
        &self,
        name: &str,
        config: StreamConfig,
        messages: Vec<Bytes>,
        should_close: bool,
    ) -> Result<CreateWithDataResult> {
        let batch_bytes = Self::batch_bytes(&messages);

        let mut reserved = false;
        let mut removed_expired_bytes = 0_u64;

        let result = (|| {
            let shard = self.shard(name);
            let txn = Self::begin_write_txn(&shard.db)?;
            let mut streams = txn
                .open_table(STREAMS)
                .map_err(|e| Self::storage_err("failed to open streams table", e))?;
            let mut message_table = txn
                .open_table(MESSAGES)
                .map_err(|e| Self::storage_err("failed to open messages table", e))?;

            if let Some(existing) = Self::read_stream_meta(&streams, name)? {
                if super::is_stream_expired(&existing.config) {
                    removed_expired_bytes = existing.total_bytes;
                    Self::delete_stream_messages(&mut message_table, name)?;
                    streams
                        .remove(name)
                        .map_err(|e| Self::storage_err("failed to remove expired stream", e))?;
                } else if existing.config == config {
                    return Ok(CreateWithDataResult {
                        status: CreateStreamResult::AlreadyExists,
                        next_offset: Offset::new(existing.next_read_seq, existing.next_byte_offset),
                        closed: existing.closed,
                    });
                } else {
                    return Err(Error::ConfigMismatch);
                }
            }

            if batch_bytes > 0 {
                self.reserve_total_bytes(batch_bytes)?;
                reserved = true;
            }

            let mut meta = Self::new_stream_meta(config);

            if batch_bytes > 0 {
                if meta.total_bytes + batch_bytes > self.max_stream_bytes {
                    return Err(Error::StreamSizeLimitExceeded);
                }
                for data in &messages {
                    let len = u64::try_from(data.len()).unwrap_or(u64::MAX);
                    message_table
                        .insert(
                            (name, meta.next_read_seq, meta.next_byte_offset),
                            data.as_ref(),
                        )
                        .map_err(|e| {
                            Self::storage_err("failed to append create-with-data message", e)
                        })?;
                    meta.next_read_seq += 1;
                    meta.next_byte_offset += len;
                    meta.total_bytes += len;
                }
            }

            if should_close {
                meta.closed = true;
            }

            let next_offset = Offset::new(meta.next_read_seq, meta.next_byte_offset);
            let closed = meta.closed;

            Self::write_stream_meta(&mut streams, name, &meta)?;
            drop(message_table);
            drop(streams);
            txn.commit()
                .map_err(|e| Self::storage_err("failed to commit create stream with data", e))?;

            Ok(CreateWithDataResult {
                status: CreateStreamResult::Created,
                next_offset,
                closed,
            })
        })();

        if result.is_err() && reserved {
            self.rollback_total_bytes(batch_bytes);
        }

        if result.is_ok() {
            if removed_expired_bytes > 0 {
                self.saturating_sub_total_bytes(removed_expired_bytes);
                self.drop_notifier(name);
            }
            if should_close || !messages.is_empty() {
                self.notify_stream(name);
            }
        }

        result
    }

    fn exists(&self, name: &str) -> bool {
        let shard = self.shard(name);
        let Ok(txn) = shard.db.begin_read() else {
            return false;
        };
        let Ok(streams) = txn.open_table(STREAMS) else {
            return false;
        };

        match Self::read_stream_meta(&streams, name) {
            Ok(Some(meta)) => !super::is_stream_expired(&meta.config),
            _ => false,
        }
    }

    fn subscribe(&self, name: &str) -> Option<broadcast::Receiver<()>> {
        let shard = self.shard(name);
        let txn = shard.db.begin_read().ok()?;
        let streams = txn.open_table(STREAMS).ok()?;
        let meta = Self::read_stream_meta(&streams, name).ok()??;

        if super::is_stream_expired(&meta.config) {
            return None;
        }

        Some(self.notifier_sender(name).subscribe())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;

    fn test_storage_dir() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let stamp = Utc::now().timestamp_nanos_opt().unwrap_or_default();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        std::env::temp_dir().join(format!("ds-acid-storage-test-{stamp}-{pid}-{seq}"))
    }

    fn test_storage() -> AcidStorage {
        AcidStorage::new(test_storage_dir(), 16, 1024 * 1024, 100 * 1024)
            .expect("acid storage should initialize")
    }

    fn producer(id: &str, epoch: u64, seq: u64) -> ProducerHeaders {
        ProducerHeaders {
            id: id.to_string(),
            epoch,
            seq,
        }
    }

    #[test]
    fn test_restore_from_disk() {
        let root = test_storage_dir();
        let cfg = StreamConfig::new("text/plain".to_string());

        {
            let storage = AcidStorage::new(root.clone(), 16, 1024 * 1024, 100 * 1024).unwrap();
            storage.create_stream("events", cfg.clone()).unwrap();
            storage
                .append("events", Bytes::from("event-1"), "text/plain")
                .unwrap();
            storage
                .append("events", Bytes::from("event-2"), "text/plain")
                .unwrap();
        }

        let restored = AcidStorage::new(root, 16, 1024 * 1024, 100 * 1024).unwrap();
        let read = restored.read("events", &Offset::start()).unwrap();

        assert_eq!(read.messages.len(), 2);
        assert_eq!(read.messages[0], Bytes::from("event-1"));
        assert_eq!(read.messages[1], Bytes::from("event-2"));
    }

    #[test]
    fn test_restore_closed_stream_from_disk() {
        let root = test_storage_dir();
        let cfg = StreamConfig::new("text/plain".to_string());

        {
            let storage = AcidStorage::new(root.clone(), 16, 1024 * 1024, 100 * 1024).unwrap();
            storage.create_stream("s", cfg.clone()).unwrap();
            storage
                .append("s", Bytes::from("data"), "text/plain")
                .unwrap();
            storage.close_stream("s").unwrap();
        }

        let restored = AcidStorage::new(root, 16, 1024 * 1024, 100 * 1024).unwrap();
        let meta = restored.head("s").unwrap();
        assert!(meta.closed);
        assert_eq!(meta.message_count, 1);

        assert!(matches!(
            restored.append("s", Bytes::from("more"), "text/plain"),
            Err(Error::StreamClosed)
        ));
    }

    #[test]
    fn test_restart_preserves_producer_state() {
        let root = test_storage_dir();

        {
            let storage = AcidStorage::new(root.clone(), 16, 1024 * 1024, 100 * 1024).unwrap();
            storage
                .create_stream("s", StreamConfig::new("text/plain".to_string()))
                .unwrap();
            let result = storage
                .append_with_producer(
                    "s",
                    vec![Bytes::from("x")],
                    "text/plain",
                    &producer("p1", 0, 0),
                    false,
                    None,
                )
                .unwrap();
            assert!(matches!(result, ProducerAppendResult::Accepted { .. }));
        }

        let restored = AcidStorage::new(root, 16, 1024 * 1024, 100 * 1024).unwrap();
        let dup = restored
            .append_with_producer(
                "s",
                vec![Bytes::from("x")],
                "text/plain",
                &producer("p1", 0, 0),
                false,
                None,
            )
            .unwrap();
        assert!(matches!(dup, ProducerAppendResult::Duplicate { .. }));
    }

    #[test]
    fn test_shard_routing_same_stream_is_stable() {
        let storage = test_storage();
        let a = storage.shard_index("same-stream");
        let b = storage.shard_index("same-stream");
        assert_eq!(a, b);
    }

    #[test]
    fn test_shard_distribution_uses_multiple_shards() {
        let storage = test_storage();
        let mut seen = std::collections::HashSet::new();
        for i in 0..256 {
            seen.insert(storage.shard_index(&format!("stream-{i}")));
        }
        assert!(seen.len() > 1);
    }

    #[test]
    fn test_startup_purges_expired_streams() {
        let root = test_storage_dir();
        {
            let storage = AcidStorage::new(root.clone(), 16, 1024 * 1024, 100 * 1024).unwrap();
            let expires = Utc::now() + Duration::milliseconds(50);
            let cfg = StreamConfig::new("text/plain".to_string()).with_expires_at(expires);
            storage.create_stream("expiring", cfg).unwrap();
            storage
                .append("expiring", Bytes::from("x"), "text/plain")
                .unwrap();
        }

        std::thread::sleep(std::time::Duration::from_millis(100));

        let restored = AcidStorage::new(root, 16, 1024 * 1024, 100 * 1024).unwrap();
        assert!(!restored.exists("expiring"));
        assert!(matches!(
            restored.read("expiring", &Offset::start()),
            Err(Error::NotFound(_)) | Err(Error::StreamExpired)
        ));
    }

    #[test]
    fn test_global_cap_strict_under_concurrency() {
        let storage = Arc::new(AcidStorage::new(test_storage_dir(), 16, 120, 120).unwrap());
        let shard_count = (0..8)
            .map(|i| storage.shard_index(&format!("s-{i}")))
            .collect::<std::collections::HashSet<_>>()
            .len();
        assert!(
            shard_count > 1,
            "test streams must span multiple shards to validate cross-shard cap behavior"
        );

        for i in 0..8 {
            storage
                .create_stream(
                    &format!("s-{i}"),
                    StreamConfig::new("text/plain".to_string()),
                )
                .unwrap();
        }

        let mut handles = Vec::new();
        for i in 0..8 {
            let storage = Arc::clone(&storage);
            handles.push(thread::spawn(move || {
                storage.append(&format!("s-{i}"), Bytes::from(vec![0_u8; 40]), "text/plain")
            }));
        }

        for h in handles {
            let _ = h.join().unwrap();
        }

        assert!(storage.total_bytes() <= 120);
    }

    #[test]
    fn test_layout_manifest_mismatch_fails_fast() {
        let root = test_storage_dir();
        let first = AcidStorage::new(root.clone(), 16, 1024 * 1024, 100 * 1024);
        assert!(first.is_ok());

        let mismatch = AcidStorage::new(root, 8, 1024 * 1024, 100 * 1024);
        assert!(matches!(mismatch, Err(Error::Storage(_))));
    }

    #[test]
    fn test_layout_manifest_invalid_json_fails_fast() {
        let root = test_storage_dir();
        let acid_dir = root.join("acid");
        fs::create_dir_all(&acid_dir).unwrap();
        fs::write(acid_dir.join("layout.json"), b"{invalid-json").unwrap();

        let reopened = AcidStorage::new(root, 16, 1024 * 1024, 100 * 1024);
        assert!(matches!(reopened, Err(Error::Storage(_))));
    }

    #[test]
    fn test_layout_manifest_hash_policy_mismatch_fails_fast() {
        let root = test_storage_dir();
        let storage = AcidStorage::new(root.clone(), 16, 1024 * 1024, 100 * 1024).unwrap();
        drop(storage);

        let layout_path = root.join("acid").join("layout.json");
        let mut layout: serde_json::Value =
            serde_json::from_slice(&fs::read(&layout_path).unwrap()).unwrap();
        layout["hash_policy"] = serde_json::Value::String("tampered-hash-policy".to_string());
        fs::write(layout_path, serde_json::to_vec_pretty(&layout).unwrap()).unwrap();

        let reopened = AcidStorage::new(root, 16, 1024 * 1024, 100 * 1024);
        assert!(matches!(reopened, Err(Error::Storage(_))));
    }

    #[test]
    fn test_corrupted_stream_metadata_fails_fast_on_startup() {
        let root = test_storage_dir();
        let storage = AcidStorage::new(root.clone(), 16, 1024 * 1024, 100 * 1024).unwrap();
        storage
            .create_stream("s", StreamConfig::new("text/plain".to_string()))
            .unwrap();
        storage
            .append("s", Bytes::from("payload"), "text/plain")
            .unwrap();

        // Simulate on-disk corruption of stream metadata.
        let shard_idx = storage.shard_index("s");
        let txn = AcidStorage::begin_write_txn(&storage.shards[shard_idx].db).unwrap();
        let mut streams = txn.open_table(STREAMS).unwrap();
        let corrupt = b"{not-json".to_vec();
        streams.insert("s", corrupt.as_slice()).unwrap();
        drop(streams);
        txn.commit().unwrap();
        drop(storage);

        let reopened = AcidStorage::new(root, 16, 1024 * 1024, 100 * 1024);
        assert!(matches!(reopened, Err(Error::Storage(_))));
    }

    #[test]
    fn test_tampered_shard_file_fails_fast_on_startup() {
        let root = test_storage_dir();
        let storage = AcidStorage::new(root.clone(), 16, 1024 * 1024, 100 * 1024).unwrap();
        storage
            .create_stream("s", StreamConfig::new("text/plain".to_string()))
            .unwrap();
        storage
            .append("s", Bytes::from("payload"), "text/plain")
            .unwrap();
        let shard_idx = storage.shard_index("s");
        drop(storage);

        let shard_path = root
            .join("acid")
            .join(format!("shard_{shard_idx:02x}.redb"));
        fs::write(&shard_path, b"not-a-valid-redb-file").unwrap();

        let reopened = AcidStorage::new(root, 16, 1024 * 1024, 100 * 1024);
        assert!(matches!(reopened, Err(Error::Storage(_))));
    }
}
