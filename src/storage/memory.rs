use super::{
    CreateStreamResult, Message, NOTIFY_CHANNEL_CAPACITY, ProducerAppendResult, ProducerCheck,
    ProducerState, ReadResult, Storage, StreamConfig, StreamMetadata,
};
use crate::protocol::error::{Error, Result};
use crate::protocol::offset::Offset;
use crate::protocol::producer::ProducerHeaders;
use bytes::Bytes;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;

const INITIAL_MESSAGES_CAPACITY: usize = 256;
const INITIAL_PRODUCERS_CAPACITY: usize = 8;

/// Internal stream entry
struct StreamEntry {
    config: StreamConfig,
    messages: Vec<Message>,
    closed: bool,
    next_read_seq: u64,
    next_byte_offset: u64,
    total_bytes: u64,
    created_at: chrono::DateTime<Utc>,
    /// Per-producer state for idempotent producer support
    producers: HashMap<String, ProducerState>,
    /// Broadcast sender for notifying long-poll/SSE subscribers
    notify: broadcast::Sender<()>,
    /// Last Stream-Seq value received (lexicographic ordering)
    last_seq: Option<String>,
}

impl StreamEntry {
    fn new(config: StreamConfig) -> Self {
        // Stream starts open; the handler closes it after any initial appends.
        // The `created_closed` flag in config is stored for idempotent checks only.
        let (notify, _) = broadcast::channel(NOTIFY_CHANNEL_CAPACITY);
        Self {
            config,
            messages: Vec::with_capacity(INITIAL_MESSAGES_CAPACITY),
            closed: false,
            next_read_seq: 0,
            next_byte_offset: 0,
            total_bytes: 0,
            created_at: Utc::now(),
            producers: HashMap::with_capacity(INITIAL_PRODUCERS_CAPACITY),
            notify,
            last_seq: None,
        }
    }
}

/// In-memory storage implementation
///
/// Thread-safe storage with:
/// - `RwLock<HashMap>` for stream lookup (concurrent reads)
/// - Per-stream `RwLock` for exclusive write access (offset monotonicity)
/// - Memory limit enforcement (global and per-stream)
///
/// # Concurrency Model
///
/// Multiple readers can access different streams concurrently.
/// Appends to the same stream are serialized via `RwLock::write()`.
/// Appends to different streams can proceed concurrently.
pub struct InMemoryStorage {
    streams: RwLock<HashMap<String, Arc<RwLock<StreamEntry>>>>,
    total_bytes: AtomicU64,
    max_total_bytes: u64,
    max_stream_bytes: u64,
}

impl InMemoryStorage {
    /// Create a new in-memory storage with memory limits
    #[must_use]
    pub fn new(max_total_bytes: u64, max_stream_bytes: u64) -> Self {
        Self {
            streams: RwLock::new(HashMap::new()),
            total_bytes: AtomicU64::new(0),
            max_total_bytes,
            max_stream_bytes,
        }
    }

    /// Get current total memory usage
    ///
    /// # Panics
    ///
    /// Panics if the `total_bytes` lock is poisoned (which indicates a panic while holding the lock).
    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Acquire)
    }

    fn saturating_sub_total_bytes(&self, bytes: u64) {
        self.total_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(bytes))
            })
            .ok();
    }

    fn get_stream(&self, name: &str) -> Option<Arc<RwLock<StreamEntry>>> {
        let streams = self.streams.read().expect("streams lock poisoned");
        streams.get(name).map(Arc::clone)
    }

    /// Commit messages to a stream, checking memory limits first.
    ///
    /// Caller must hold the stream write lock. Updates both stream-level
    /// and global memory counters atomically.
    fn commit_messages(&self, stream: &mut StreamEntry, messages: Vec<Bytes>) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let mut total_batch_bytes = 0u64;
        let mut message_sizes = Vec::with_capacity(messages.len());
        for data in &messages {
            let byte_len = u64::try_from(data.len()).unwrap_or(u64::MAX);
            message_sizes.push(byte_len);
            total_batch_bytes += byte_len;
        }

        // Reserve global bytes atomically (global precedence before per-stream).
        if self
            .total_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_add(total_batch_bytes)
                    .filter(|next| *next <= self.max_total_bytes)
            })
            .is_err()
        {
            return Err(Error::MemoryLimitExceeded);
        }
        if stream.total_bytes + total_batch_bytes > self.max_stream_bytes {
            self.saturating_sub_total_bytes(total_batch_bytes);
            return Err(Error::StreamSizeLimitExceeded);
        }

        for (data, byte_len) in messages.into_iter().zip(message_sizes) {
            let offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);
            stream.next_read_seq += 1;
            stream.next_byte_offset += byte_len;
            stream.total_bytes += byte_len;
            let message = Message::new(offset, data);
            stream.messages.push(message);
        }

        // Notify long-poll/SSE subscribers that new data is available.
        // Ignore errors (no active receivers is fine).
        let _ = stream.notify.send(());

        Ok(())
    }
}

impl Storage for InMemoryStorage {
    fn create_stream(&self, name: &str, config: StreamConfig) -> Result<CreateStreamResult> {
        let mut streams = self.streams.write().expect("streams lock poisoned");

        if let Some(stream_arc) = streams.get(name) {
            let stream = stream_arc.read().expect("stream lock poisoned");

            if super::is_stream_expired(&stream.config) {
                let stream_bytes = stream.total_bytes;
                drop(stream);
                streams.remove(name);

                self.total_bytes
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                        Some(current.saturating_sub(stream_bytes))
                    })
                    .ok();
            } else {
                if stream.config == config {
                    return Ok(CreateStreamResult::AlreadyExists);
                }
                return Err(Error::ConfigMismatch);
            }
        }

        let entry = StreamEntry::new(config);
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

        let byte_len = u64::try_from(data.len()).unwrap_or(u64::MAX);

        if self
            .total_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_add(byte_len)
                    .filter(|next| *next <= self.max_total_bytes)
            })
            .is_err()
        {
            return Err(Error::MemoryLimitExceeded);
        }

        if stream.total_bytes + byte_len > self.max_stream_bytes {
            self.saturating_sub_total_bytes(byte_len);
            return Err(Error::StreamSizeLimitExceeded);
        }

        let offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);

        stream.next_read_seq += 1;
        stream.next_byte_offset += byte_len;
        stream.total_bytes += byte_len;

        let message = Message::new(offset.clone(), data);
        stream.messages.push(message);

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

        self.commit_messages(&mut stream, messages)?;
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

        if super::is_stream_expired(&stream.config) {
            return Err(Error::StreamExpired);
        }

        if from_offset.is_now() {
            let next_offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);
            return Ok(ReadResult {
                messages: Vec::new(),
                next_offset,
                at_tail: true,
                closed: stream.closed,
            });
        }

        let start_idx = if from_offset.is_start() {
            0
        } else {
            match stream
                .messages
                .binary_search_by(|m| m.offset.cmp(from_offset))
            {
                Ok(idx) | Err(idx) => idx,
            }
        };

        let messages: Vec<Bytes> = stream.messages[start_idx..]
            .iter()
            .map(|m| m.data.clone())
            .collect();

        let next_offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);

        let at_tail = start_idx + messages.len() >= stream.messages.len();

        Ok(ReadResult {
            messages,
            next_offset,
            at_tail,
            closed: stream.closed,
        })
    }

    fn delete(&self, name: &str) -> Result<()> {
        let mut streams = self.streams.write().expect("streams lock poisoned");

        if let Some(stream_arc) = streams.remove(name) {
            let stream = stream_arc.read().expect("stream lock poisoned");
            self.saturating_sub_total_bytes(stream.total_bytes);
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
            message_count: u64::try_from(stream.messages.len()).unwrap_or(u64::MAX),
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

        match super::check_producer(stream.producers.get(&producer.id), producer, stream.closed)? {
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

        self.commit_messages(&mut stream, messages)?;
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

        let next_offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);
        let closed = stream.closed;

        Ok(ProducerAppendResult::Accepted {
            epoch: producer.epoch,
            seq: producer.seq,
            next_offset,
            closed,
        })
    }

    fn create_stream_with_data(
        &self,
        name: &str,
        config: StreamConfig,
        messages: Vec<Bytes>,
        should_close: bool,
    ) -> Result<super::CreateWithDataResult> {
        let mut streams = self.streams.write().expect("streams lock poisoned");

        if let Some(stream_arc) = streams.get(name) {
            let stream = stream_arc.read().expect("stream lock poisoned");

            if super::is_stream_expired(&stream.config) {
                let stream_bytes = stream.total_bytes;
                drop(stream);
                streams.remove(name);
                self.saturating_sub_total_bytes(stream_bytes);
            } else if stream.config == config {
                let next_offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);
                let closed = stream.closed;
                return Ok(super::CreateWithDataResult {
                    status: CreateStreamResult::AlreadyExists,
                    next_offset,
                    closed,
                });
            } else {
                return Err(Error::ConfigMismatch);
            }
        }

        let mut entry = StreamEntry::new(config);

        if !messages.is_empty() {
            self.commit_messages(&mut entry, messages)?;
        }

        if should_close {
            entry.closed = true;
        }

        let next_offset = Offset::new(entry.next_read_seq, entry.next_byte_offset);
        let closed = entry.closed;

        streams.insert(name.to_string(), Arc::new(RwLock::new(entry)));

        Ok(super::CreateWithDataResult {
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
    use std::sync::Arc;
    use std::thread;

    fn test_storage() -> InMemoryStorage {
        InMemoryStorage::new(1024 * 1024, 100 * 1024)
    }

    fn producer(id: &str, epoch: u64, seq: u64) -> ProducerHeaders {
        ProducerHeaders {
            id: id.to_string(),
            epoch,
            seq,
        }
    }

    #[test]
    fn test_concurrent_producer_appends() {
        let storage = Arc::new(test_storage());
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();

        let num_producers = 4;
        let seqs_per_producer = 50;

        let handles: Vec<_> = (0..num_producers)
            .map(|p| {
                let storage = Arc::clone(&storage);
                thread::spawn(move || {
                    let prod_id = format!("p{p}");
                    for seq in 0..seqs_per_producer {
                        let result = storage.append_with_producer(
                            "test",
                            vec![Bytes::from(format!("{prod_id}-{seq}"))],
                            "text/plain",
                            &producer(&prod_id, 0, seq),
                            false,
                            None,
                        );
                        assert!(
                            result.is_ok(),
                            "Producer {prod_id} seq {seq} failed: {result:?}"
                        );
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("thread panicked");
        }

        let metadata = storage.head("test").unwrap();
        assert_eq!(
            metadata.message_count,
            u64::try_from(num_producers * seqs_per_producer).unwrap()
        );
    }
}
