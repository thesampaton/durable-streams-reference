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
            messages: Vec::new(),
            closed: false,
            next_read_seq: 0,
            next_byte_offset: 0,
            total_bytes: 0,
            created_at: Utc::now(),
            producers: HashMap::new(),
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
            self.total_bytes
                .fetch_sub(total_batch_bytes, Ordering::AcqRel);
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
            self.total_bytes.fetch_sub(byte_len, Ordering::AcqRel);
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

        let messages: Vec<Message> = stream.messages[start_idx..].to_vec();

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
            self.total_bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    Some(current.saturating_sub(stream.total_bytes))
                })
                .ok();
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
                self.total_bytes
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                        Some(current.saturating_sub(stream_bytes))
                    })
                    .ok();
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

    fn test_storage() -> InMemoryStorage {
        InMemoryStorage::new(1024 * 1024, 100 * 1024)
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
            assert!(
                all_offsets[i - 1] < all_offsets[i],
                "Offset ordering violated"
            );
        }

        let metadata = storage.head("test").unwrap();
        assert_eq!(metadata.message_count, 100);
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

        storage
            .append("test", Bytes::from("data"), "TEXT/PLAIN")
            .unwrap();
    }

    #[test]
    fn test_memory_limits() {
        let storage = InMemoryStorage::new(100, 50);
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
        let storage = InMemoryStorage::new(1024, 8);
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
        let storage = InMemoryStorage::new(1024, 8);
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
        let storage = InMemoryStorage::new(1024, 8);
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

    fn producer(id: &str, epoch: u64, seq: u64) -> ProducerHeaders {
        ProducerHeaders {
            id: id.to_string(),
            epoch,
            seq,
        }
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
    fn test_concurrent_producer_appends() {
        use std::thread;

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
