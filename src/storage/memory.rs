use super::{Message, ProducerAppendResult, ReadResult, Storage, StreamConfig, StreamMetadata};
use crate::protocol::error::{Error, Result};
use crate::protocol::offset::Offset;
use crate::protocol::producer::ProducerHeaders;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;

/// Per-producer state tracked within a stream
struct ProducerState {
    epoch: u64,
    last_seq: u64,
    updated_at: DateTime<Utc>,
}

/// Duration after which stale producer state is cleaned up (7 days)
const PRODUCER_STATE_TTL_SECS: i64 = 7 * 24 * 60 * 60;

/// Broadcast channel capacity for long-poll/SSE notifications.
/// Small because notifications are hints (no payload), not data delivery.
const NOTIFY_CHANNEL_CAPACITY: usize = 16;

/// Internal stream entry
struct StreamEntry {
    config: StreamConfig,
    messages: Vec<Message>,
    closed: bool,
    next_read_seq: u64,
    next_byte_offset: u64,
    total_bytes: u64,
    created_at: DateTime<Utc>,
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

    /// Clean up producer state entries older than the TTL threshold.
    /// Called lazily on producer append operations.
    fn cleanup_stale_producers(&mut self) {
        let cutoff = Utc::now()
            - chrono::TimeDelta::try_seconds(PRODUCER_STATE_TTL_SECS)
                .expect("7 days fits in TimeDelta");
        self.producers.retain(|_, state| state.updated_at > cutoff);
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
    total_bytes: RwLock<u64>,
    max_total_bytes: u64,
    max_stream_bytes: u64,
}

impl InMemoryStorage {
    /// Create a new in-memory storage with memory limits
    #[must_use]
    pub fn new(max_total_bytes: u64, max_stream_bytes: u64) -> Self {
        Self {
            streams: RwLock::new(HashMap::new()),
            total_bytes: RwLock::new(0),
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
        *self.total_bytes.read().expect("total_bytes lock poisoned")
    }

    fn get_stream(&self, name: &str) -> Option<Arc<RwLock<StreamEntry>>> {
        let streams = self.streams.read().expect("streams lock poisoned");
        streams.get(name).map(Arc::clone)
    }

    /// Check if a stream is expired based on its `expires_at` timestamp
    fn is_expired(entry: &StreamEntry) -> bool {
        if let Some(expires_at) = entry.config.expires_at {
            Utc::now() >= expires_at
        } else {
            false
        }
    }

    /// Validate and update Stream-Seq on a stream entry.
    ///
    /// Returns `Err(SeqOrderingViolation)` if the new seq is not strictly
    /// greater than the last seq (lexicographic comparison).
    fn validate_seq(stream: &mut StreamEntry, seq: Option<&str>) -> Result<()> {
        if let Some(new_seq) = seq {
            if let Some(ref last) = stream.last_seq
                && new_seq <= last.as_str()
            {
                return Err(Error::SeqOrderingViolation {
                    last: last.clone(),
                    received: new_seq.to_string(),
                });
            }
            stream.last_seq = Some(new_seq.to_string());
        }
        Ok(())
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

        let current_total = *self.total_bytes.read().expect("total_bytes lock poisoned");
        if current_total + total_batch_bytes > self.max_total_bytes {
            return Err(Error::MemoryLimitExceeded);
        }
        if stream.total_bytes + total_batch_bytes > self.max_stream_bytes {
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

        let mut total = self.total_bytes.write().expect("total_bytes lock poisoned");
        *total += total_batch_bytes;

        // Notify long-poll/SSE subscribers that new data is available.
        // Ignore errors (no active receivers is fine).
        let _ = stream.notify.send(());

        Ok(())
    }
}

impl Storage for InMemoryStorage {
    fn create_stream(&self, name: &str, config: StreamConfig) -> Result<()> {
        let mut streams = self.streams.write().expect("streams lock poisoned");

        if let Some(stream_arc) = streams.get(name) {
            // Stream exists, check if expired
            let stream = stream_arc.read().expect("stream lock poisoned");

            if Self::is_expired(&stream) {
                // Stream is expired, remove it and create new
                // Capture bytes to reclaim from global counter
                let stream_bytes = stream.total_bytes;
                drop(stream); // Release read lock before modifying map
                streams.remove(name);

                // Reclaim memory from global counter
                let mut total = self.total_bytes.write().expect("total_bytes lock poisoned");
                *total = total.saturating_sub(stream_bytes);
                drop(total); // Release lock before proceeding

            // Fall through to create new stream
            } else {
                // Stream is not expired, check for config match
                if stream.config == config {
                    // Idempotent create with matching config
                    return Ok(());
                }
                return Err(Error::ConfigMismatch);
            }
        }

        // Create new stream
        let entry = StreamEntry::new(config);
        streams.insert(name.to_string(), Arc::new(RwLock::new(entry)));

        Ok(())
    }

    fn append(&self, name: &str, data: Bytes, content_type: &str) -> Result<Offset> {
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        // Acquire per-stream write lock to serialize appends and ensure monotonicity
        // This lock is held across offset generation and message insertion
        let mut stream = stream_arc.write().expect("stream lock poisoned");

        // Check if stream is expired
        if Self::is_expired(&stream) {
            return Err(Error::StreamExpired);
        }

        // Check if stream is closed
        if stream.closed {
            return Err(Error::StreamClosed);
        }

        // Check content type match (normalized comparison)
        let normalized_ct = content_type.to_lowercase();
        let expected_ct = stream.config.content_type.to_lowercase();
        if normalized_ct != expected_ct {
            return Err(Error::ContentTypeMismatch {
                expected: stream.config.content_type.clone(),
                actual: content_type.to_string(),
            });
        }

        let byte_len = u64::try_from(data.len()).unwrap_or(u64::MAX);

        // Check memory limits
        let total_bytes = *self.total_bytes.read().expect("total_bytes lock poisoned");
        if total_bytes + byte_len > self.max_total_bytes {
            return Err(Error::MemoryLimitExceeded);
        }

        if stream.total_bytes + byte_len > self.max_stream_bytes {
            return Err(Error::StreamSizeLimitExceeded);
        }

        // Generate offset (monotonic)
        let offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);

        // Update counters
        stream.next_read_seq += 1;
        stream.next_byte_offset += byte_len;
        stream.total_bytes += byte_len;

        // Create and store message
        let message = Message::new(offset.clone(), data);
        stream.messages.push(message);

        // Update global memory counter
        let mut total = self.total_bytes.write().expect("total_bytes lock poisoned");
        *total += byte_len;

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

        // Acquire per-stream write lock for atomic batch operation
        let mut stream = stream_arc.write().expect("stream lock poisoned");

        // Check if stream is expired
        if Self::is_expired(&stream) {
            return Err(Error::StreamExpired);
        }

        // Check if stream is closed
        if stream.closed {
            return Err(Error::StreamClosed);
        }

        // Check content type match (normalized comparison)
        let normalized_ct = content_type.to_lowercase();
        let expected_ct = stream.config.content_type.to_lowercase();
        if normalized_ct != expected_ct {
            return Err(Error::ContentTypeMismatch {
                expected: stream.config.content_type.clone(),
                actual: content_type.to_string(),
            });
        }

        // Validate Stream-Seq ordering
        Self::validate_seq(&mut stream, seq)?;

        self.commit_messages(&mut stream, messages)?;

        // Return next_offset snapshot from within the lock
        Ok(Offset::new(stream.next_read_seq, stream.next_byte_offset))
    }

    fn read(&self, name: &str, from_offset: &Offset) -> Result<ReadResult> {
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let stream = stream_arc.read().expect("stream lock poisoned");

        // Check if stream is expired
        if Self::is_expired(&stream) {
            return Err(Error::StreamExpired);
        }

        // Handle sentinels
        if from_offset.is_now() {
            // Reading from "now" returns empty at tail
            let next_offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);
            return Ok(ReadResult {
                messages: Vec::new(),
                next_offset,
                at_tail: true,
                closed: stream.closed,
            });
        }

        // Find starting position
        let start_idx = if from_offset.is_start() {
            0
        } else {
            // Find first message >= from_offset
            // binary_search_by returns Ok(idx) if exact match, Err(idx) for insertion point
            // Both give us the correct starting position
            match stream
                .messages
                .binary_search_by(|m| m.offset.cmp(from_offset))
            {
                Ok(idx) | Err(idx) => idx,
            }
        };

        // Read all messages from start_idx to end
        let messages: Vec<Message> = stream.messages[start_idx..].to_vec();

        // Next offset is always what would be assigned to next append
        let next_offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);

        // We're at tail if we've read to the end of available messages
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
            // Update global memory counter
            let stream = stream_arc.read().expect("stream lock poisoned");
            let mut total = self.total_bytes.write().expect("total_bytes lock poisoned");
            *total = total.saturating_sub(stream.total_bytes);
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

        // Check if stream is expired
        if Self::is_expired(&stream) {
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

        // Check if stream is expired
        if Self::is_expired(&stream) {
            return Err(Error::StreamExpired);
        }

        stream.closed = true;

        // Notify long-poll subscribers so they wake up and see the closed state
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

        // Acquire per-stream write lock for atomic validation + append
        let mut stream = stream_arc.write().expect("stream lock poisoned");

        // Check expiration
        if Self::is_expired(&stream) {
            return Err(Error::StreamExpired);
        }

        // Lazy cleanup of stale producer state
        stream.cleanup_stale_producers();

        // Check content type match (only when there are messages to append)
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

        // --- Producer validation (atomic with append) ---
        //
        // Order matters:
        //   1. Epoch fencing — always checked first (403)
        //   2. Duplicate detection — before closed check so retries work (204)
        //   3. Closed check — blocks new sequences on closed streams (409)
        //   4. Gap / epoch-bump validation — only reached for non-duplicate, open streams
        //   5. Accept + append
        let now = Utc::now();

        if let Some(state) = stream.producers.get(&producer.id) {
            if producer.epoch < state.epoch {
                return Err(Error::EpochFenced {
                    current: state.epoch,
                    received: producer.epoch,
                });
            }

            if producer.epoch == state.epoch && producer.seq <= state.last_seq {
                // Duplicate — idempotent success regardless of closed state
                return Ok(ProducerAppendResult::Duplicate {
                    epoch: state.epoch,
                    seq: state.last_seq,
                    next_offset: Offset::new(stream.next_read_seq, stream.next_byte_offset),
                    closed: stream.closed,
                });
            }

            // Not a duplicate — if stream is closed, reject
            if stream.closed {
                return Err(Error::StreamClosed);
            }

            if producer.epoch > state.epoch {
                if producer.seq != 0 {
                    return Err(Error::InvalidProducerState(
                        "new epoch must start at seq 0".to_string(),
                    ));
                }
                // Epoch bump with seq=0 → accept, will reset state below
            } else if producer.seq > state.last_seq + 1 {
                return Err(Error::SequenceGap {
                    expected: state.last_seq + 1,
                    actual: producer.seq,
                });
            }
            // seq == state.last_seq + 1 → accept, fall through
        } else {
            // New producer — if stream is closed, reject
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

        // Validate Stream-Seq ordering
        Self::validate_seq(&mut stream, seq)?;

        self.commit_messages(&mut stream, messages)?;

        // Close stream if requested (atomic with append)
        if should_close {
            stream.closed = true;
        }

        // Update producer state
        stream.producers.insert(
            producer.id.clone(),
            ProducerState {
                epoch: producer.epoch,
                last_seq: producer.seq,
                updated_at: now,
            },
        );

        // Snapshot stream state while still holding the lock
        let next_offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);
        let closed = stream.closed;

        Ok(ProducerAppendResult::Accepted {
            epoch: producer.epoch,
            seq: producer.seq,
            next_offset,
            closed,
        })
    }

    fn exists(&self, name: &str) -> bool {
        let streams = self.streams.read().expect("streams lock poisoned");
        if let Some(stream_arc) = streams.get(name) {
            let stream = stream_arc.read().expect("stream lock poisoned");
            // Expired streams are treated as non-existent
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

    fn test_storage() -> InMemoryStorage {
        InMemoryStorage::new(1024 * 1024, 100 * 1024)
    }

    #[test]
    fn test_create_stream() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());

        // Create stream
        storage.create_stream("test", config.clone()).unwrap();
        assert!(storage.exists("test"));

        // Idempotent create with same config
        storage.create_stream("test", config).unwrap();

        // Create with different config should fail
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

        // Append some data
        let data1 = Bytes::from("hello");
        let offset1 = storage.append("test", data1.clone(), "text/plain").unwrap();

        let data2 = Bytes::from("world");
        let offset2 = storage.append("test", data2.clone(), "text/plain").unwrap();

        // Offsets should be monotonically increasing
        assert!(offset1 < offset2);

        // Read from start
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

        // Verify all offsets are strictly increasing
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

        // Spawn multiple threads appending concurrently
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

        // Collect all offsets from all threads
        let mut all_offsets = Vec::new();
        for handle in handles {
            let offsets = handle.join().unwrap();
            all_offsets.extend(offsets);
        }

        // Verify all offsets are unique and monotonic when sorted
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

        // Verify total message count
        let metadata = storage.head("test").unwrap();
        assert_eq!(metadata.message_count, 100); // 5 threads * 20 messages
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

        // Read from offset2 should get msg2 and msg3
        let result = storage.read("test", &offset2).unwrap();
        assert_eq!(result.messages.len(), 2);
        assert_eq!(result.messages[0].data, Bytes::from("msg2"));
        assert_eq!(result.messages[1].data, Bytes::from("msg3"));

        // Read from offset1 should get all three
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

        // Read from "now" should return empty at tail
        let result = storage.read("test", &Offset::now()).unwrap();
        assert_eq!(result.messages.len(), 0);
        assert!(result.at_tail);

        // Read from start should get all messages
        let result = storage.read("test", &Offset::start()).unwrap();
        assert_eq!(result.messages.len(), 1);
    }

    #[test]
    fn test_stream_closed() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();

        // Close stream
        storage.close_stream("test").unwrap();

        // Append should fail
        assert!(matches!(
            storage.append("test", Bytes::from("data"), "text/plain"),
            Err(Error::StreamClosed)
        ));

        // Reads should still work
        let result = storage.read("test", &Offset::start()).unwrap();
        assert!(result.closed);
    }

    #[test]
    fn test_content_type_mismatch() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();

        // Append with wrong content type
        assert!(matches!(
            storage.append("test", Bytes::from("data"), "application/json"),
            Err(Error::ContentTypeMismatch { .. })
        ));

        // Case-insensitive comparison
        storage
            .append("test", Bytes::from("data"), "TEXT/PLAIN")
            .unwrap();
    }

    #[test]
    fn test_memory_limits() {
        let storage = InMemoryStorage::new(100, 50); // Small limits
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test1", config.clone()).unwrap();
        storage.create_stream("test2", config).unwrap();

        // Fill up test1 to stream limit
        storage
            .append("test1", Bytes::from(vec![0u8; 50]), "text/plain")
            .unwrap();

        // Should hit stream limit
        assert!(matches!(
            storage.append("test1", Bytes::from(vec![0u8; 10]), "text/plain"),
            Err(Error::StreamSizeLimitExceeded)
        ));

        // test2 can still append
        storage
            .append("test2", Bytes::from(vec![0u8; 40]), "text/plain")
            .unwrap();

        // Should hit global memory limit
        assert!(matches!(
            storage.append("test2", Bytes::from(vec![0u8; 20]), "text/plain"),
            Err(Error::MemoryLimitExceeded)
        ));
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

        // Delete stream
        storage.delete("test").unwrap();
        assert!(!storage.exists("test"));

        let bytes_after = storage.total_bytes();
        assert_eq!(bytes_after, 0);

        // Delete non-existent returns NotFound
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

        // Verify all 3 messages stored
        let metadata = storage.head("test").unwrap();
        assert_eq!(metadata.message_count, 3);
    }

    #[test]
    fn test_producer_duplicate_detection() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();

        // First append
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

        // Duplicate
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

        // Only 1 message stored (not 2)
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

        // Skip seq 1, send seq 2
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

        // Establish epoch 1
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

        // Try with old epoch 0
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

        // Epoch 0, seq 0..2
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

        // Bump to epoch 1 with seq 0
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

        // Producer A seq 0
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

        // Producer B seq 0 (independent)
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

        // Producer A seq 1
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

        // Append + close atomically
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

        // Send gap seq to closed stream — must get StreamClosed, not SequenceGap
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
