use super::{Message, ReadResult, Storage, StreamConfig, StreamMetadata};
use crate::protocol::error::{Error, Result};
use crate::protocol::offset::Offset;
use bytes::Bytes;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Internal stream entry
struct StreamEntry {
    config: StreamConfig,
    messages: Vec<Message>,
    closed: bool,
    next_read_seq: u64,
    next_byte_offset: u64,
    total_bytes: u64,
    created_at: chrono::DateTime<chrono::Utc>,
}

impl StreamEntry {
    fn new(config: StreamConfig) -> Self {
        // Initialize closed flag from config
        let closed = config.created_closed;
        Self {
            config,
            messages: Vec::new(),
            closed,
            next_read_seq: 0,
            next_byte_offset: 0,
            total_bytes: 0,
            created_at: Utc::now(),
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
        }

        // Idempotent - Ok even if stream didn't exist
        Ok(())
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

        Ok(())
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

        // Idempotent delete
        storage.delete("test").unwrap();
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
}
