mod common;

use bytes::Bytes;
use common::{StorageTestBackend, create_test_storage, create_test_storage_with_limits};
use durable_streams_reference::protocol::error::Error;
use durable_streams_reference::protocol::offset::Offset;
use durable_streams_reference::protocol::producer::ProducerHeaders;
use durable_streams_reference::storage::{
    CreateStreamResult, ProducerAppendResult, Storage, StreamConfig,
};
use std::panic::{AssertUnwindSafe, RefUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::thread;

const BACKENDS: [StorageTestBackend; 3] = [
    StorageTestBackend::Memory,
    StorageTestBackend::FileDurable,
    StorageTestBackend::Acid,
];

fn producer(id: &str, epoch: u64, seq: u64) -> ProducerHeaders {
    ProducerHeaders {
        id: id.to_string(),
        epoch,
        seq,
    }
}

fn plain_text_config() -> StreamConfig {
    StreamConfig::new("text/plain".to_string())
}

fn with_each_backend(test: impl Fn(StorageTestBackend) + RefUnwindSafe) {
    for backend in BACKENDS {
        let result = catch_unwind(AssertUnwindSafe(|| test(backend)));
        if let Err(payload) = result {
            let panic_msg = if let Some(msg) = payload.downcast_ref::<&str>() {
                (*msg).to_string()
            } else if let Some(msg) = payload.downcast_ref::<String>() {
                msg.clone()
            } else {
                "non-string panic payload".to_string()
            };
            panic!(
                "backend contract failed for backend={}: {}",
                backend.as_str(),
                panic_msg
            );
        }
    }
}

mod core {
    use super::*;

    #[test]
    fn create_idempotent_and_config_mismatch() {
        with_each_backend(|backend| {
            let handle = create_test_storage(backend);
            let storage = &handle.storage;
            let cfg = plain_text_config();

            let created = storage.create_stream("s", cfg.clone()).unwrap();
            assert_eq!(
                created,
                CreateStreamResult::Created,
                "backend={} failed create",
                backend.as_str()
            );

            let idempotent = storage.create_stream("s", cfg).unwrap();
            assert_eq!(
                idempotent,
                CreateStreamResult::AlreadyExists,
                "backend={} failed idempotent create",
                backend.as_str()
            );

            assert!(matches!(
                storage.create_stream("s", StreamConfig::new("application/json".to_string())),
                Err(Error::ConfigMismatch)
            ));
        });
    }

    #[test]
    fn append_read_and_offset_monotonicity() {
        with_each_backend(|backend| {
            let handle = create_test_storage(backend);
            let storage = &handle.storage;
            storage.create_stream("s", plain_text_config()).unwrap();

            let o1 = storage.append("s", Bytes::from("a"), "text/plain").unwrap();
            let o2 = storage.append("s", Bytes::from("b"), "text/plain").unwrap();
            assert!(o1 < o2, "backend={} offset monotonicity", backend.as_str());

            let read = storage.read("s", &Offset::start()).unwrap();
            assert_eq!(read.messages, vec![Bytes::from("a"), Bytes::from("b")]);
            assert!(read.at_tail);
        });
    }

    #[test]
    fn read_from_offset_and_sentinels() {
        with_each_backend(|backend| {
            let handle = create_test_storage(backend);
            let storage = &handle.storage;
            storage.create_stream("s", plain_text_config()).unwrap();

            let o1 = storage
                .append("s", Bytes::from("m1"), "text/plain")
                .unwrap();
            let o2 = storage
                .append("s", Bytes::from("m2"), "text/plain")
                .unwrap();
            let _ = storage
                .append("s", Bytes::from("m3"), "text/plain")
                .unwrap();

            let from_o2 = storage.read("s", &o2).unwrap();
            assert_eq!(from_o2.messages, vec![Bytes::from("m2"), Bytes::from("m3")]);

            let from_o1 = storage.read("s", &o1).unwrap();
            assert_eq!(from_o1.messages.len(), 3);

            let from_now = storage.read("s", &Offset::now()).unwrap();
            assert!(from_now.messages.is_empty());
            assert!(from_now.at_tail);
        });
    }

    #[test]
    fn close_and_content_type_rules() {
        with_each_backend(|backend| {
            let handle = create_test_storage(backend);
            let storage = &handle.storage;
            storage.create_stream("s", plain_text_config()).unwrap();

            assert!(matches!(
                storage.append("s", Bytes::from("x"), "application/json"),
                Err(Error::ContentTypeMismatch { .. })
            ));

            storage
                .append("s", Bytes::from("ok"), "TEXT/PLAIN")
                .unwrap();
            storage.close_stream("s").unwrap();

            assert!(matches!(
                storage.append("s", Bytes::from("x"), "text/plain"),
                Err(Error::StreamClosed)
            ));

            let read = storage.read("s", &Offset::start()).unwrap();
            assert!(read.closed);
        });
    }

    #[test]
    fn delete_and_exists() {
        with_each_backend(|backend| {
            let handle = create_test_storage(backend);
            let storage = &handle.storage;
            storage.create_stream("s", plain_text_config()).unwrap();
            storage.append("s", Bytes::from("x"), "text/plain").unwrap();

            assert!(storage.exists("s"));
            storage.delete("s").unwrap();
            assert!(!storage.exists("s"));
            assert_eq!(
                storage.total_bytes(),
                0,
                "backend={} leaked bytes after delete",
                backend.as_str()
            );
            assert!(matches!(storage.delete("s"), Err(Error::NotFound(_))));
        });
    }
}

mod limits_atomicity {
    use super::*;

    #[test]
    fn limits_and_not_found() {
        with_each_backend(|backend| {
            let handle = create_test_storage_with_limits(backend, 100, 50);
            let storage = &handle.storage;
            let cfg = plain_text_config();
            storage.create_stream("a", cfg.clone()).unwrap();
            storage.create_stream("b", cfg).unwrap();

            storage
                .append("a", Bytes::from(vec![0_u8; 50]), "text/plain")
                .unwrap();
            assert!(matches!(
                storage.append("a", Bytes::from(vec![0_u8; 10]), "text/plain"),
                Err(Error::StreamSizeLimitExceeded)
            ));

            storage
                .append("b", Bytes::from(vec![0_u8; 40]), "text/plain")
                .unwrap();
            assert!(matches!(
                storage.append("b", Bytes::from(vec![0_u8; 20]), "text/plain"),
                Err(Error::MemoryLimitExceeded)
            ));

            assert!(matches!(
                storage.append("missing", Bytes::from("x"), "text/plain"),
                Err(Error::NotFound(_))
            ));
            assert!(matches!(
                storage.read("missing", &Offset::start()),
                Err(Error::NotFound(_))
            ));
            assert!(matches!(storage.head("missing"), Err(Error::NotFound(_))));
            assert!(matches!(
                storage.close_stream("missing"),
                Err(Error::NotFound(_))
            ));
        });
    }

    #[test]
    fn create_with_data_atomicity_and_idempotency() {
        with_each_backend(|backend| {
            let small = create_test_storage_with_limits(backend, 1024, 8);
            let cfg = plain_text_config();
            let oversized = vec![Bytes::from(vec![0_u8; 9])];
            let err = small
                .storage
                .create_stream_with_data("s", cfg.clone(), oversized, false);
            assert!(matches!(err, Err(Error::StreamSizeLimitExceeded)));
            assert!(!small.storage.exists("s"));

            let handle = create_test_storage(backend);
            let storage = &handle.storage;

            let closed_cfg = plain_text_config().with_created_closed(true);
            let created = storage
                .create_stream_with_data("closed", closed_cfg, vec![], true)
                .unwrap();
            assert_eq!(created.status, CreateStreamResult::Created);
            assert!(created.closed);

            let r1 = storage
                .create_stream_with_data("idempotent", cfg.clone(), vec![Bytes::from("a")], false)
                .unwrap();
            assert_eq!(r1.status, CreateStreamResult::Created);

            let r2 = storage
                .create_stream_with_data("idempotent", cfg, vec![Bytes::from("b")], false)
                .unwrap();
            assert_eq!(r2.status, CreateStreamResult::AlreadyExists);

            let meta = storage.head("idempotent").unwrap();
            assert_eq!(meta.message_count, 1);
        });
    }

    #[test]
    fn stream_seq_rollback_after_failed_commit() {
        with_each_backend(|backend| {
            let small = create_test_storage_with_limits(backend, 1024, 8);
            small
                .storage
                .create_stream("s", plain_text_config())
                .unwrap();

            let oversized = vec![Bytes::from(vec![0_u8; 9])];
            let err = small
                .storage
                .batch_append("s", oversized.clone(), "text/plain", Some("s1"));
            assert!(matches!(err, Err(Error::StreamSizeLimitExceeded)));

            let retry =
                small
                    .storage
                    .batch_append("s", vec![Bytes::from("ok")], "text/plain", Some("s1"));
            assert!(retry.is_ok());

            let err = small.storage.append_with_producer(
                "s",
                oversized,
                "text/plain",
                &producer("p1", 0, 0),
                false,
                Some("s2"),
            );
            assert!(matches!(err, Err(Error::StreamSizeLimitExceeded)));

            let retry = small.storage.append_with_producer(
                "s",
                vec![Bytes::from("ok")],
                "text/plain",
                &producer("p1", 0, 0),
                false,
                Some("s2"),
            );
            assert!(matches!(retry, Ok(ProducerAppendResult::Accepted { .. })));
        });
    }
}

mod producer {
    use super::*;

    #[test]
    fn duplicate_gap_fencing_and_epoch_reset_rules() {
        with_each_backend(|backend| {
            let handle = create_test_storage(backend);
            let storage = &handle.storage;
            storage.create_stream("s", plain_text_config()).unwrap();

            let accepted = storage
                .append_with_producer(
                    "s",
                    vec![Bytes::from("m0")],
                    "text/plain",
                    &producer("p1", 0, 0),
                    false,
                    None,
                )
                .unwrap();
            assert!(matches!(accepted, ProducerAppendResult::Accepted { .. }));

            let duplicate = storage
                .append_with_producer(
                    "s",
                    vec![Bytes::from("m0")],
                    "text/plain",
                    &producer("p1", 0, 0),
                    false,
                    None,
                )
                .unwrap();
            assert!(matches!(duplicate, ProducerAppendResult::Duplicate { .. }));

            let gap = storage
                .append_with_producer(
                    "s",
                    vec![Bytes::from("m3")],
                    "text/plain",
                    &producer("p1", 0, 3),
                    false,
                    None,
                )
                .unwrap_err();
            assert!(matches!(
                gap,
                Error::SequenceGap {
                    expected: 1,
                    actual: 3
                }
            ));

            let new_epoch = storage
                .append_with_producer(
                    "s",
                    vec![Bytes::from("m")],
                    "text/plain",
                    &producer("p1", 2, 0),
                    false,
                    None,
                )
                .unwrap();
            assert!(matches!(new_epoch, ProducerAppendResult::Accepted { .. }));

            let stale_epoch = storage
                .append_with_producer(
                    "s",
                    vec![Bytes::from("zombie")],
                    "text/plain",
                    &producer("p1", 1, 0),
                    false,
                    None,
                )
                .unwrap_err();
            assert!(matches!(stale_epoch, Error::EpochFenced { .. }));

            let nonzero_after_bump = storage
                .append_with_producer(
                    "s",
                    vec![Bytes::from("bad")],
                    "text/plain",
                    &producer("p1", 3, 2),
                    false,
                    None,
                )
                .unwrap_err();
            assert!(matches!(nonzero_after_bump, Error::InvalidProducerState(_)));
        });
    }

    #[test]
    fn multi_producer_independence_and_closed_precedence() {
        with_each_backend(|backend| {
            let handle = create_test_storage(backend);
            let storage = &handle.storage;
            storage.create_stream("s", plain_text_config()).unwrap();

            for producer_id in ["p1", "p2"] {
                let result = storage
                    .append_with_producer(
                        "s",
                        vec![Bytes::from(format!("{producer_id}-0"))],
                        "text/plain",
                        &producer(producer_id, 0, 0),
                        false,
                        None,
                    )
                    .unwrap();
                assert!(matches!(result, ProducerAppendResult::Accepted { .. }));
            }

            let closed = storage
                .append_with_producer(
                    "s",
                    vec![Bytes::from("close")],
                    "text/plain",
                    &producer("p1", 0, 1),
                    true,
                    None,
                )
                .unwrap();
            assert!(matches!(
                closed,
                ProducerAppendResult::Accepted { closed: true, .. }
            ));

            let closed_precedence = storage
                .append_with_producer(
                    "s",
                    vec![Bytes::from("later")],
                    "text/plain",
                    &producer("p1", 0, 10),
                    false,
                    None,
                )
                .unwrap_err();
            assert!(matches!(closed_precedence, Error::StreamClosed));
        });
    }
}

mod concurrency {
    use super::*;

    #[test]
    fn concurrent_append_offsets_unique() {
        with_each_backend(|backend| {
            let handle = create_test_storage(backend);
            let storage = Arc::new(handle.storage);
            storage.create_stream("s", plain_text_config()).unwrap();

            let mut handles = Vec::new();
            for worker in 0..4 {
                let storage = Arc::clone(&storage);
                handles.push(thread::spawn(move || {
                    let mut offsets = Vec::new();
                    for i in 0..20 {
                        let offset = storage
                            .append(
                                "s",
                                Bytes::from(format!("worker-{worker}-{i}")),
                                "text/plain",
                            )
                            .unwrap();
                        offsets.push(offset);
                    }
                    offsets
                }));
            }

            let mut all = Vec::new();
            for handle in handles {
                all.extend(handle.join().unwrap());
            }

            all.sort();
            for idx in 1..all.len() {
                assert_ne!(all[idx - 1], all[idx]);
                assert!(all[idx - 1] < all[idx]);
            }

            let meta = storage.head("s").unwrap();
            assert_eq!(
                meta.message_count,
                80,
                "backend={} failed",
                backend.as_str()
            );
        });
    }
}
