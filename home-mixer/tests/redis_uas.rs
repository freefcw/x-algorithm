//! Redis UAS projection adapter against a real `redis-server`.
//!
//! Covers the contracts the documentation makes: newest-N retention and
//! reads, idempotent redelivery, window and clock-skew handling, TTL, and
//! tolerance to members the reader cannot decode.

#![cfg(unix)]

mod common;

use common::RedisFixture;
use home_mixer::clients::uas_fetcher::{
    RecordOutcome, RedisUserActionSequenceStore, SkipReason, UserActionEvent, UserActionEventSink,
    UserActionSequenceOps, ValidatedUserAction,
};
use home_mixer::models::query::ScoredPostsQuery;
use home_mixer::models::{pid, uid, PostId, UserId};
use home_mixer::query_hydrators::user_action_seq_query_hydrator::UserActionSeqQueryHydrator;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DAY_MS: i64 = 24 * 60 * 60 * 1_000;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("current time")
        .as_millis()
        .try_into()
        .expect("millisecond timestamp fits i64")
}

fn action(user: UserId, post: u64, action_time_ms: i64, action_type: i32) -> ValidatedUserAction {
    UserActionEvent {
        user_id: user.to_string(),
        tweet_id: pid(post).to_string(),
        author_id: uid(post + 100).to_string(),
        action_time_ms,
        action_type,
    }
    .validate()
    .expect("fixture event is valid")
}

async fn read_post_ids(store: &RedisUserActionSequenceStore, user: UserId) -> Vec<PostId> {
    let sequence = store
        .get_by_user_id(user)
        .await
        .expect("read projected sequence");
    let actions = sequence.user_actions.expect("action list");
    assert!(
        actions.windows(2).all(|pair| {
            pair[0].action_time_ms.expect("time") <= pair[1].action_time_ms.expect("time")
        }),
        "actions must be returned in time order"
    );
    actions
        .iter()
        .map(|action| action.tweet_id.expect("tweet id"))
        .collect()
}

fn zcard(redis: &RedisFixture, prefix: &str, user: UserId) -> i64 {
    redis::cmd("ZCARD")
        .arg(redis.key(prefix, user, "actions"))
        .query(&mut redis.connection())
        .expect("ZCARD")
}

#[tokio::test]
#[ignore = "requires redis-server"]
async fn projection_keeps_only_the_newest_bounded_actions_in_time_order() {
    let redis = RedisFixture::start();
    let store = RedisUserActionSequenceStore::new(redis.uas_config("test:uas", 3))
        .await
        .expect("create UAS adapter");
    let now = now_ms();
    let user = uid(7);

    // Deliberately arrive out of order. The ZSET must keep the three newest
    // actions by event time rather than the last three consumed messages.
    for (post, age_ms) in [(4, 1_000), (1, 4_000), (3, 2_000), (2, 3_000)] {
        assert_eq!(
            store.record(&action(user, post, now - age_ms, 1)).await,
            Ok(RecordOutcome::Stored)
        );
    }
    // Replayed data outside the seven-day online window is settled without
    // being written, so it can never displace a recent action.
    assert_eq!(
        store.record(&action(user, 99, now - 8 * DAY_MS, 1)).await,
        Ok(RecordOutcome::Skipped(SkipReason::OutsideWindow))
    );

    assert_eq!(
        read_post_ids(&store, user).await,
        vec![pid(2), pid(3), pid(4)]
    );
    assert_eq!(zcard(&redis, "test:uas", user), 3);
}

#[tokio::test]
#[ignore = "requires redis-server"]
async fn reader_keeps_the_newest_actions_when_the_store_exceeds_its_limit() {
    let redis = RedisFixture::start();
    let prefix = "test:uas-limits";
    // The projection job and Home Mixer read UAS_MAX_ACTIONS independently;
    // a reader with the smaller bound must still see the newest actions.
    let writer = RedisUserActionSequenceStore::new(redis.uas_config(prefix, 5))
        .await
        .expect("create writer");
    let reader = RedisUserActionSequenceStore::new(redis.uas_config(prefix, 2))
        .await
        .expect("create reader");
    let now = now_ms();
    let user = uid(8);
    for post in 1..=5 {
        writer
            .record(&action(user, post, now - (6 - post as i64) * 1_000, 1))
            .await
            .expect("project action");
    }

    assert_eq!(read_post_ids(&reader, user).await, vec![pid(4), pid(5)]);
    assert_eq!(
        read_post_ids(&writer, user).await,
        vec![pid(1), pid(2), pid(3), pid(4), pid(5)]
    );
}

#[tokio::test]
#[ignore = "requires redis-server"]
async fn redelivering_an_event_does_not_duplicate_members() {
    let redis = RedisFixture::start();
    let prefix = "test:uas-redelivery";
    let store = RedisUserActionSequenceStore::new(redis.uas_config(prefix, 10))
        .await
        .expect("create UAS adapter");
    let now = now_ms();
    let user = uid(9);
    let event = action(user, 1, now - 1_000, 3);

    for _ in 0..3 {
        assert_eq!(store.record(&event).await, Ok(RecordOutcome::Stored));
    }
    // A different action on the same post is a different member.
    store
        .record(&action(user, 1, now - 1_000, 6))
        .await
        .expect("second action type");

    assert_eq!(zcard(&redis, prefix, user), 2);
    let actions = store
        .get_by_user_id(user)
        .await
        .expect("read")
        .user_actions
        .expect("actions");
    assert_eq!(actions.len(), 2);
    assert!(actions.iter().all(|action| action.tweet_id == Some(pid(1))));
}

#[tokio::test]
#[ignore = "requires redis-server"]
async fn future_timestamps_within_the_skew_tolerance_are_stored_and_beyond_are_skipped() {
    let redis = RedisFixture::start();
    let prefix = "test:uas-skew";
    let mut config = redis.uas_config(prefix, 10);
    config.max_future_skew = Duration::from_secs(60);
    let tolerant = RedisUserActionSequenceStore::new(config)
        .await
        .expect("create tolerant adapter");
    let mut strict_config = redis.uas_config(prefix, 10);
    strict_config.max_future_skew = Duration::ZERO;
    let strict = RedisUserActionSequenceStore::new(strict_config)
        .await
        .expect("create strict adapter");
    let now = now_ms();
    let user = uid(10);

    tolerant
        .record(&action(user, 1, now - 1_000, 1))
        .await
        .expect("current action");
    // A producer clock a few seconds ahead is stored with its own timestamp.
    assert_eq!(
        tolerant.record(&action(user, 2, now + 10_000, 1)).await,
        Ok(RecordOutcome::Stored)
    );
    // Beyond the tolerance the event is settled but reported, not silently
    // written as the "newest" action.
    assert_eq!(
        tolerant.record(&action(user, 3, now + 120_000, 1)).await,
        Ok(RecordOutcome::Skipped(SkipReason::FutureTimestamp))
    );
    assert_eq!(
        strict.record(&action(user, 4, now + 2_000, 1)).await,
        Ok(RecordOutcome::Skipped(SkipReason::FutureTimestamp))
    );

    assert_eq!(zcard(&redis, prefix, user), 2);
    // The read window ends at the reader's clock, so the skewed action
    // becomes visible only once wall-clock time reaches it.
    assert_eq!(read_post_ids(&tolerant, user).await, vec![pid(1)]);
}

#[tokio::test]
#[ignore = "requires redis-server"]
async fn ttl_follows_the_config_and_none_persists_the_key() {
    let redis = RedisFixture::start();
    let prefix = "test:uas-ttl";
    let user = uid(11);
    let now = now_ms();

    let mut expiring_config = redis.uas_config(prefix, 10);
    expiring_config.ttl_secs = Some(30);
    let expiring = RedisUserActionSequenceStore::new(expiring_config)
        .await
        .expect("create expiring adapter");
    expiring
        .record(&action(user, 1, now - 1_000, 1))
        .await
        .expect("first write");
    let key = redis.key(prefix, user, "actions");
    let mut control = redis.connection();
    let ttl: i64 = redis::cmd("PTTL").arg(&key).query(&mut control).unwrap();
    assert!((0..=30_000).contains(&ttl), "expiring key has a TTL: {ttl}");

    let mut persistent_config = redis.uas_config(prefix, 10);
    persistent_config.ttl_secs = None;
    let persistent = RedisUserActionSequenceStore::new(persistent_config)
        .await
        .expect("create persistent adapter");
    persistent
        .record(&action(user, 2, now - 500, 1))
        .await
        .expect("second write");
    let ttl: i64 = redis::cmd("PTTL").arg(&key).query(&mut control).unwrap();
    assert_eq!(ttl, -1, "ttl_secs = None must clear the expiry");
}

#[tokio::test]
#[ignore = "requires redis-server"]
async fn undecodable_members_are_skipped_without_losing_the_rest() {
    let redis = RedisFixture::start();
    let prefix = "test:uas-corrupt";
    let store = RedisUserActionSequenceStore::new(redis.uas_config(prefix, 10))
        .await
        .expect("create UAS adapter");
    let now = now_ms();
    let user = uid(12);
    store
        .record(&action(user, 1, now - 3_000, 1))
        .await
        .expect("first action");
    store
        .record(&action(user, 2, now - 1_000, 1))
        .await
        .expect("second action");

    let key = redis.key(prefix, user, "actions");
    let mut control = redis.connection();
    // A corrupt member and a member from a future schema version, both
    // newer than one valid action so the skip happens mid-sequence.
    redis::cmd("ZADD")
        .arg(&key)
        .arg(now - 2_000)
        .arg("not json")
        .arg(now - 1_500)
        .arg(r#"{"version":2,"tweet_id":"000000000000000000000063","author_id":"0000000000000000000000c7","action_time_ms":1,"action_type":1}"#)
        .query::<i64>(&mut control)
        .unwrap();

    // A version bump must never take every user's personalization down for
    // the whole retention window; the reader degrades member by member.
    assert_eq!(read_post_ids(&store, user).await, vec![pid(1), pid(2)]);
}

#[tokio::test]
#[ignore = "requires redis-server"]
async fn writes_run_on_the_replacement_connection_after_a_server_side_disconnect() {
    let redis = RedisFixture::start();
    let prefix = "test:uas-disconnect";
    let store = RedisUserActionSequenceStore::new(redis.uas_config(prefix, 10))
        .await
        .expect("create UAS adapter");
    let now = now_ms();
    let user = uid(13);
    store
        .record(&action(user, 1, now - 2_000, 1))
        .await
        .expect("first write");

    // A proxy idle timeout closes the projection job's connection between
    // two events. The idempotent write must succeed on the replacement
    // instead of surfacing a storage failure and a retry cycle.
    redis.disconnect_clients();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        store.record(&action(user, 2, now - 1_000, 1)).await,
        Ok(RecordOutcome::Stored)
    );

    redis.disconnect_clients();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(read_post_ids(&store, user).await, vec![pid(1), pid(2)]);
}

#[tokio::test]
#[ignore = "requires redis-server"]
async fn the_sequence_hydrator_consumes_the_projected_actions() {
    let redis = RedisFixture::start();
    let store = RedisUserActionSequenceStore::new(redis.uas_config("test:uas-hydrate", 10))
        .await
        .expect("create UAS adapter");
    let now = now_ms();
    let user = uid(14);
    // Two actions on one post aggregate into one entry with both mask bits.
    for (post, age_ms, action_type) in [(1, 3_000, 1), (1, 2_000, 2), (2, 1_000, 6)] {
        store
            .record(&action(user, post, now - age_ms, action_type))
            .await
            .expect("project action");
    }

    let hydrator = UserActionSeqQueryHydrator::new(Arc::new(store));
    let sequence = hydrator
        .hydrate_sequence(&ScoredPostsQuery {
            user_id: user,
            request_id: "redis-uas".to_string(),
            prediction_id: 1,
            request_time_ms: now,
            ..Default::default()
        })
        .await
        .expect("aggregated sequence");
    let metadata = sequence.metadata.expect("metadata");
    assert_eq!(metadata.length, 2);
    assert_eq!(metadata.first_sequence_time, (now - 3_000) as u64);
    assert_eq!(metadata.last_sequence_time, (now - 1_000) as u64);
    assert_eq!(sequence.user_id, user.to_string());
}
