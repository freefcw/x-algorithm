#![cfg(unix)]

mod common;

use common::{unix_redis_url, RedisFixture};
use home_mixer::clients::redis_feed_state_store::{RedisFeedStateConfig, RedisFeedStateStore};
use home_mixer::feed_state::{FeedStateSnapshot, FeedStateStore, InMemoryFeedStateStore};
use std::fs;
use std::net::Shutdown;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier as ThreadBarrier};
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::Barrier;

fn sorted<T: Ord>(mut values: Vec<T>) -> Vec<T> {
    values.sort_unstable();
    values
}

#[tokio::test]
#[ignore = "requires redis-server"]
async fn missing_users_are_empty_and_independent_adapters_share_isolated_user_state() {
    let redis = RedisFixture::start();
    let prefix = "test:shared-state";
    let first = RedisFeedStateStore::new(redis.config(prefix, 10, 10))
        .await
        .expect("create first adapter");
    let second = RedisFeedStateStore::new(redis.config(prefix, 10, 10))
        .await
        .expect("create second adapter");
    let alice = 1007;
    let bob = 1014;
    let post = 1021;

    assert_eq!(
        first.load(alice).await.unwrap(),
        FeedStateSnapshot::default()
    );
    first.record(alice, vec![post], -7).await.unwrap();
    assert_eq!(
        second.load(alice).await.unwrap(),
        FeedStateSnapshot {
            served_post_ids: vec![post],
            request_timestamps_ms: vec![-7],
        }
    );
    assert_eq!(
        second.load(bob).await.unwrap(),
        FeedStateSnapshot::default()
    );

    second.record(bob, vec![], 91).await.unwrap();
    assert_eq!(first.load(alice).await.unwrap().served_post_ids, vec![post]);
    assert_eq!(
        first.load(bob).await.unwrap().request_timestamps_ms,
        vec![91]
    );
}

#[tokio::test]
#[ignore = "requires redis-server"]
async fn redis_matches_in_memory_deduplication_order_and_truncation_including_zero_limits() {
    let redis = RedisFixture::start();
    let prefix = "test:bounded-state";
    let remote = RedisFeedStateStore::new(redis.config(prefix, 3, 2))
        .await
        .expect("create Redis adapter");
    let local = InMemoryFeedStateStore::new(3, 2);
    let user = 1028;
    let a = 1035;
    let b = 1042;
    let c = 1049;
    let d = 1056;
    let updates = [
        (vec![a, b, a], i64::MIN),
        (vec![c, b, d], -1),
        (vec![d], i64::MAX),
    ];

    for (ids, timestamp) in updates {
        local.record(user, ids.clone(), timestamp).await.unwrap();
        remote.record(user, ids, timestamp).await.unwrap();
    }

    let expected = local.load(user).await.unwrap();
    assert_eq!(expected.served_post_ids, vec![c, b, d]);
    assert_eq!(expected.request_timestamps_ms, vec![-1, i64::MAX]);
    assert_eq!(remote.load(user).await.unwrap(), expected);

    let zero = RedisFeedStateStore::new(redis.config("test:zero-state", 0, 0))
        .await
        .expect("create zero-limit adapter");
    zero.record(user, vec![a, b], i64::MIN).await.unwrap();
    assert_eq!(zero.load(user).await.unwrap(), FeedStateSnapshot::default());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires redis-server"]
async fn concurrent_adapters_do_not_lose_updates_and_round_trip_full_width_values() {
    const UPDATE_COUNT: usize = 48;

    let redis = RedisFixture::start();
    let prefix = "test:concurrent-state";
    let first = Arc::new(
        RedisFeedStateStore::new(redis.config(prefix, UPDATE_COUNT, UPDATE_COUNT))
            .await
            .expect("create first adapter"),
    );
    let second = Arc::new(
        RedisFeedStateStore::new(redis.config(prefix, UPDATE_COUNT, UPDATE_COUNT))
            .await
            .expect("create second adapter"),
    );
    let user = 1063;
    let barrier = Arc::new(Barrier::new(UPDATE_COUNT + 1));
    let mut tasks = Vec::with_capacity(UPDATE_COUNT);
    let mut expected_ids = Vec::with_capacity(UPDATE_COUNT);
    let mut expected_timestamps = Vec::with_capacity(UPDATE_COUNT);

    for index in 0..UPDATE_COUNT {
        let id = 0x8000_0000u64 + index as u64;
        let timestamp = if index == 0 {
            i64::MIN
        } else if index == 1 {
            i64::MAX
        } else {
            -(index as i64)
        };
        expected_ids.push(id);
        expected_timestamps.push(timestamp);
        let store = if index % 2 == 0 {
            first.clone()
        } else {
            second.clone()
        };
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            store.record(user, vec![id], timestamp).await
        }));
    }

    barrier.wait().await;
    for task in tasks {
        task.await.expect("record task did not panic").unwrap();
    }

    let snapshot = first.load(user).await.unwrap();
    assert_eq!(snapshot.served_post_ids.len(), UPDATE_COUNT);
    assert_eq!(snapshot.request_timestamps_ms.len(), UPDATE_COUNT);
    assert_eq!(sorted(snapshot.served_post_ids), sorted(expected_ids));
    assert_eq!(
        sorted(snapshot.request_timestamps_ms),
        sorted(expected_timestamps)
    );
}

#[tokio::test]
#[ignore = "requires redis-server"]
async fn ttl_slides_expires_and_none_removes_an_existing_expiry() {
    let redis = RedisFixture::start();
    let user = 1070;
    let first_post = 1077;
    let second_post = 1084;

    let mut expiring_config = redis.config("test:sliding-ttl", 10, 10);
    expiring_config.ttl_secs = Some(1);
    let expiring = RedisFeedStateStore::new(expiring_config)
        .await
        .expect("create expiring adapter");
    expiring.record(user, vec![first_post], 1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(650)).await;
    expiring.record(user, vec![second_post], 2).await.unwrap();
    tokio::time::sleep(Duration::from_millis(650)).await;
    assert_eq!(
        expiring.load(user).await.unwrap().served_post_ids,
        vec![first_post, second_post],
        "the second write must refresh the one-second TTL"
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        expiring.load(user).await.unwrap(),
        FeedStateSnapshot::default(),
        "both state keys must expire after the refreshed TTL elapses"
    );

    let persist_prefix = "test:persist-ttl";
    let mut initial_config = redis.config(persist_prefix, 10, 10);
    initial_config.ttl_secs = Some(30);
    let initial = RedisFeedStateStore::new(initial_config).await.unwrap();
    initial.record(user, vec![first_post], 3).await.unwrap();

    let served_key = redis.key(persist_prefix, user, "served");
    let timestamps_key = redis.key(persist_prefix, user, "timestamps");
    let mut control = redis.connection();
    let initial_ttls = [
        redis::cmd("PTTL")
            .arg(&served_key)
            .query::<i64>(&mut control)
            .unwrap(),
        redis::cmd("PTTL")
            .arg(&timestamps_key)
            .query::<i64>(&mut control)
            .unwrap(),
    ];
    assert!(initial_ttls.into_iter().all(|ttl| ttl > 0));

    let mut persistent_config = redis.config(persist_prefix, 10, 10);
    persistent_config.ttl_secs = None;
    let persistent = RedisFeedStateStore::new(persistent_config).await.unwrap();
    persistent.record(user, vec![second_post], 4).await.unwrap();
    let persisted_ttls = [
        redis::cmd("PTTL")
            .arg(&served_key)
            .query::<i64>(&mut control)
            .unwrap(),
        redis::cmd("PTTL")
            .arg(&timestamps_key)
            .query::<i64>(&mut control)
            .unwrap(),
    ];
    assert_eq!(persisted_ttls, [-1, -1]);
}

#[tokio::test]
#[ignore = "requires redis-server"]
async fn malformed_redis_state_is_reported_instead_of_becoming_empty_state() {
    let redis = RedisFixture::start();
    let prefix = "test:malformed-state";
    let store = RedisFeedStateStore::new(redis.config(prefix, 10, 10))
        .await
        .expect("create Redis adapter");
    let user = 1091;
    let post = 1098;
    store.record(user, vec![post], 10).await.unwrap();

    let served_key = redis.key(prefix, user, "served");
    let mut control = redis.connection();
    redis::cmd("DEL")
        .arg(&served_key)
        .query::<usize>(&mut control)
        .unwrap();
    redis::cmd("RPUSH")
        .arg(&served_key)
        .arg("not-a-96-bit-object-id")
        .query::<usize>(&mut control)
        .unwrap();

    let error = store
        .load(user)
        .await
        .expect_err("corrupt ID must fail load");
    assert!(
        error.contains("served") || error.contains("post") || error.contains("object id"),
        "error should identify malformed served state: {error}"
    );

    redis::cmd("DEL")
        .arg(&served_key)
        .query::<usize>(&mut control)
        .unwrap();
    let timestamps_key = redis.key(prefix, user, "timestamps");
    redis::cmd("DEL")
        .arg(&timestamps_key)
        .query::<usize>(&mut control)
        .unwrap();
    redis::cmd("RPUSH")
        .arg(&timestamps_key)
        .arg("not-an-i64")
        .query::<usize>(&mut control)
        .unwrap();
    assert!(
        store.load(user).await.is_err(),
        "corrupt timestamp must fail load"
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires redis-server"]
async fn paused_redis_times_out_without_blocking_tokio_and_recovers_for_later_requests() {
    let redis = RedisFixture::start();
    let prefix = "test:paused-state";
    let mut config = redis.config(prefix, 10, 10);
    config.request_timeout = Duration::from_millis(100);
    let store = RedisFeedStateStore::new(config)
        .await
        .expect("create Redis adapter");
    let user = 1105;
    let post = 1112;
    store.record(user, vec![post], 1).await.unwrap();

    pause_redis(&redis, 600);
    let timer_started = Instant::now();
    let timer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        timer_started.elapsed()
    });
    let load_started = Instant::now();
    let error = store
        .load(user)
        .await
        .expect_err("paused Redis read must time out");
    let load_elapsed = load_started.elapsed();
    let timer_elapsed = timer.await.expect("timer task did not panic");
    assert!(
        load_elapsed < Duration::from_millis(350),
        "read ignored the 100ms request timeout: {load_elapsed:?}, error={error}"
    );
    assert!(
        timer_elapsed < Duration::from_millis(250),
        "Redis I/O blocked the current-thread Tokio scheduler for {timer_elapsed:?}"
    );

    tokio::time::sleep(Duration::from_millis(650)).await;
    pause_redis(&redis, 600);
    let write_started = Instant::now();
    let error = store
        .record(user, vec![post], 2)
        .await
        .expect_err("paused Redis write must time out");
    assert!(
        write_started.elapsed() < Duration::from_millis(350),
        "write ignored the 100ms request timeout: {error}"
    );

    tokio::time::sleep(Duration::from_millis(650)).await;
    store.record(user, vec![post], 3).await.unwrap();
    let recovered = store.load(user).await.unwrap();
    assert_eq!(recovered.served_post_ids, vec![post]);
    // A write that times out may still have reached Redis. Recovery only
    // promises that later commands work; it cannot retroactively cancel that
    // transaction.
    assert!(recovered.request_timestamps_ms.contains(&1));
    assert!(recovered.request_timestamps_ms.contains(&3));
    assert_eq!(recovered.request_timestamps_ms.last(), Some(&3));
}

#[tokio::test]
#[ignore = "requires redis-server"]
async fn first_read_after_a_server_side_disconnect_sees_the_served_history() {
    let redis = RedisFixture::start();
    let store = RedisFeedStateStore::new(redis.config("test:disconnect", 10, 10))
        .await
        .expect("create Redis adapter");
    let user = 1119;
    let post = 1126;
    store.record(user, vec![post], 1).await.unwrap();

    // A proxy idle timeout or server-side eviction closes the adapter's
    // connection between two requests. The next read must not fail open and
    // let the request serve the same posts again.
    redis.disconnect_clients();
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(
        store.load(user).await,
        Ok(FeedStateSnapshot {
            served_post_ids: vec![post],
            request_timestamps_ms: vec![1],
        }),
        "the first read after a disconnect must run on the replacement connection"
    );
}

#[tokio::test]
#[ignore = "requires redis-server"]
async fn first_read_after_a_redis_restart_sees_the_persisted_history() {
    let mut redis = RedisFixture::start();
    let store = RedisFeedStateStore::new(redis.config("test:restart", 10, 10))
        .await
        .expect("create Redis adapter");
    let user = 1133;
    let post = 1140;
    store.record(user, vec![post], 1).await.unwrap();

    // Maintenance restart or failover with no request in flight.
    redis.stop_preserving_data();
    redis.restart();

    assert_eq!(
        store.load(user).await,
        Ok(FeedStateSnapshot {
            served_post_ids: vec![post],
            request_timestamps_ms: vec![1],
        }),
        "history persisted across the restart must be visible to the first request"
    );
}

#[tokio::test]
#[ignore = "requires redis-server"]
async fn first_read_after_an_outage_recovers_without_a_restart_of_the_adapter() {
    let mut redis = RedisFixture::start();
    let mut config = redis.config("test:outage", 10, 10);
    config.request_timeout = Duration::from_millis(200);
    config.connect_timeout = Duration::from_millis(200);
    let store = RedisFeedStateStore::new(config)
        .await
        .expect("create Redis adapter");
    let user = 1147;
    let post = 1154;
    store.record(user, vec![post], 1).await.unwrap();

    // While Redis is down every request fails closed at the write, so the
    // read failing here cannot cause a duplicate.
    redis.stop_losing_data();
    assert!(store.load(user).await.is_err(), "read during outage");
    assert!(
        store.record(user, vec![post], 2).await.is_err(),
        "write during outage"
    );
    // Let the background reconnect attempt run (and fail) while the server is
    // still down, as it does on a multi-threaded runtime.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // The first request after recovery previously consumed the stale
    // "reconnecting failed" result, failed open, and then persisted
    // successfully on the new connection.
    redis.restart();
    assert_eq!(
        store.load(user).await,
        Ok(FeedStateSnapshot::default()),
        "the first read after recovery must succeed on the new connection"
    );
    store.record(user, vec![post], 3).await.unwrap();
    assert_eq!(store.load(user).await.unwrap().served_post_ids, vec![post]);
}

fn pause_redis(redis: &RedisFixture, duration_ms: u64) {
    redis.pause(duration_ms);
}

struct BlackholeRedis {
    directory: PathBuf,
    socket: PathBuf,
    stop: Arc<AtomicBool>,
    accept_thread: Option<thread::JoinHandle<()>>,
}

impl BlackholeRedis {
    fn start() -> Self {
        let directory = common::fixture_directory("blackhole");
        let socket = directory.join("redis.sock");
        let listener = UnixListener::bind(&socket).expect("bind blackhole Redis socket");
        listener
            .set_nonblocking(true)
            .expect("make blackhole listener nonblocking");
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = stop.clone();
        let started = Arc::new(ThreadBarrier::new(2));
        let thread_started = started.clone();
        let accept_thread = thread::spawn(move || {
            let mut clients: Vec<UnixStream> = Vec::new();
            thread_started.wait();
            while !thread_stop.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((client, _)) => clients.push(client),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
            for client in clients {
                let _ = client.shutdown(Shutdown::Both);
            }
        });
        started.wait();
        Self {
            directory,
            socket,
            stop,
            accept_thread: Some(accept_thread),
        }
    }

    fn url(&self) -> String {
        unix_redis_url(&self.socket)
    }
}

impl Drop for BlackholeRedis {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        // Wake the nonblocking accept loop promptly on slow test hosts.
        let _ = UnixStream::connect(&self.socket);
        if let Some(thread) = self.accept_thread.take() {
            let _ = thread.join();
        }
        let _ = fs::remove_file(&self.socket);
        let _ = fs::remove_dir_all(&self.directory);
    }
}

#[tokio::test]
#[ignore = "requires redis-server"]
async fn connection_to_an_unresponsive_redis_is_bounded_by_configured_timeouts() {
    let blackhole = BlackholeRedis::start();
    let mut config = RedisFeedStateConfig::new(blackhole.url());
    config.connect_timeout = Duration::from_millis(100);
    config.request_timeout = Duration::from_millis(100);
    let started = Instant::now();

    let result = tokio::time::timeout(Duration::from_secs(1), RedisFeedStateStore::new(config))
        .await
        .expect("constructor exceeded its configured timeouts");
    let elapsed = started.elapsed();

    assert!(result.is_err(), "unresponsive Redis must fail construction");
    assert!(
        elapsed < Duration::from_millis(500),
        "connection setup was not bounded by configured timeouts: {elapsed:?}"
    );
}

#[tokio::test]
#[ignore = "requires redis-server and redis-cli (cluster mode)"]
async fn cluster_routing_round_trips_feed_state_across_many_users() {
    let cluster = common::RedisClusterFixture::start();
    let store = RedisFeedStateStore::new(cluster.feed_config("test:cluster-feed", 5, 5))
        .await
        .expect("create cluster-backed feed-state adapter");

    // Enough users to spread over the 3 masters' slots; each record is one
    // hash-tagged MULTI, so success itself proves slot collocation.
    let first = 1u64;
    let second = 12u64;
    for sequence in first..=second {
        let user = sequence;
        let post = sequence + 100;
        store
            .record(user, vec![post], -(sequence as i64))
            .await
            .expect("record through cluster routing");
        let snapshot = store
            .load(user)
            .await
            .expect("load through cluster routing");
        assert_eq!(snapshot.served_post_ids, vec![post]);
        assert_eq!(snapshot.request_timestamps_ms, vec![-(sequence as i64)]);
    }

    // Users stay independent under cluster routing.
    assert_eq!(
        store.load(second).await.unwrap().served_post_ids,
        vec![second + 100]
    );
    assert_eq!(store.load(first).await.unwrap().served_post_ids, vec![first + 100]);
}
