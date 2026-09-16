// 用户行为序列获取器 (UAS Fetcher)
//
// 替代原始被阉割的 UAS 获取客户端。
//
// 原始功能说明：
// UserActionSequenceFetcher 从 X 内部的 UAS 存储服务
// 获取指定用户的行为序列数据（Thrift 格式）。
//
// 数据流：
//   用户在客户端的各种操作（点赞、回复等）
//     → 客户端/服务端埋点
//     → Kafka 行为事件流
//     → UAS 聚合服务 → UAS 存储（Manhattan KV）
//     → Home Mixer 通过 UAS Fetcher 获取
//
// 这是 Phoenix 精排模型最核心的输入特征来源。
// 没有用户行为序列，Phoenix 无法做个性化排序。
//
// 本地实现使用 Redis ZSET 作为轻量投影：`uas-worker` 消费 Kafka/stdin 事件，
// 按行为时间写入最近 7 天、有界条数的成员；Home Mixer 进程内 adapter 取最新
// 一段交给现有聚合器。事件只在 [`UserActionEvent::validate`] 这一处校验，
// 写入幂等，读取对无法解码的成员容忍跳过并告警。
// Disabled 实现保留给显式不配置 UAS Redis 的装配路径与测试。

use crate::models::ids::{ObjectId, PostId, UserId};
use crate::recsys_compat::{is_supported_action_type, MAX_SUPPORTED_ACTION_TYPE};
use crate::uas_compat;
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tonic::async_trait;

const DEFAULT_UAS_KEY_PREFIX: &str = "home_mixer:uas";
const DEFAULT_UAS_TTL_SECS: u64 = 7 * 24 * 60 * 60;

/// 用户行为序列操作 trait
///
/// 定义了获取用户行为序列数据的标准接口。
/// 生产实现应从用户行为存储（如 Redis、自建 KV）中获取。
#[async_trait]
pub trait UserActionSequenceOps: Send + Sync {
    /// 根据用户 ID 获取行为序列
    ///
    /// # Arguments
    /// * `user_id` - 用户 ID
    ///
    /// # Returns
    /// Thrift 格式的用户行为序列
    async fn get_by_user_id(
        &self,
        user_id: UserId,
    ) -> Result<uas_compat::UserActionSequence, anyhow::Error>;
}

/// Write port used by projection jobs. Kafka and stdin handling depend on this
/// boundary rather than on Redis, which keeps delivery decisions testable.
#[async_trait]
pub trait UserActionEventSink: Send + Sync {
    /// Project one validated action.
    ///
    /// `Err` always means the storage backend failed. The action itself was
    /// accepted at the [`UserActionEvent::validate`] boundary, so callers may
    /// retry the same action without re-validating; the write is idempotent.
    async fn record(&self, action: &ValidatedUserAction) -> Result<RecordOutcome, String>;
}

/// What the sink did with an accepted action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordOutcome {
    Stored,
    /// Accepted but deliberately not written; the caller may advance past it.
    Skipped(SkipReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SkipReason {
    /// Older than the online window: a replay that could only displace
    /// useful recent actions.
    OutsideWindow,
    /// Ahead of the job clock by more than the configured skew tolerance.
    FutureTimestamp,
}

impl SkipReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OutsideWindow => "outside_window",
            Self::FutureTimestamp => "future_timestamp",
        }
    }
}

/// Standardized event written by the telemetry/Kafka projection job.
/// Keeping this payload small makes it suitable for a Kafka JSON envelope and
/// for a Redis ZSET member. IDs stay strings at the event boundary.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct UserActionEvent {
    pub user_id: String,
    pub tweet_id: String,
    pub author_id: String,
    pub action_time_ms: i64,
    pub action_type: i32,
}

/// An event that passed the boundary checks: every id parses and is non-nil,
/// the timestamp is positive and the action type is one the model consumes.
/// It can only be built through [`UserActionEvent::validate`], so sinks do
/// not re-validate and a sink error is always a storage error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedUserAction {
    user_id: UserId,
    tweet_id: PostId,
    author_id: UserId,
    action_time_ms: i64,
    action_type: i32,
}

impl ValidatedUserAction {
    pub fn user_id(&self) -> UserId {
        self.user_id
    }

    pub fn action_time_ms(&self) -> i64 {
        self.action_time_ms
    }
}

impl UserActionEvent {
    pub fn validate(self) -> Result<ValidatedUserAction, String> {
        let user_id =
            ObjectId::parse(&self.user_id).map_err(|e| format!("invalid user_id: {e}"))?;
        let tweet_id =
            ObjectId::parse(&self.tweet_id).map_err(|e| format!("invalid tweet_id: {e}"))?;
        let author_id =
            ObjectId::parse(&self.author_id).map_err(|e| format!("invalid author_id: {e}"))?;
        if user_id.is_nil() || tweet_id.is_nil() || author_id.is_nil() {
            return Err("user_id, tweet_id and author_id must be non-nil".to_string());
        }
        if self.action_time_ms <= 0 {
            return Err("action_time_ms must be positive".to_string());
        }
        if !is_supported_action_type(self.action_type) {
            return Err(format!(
                "action_type must be between 1 and {MAX_SUPPORTED_ACTION_TYPE}"
            ));
        }
        Ok(ValidatedUserAction {
            user_id,
            tweet_id,
            author_id,
            action_time_ms: self.action_time_ms,
            action_type: self.action_type,
        })
    }
}

/// Stable Redis member schema. It is deliberately separate from the domain
/// model so an internal refactor cannot silently invalidate live Redis data.
/// Field order is the serialized order: an identical event must serialize to
/// an identical member so that redelivery does not create duplicates.
#[derive(Debug, Deserialize, Serialize)]
struct StoredUserAction {
    version: u8,
    tweet_id: String,
    author_id: String,
    action_time_ms: i64,
    action_type: i32,
}

impl StoredUserAction {
    const VERSION: u8 = 1;

    fn from_validated(action: &ValidatedUserAction) -> Self {
        Self {
            version: Self::VERSION,
            tweet_id: action.tweet_id.to_string(),
            author_id: action.author_id.to_string(),
            action_time_ms: action.action_time_ms,
            action_type: action.action_type,
        }
    }

    fn into_action(self) -> Result<uas_compat::UserAction, String> {
        // Only the current layout is known. When VERSION is bumped, keep
        // decoding the previous layout here until every member written with
        // it has aged out of the window; the reader skips what it cannot
        // decode instead of failing the whole sequence.
        if self.version != Self::VERSION {
            return Err(format!("unsupported member version {}", self.version));
        }
        let tweet_id = ObjectId::parse(&self.tweet_id)
            .map_err(|error| format!("invalid stored tweet_id: {error}"))?;
        let author_id = ObjectId::parse(&self.author_id)
            .map_err(|error| format!("invalid stored author_id: {error}"))?;
        if !is_supported_action_type(self.action_type) {
            return Err(format!("unsupported action_type {}", self.action_type));
        }
        Ok(uas_compat::UserAction {
            tweet_id: Some(tweet_id),
            author_id: Some(author_id),
            action_time_ms: Some(self.action_time_ms),
            action_type: Some(self.action_type),
        })
    }
}

/// Decode one ZSET member. Corrupt payloads and unknown versions are reported
/// so the reader can skip them rather than lose the user's whole sequence.
fn decode_stored_member(member: &str) -> Result<uas_compat::UserAction, String> {
    serde_json::from_str::<StoredUserAction>(member)
        .map_err(|error| format!("member is not a StoredUserAction: {error}"))?
        .into_action()
}

#[derive(Clone, Eq, PartialEq)]
pub struct RedisUserActionSequenceConfig {
    pub url: String,
    pub key_prefix: String,
    /// Raw actions kept per user; see `params::UAS_STORE_MAX_ACTIONS`.
    pub max_actions: usize,
    pub window: Duration,
    /// How far ahead of the job clock an action timestamp may be and still be
    /// stored; see `params::UAS_MAX_FUTURE_SKEW_MS`.
    pub max_future_skew: Duration,
    pub ttl_secs: Option<u64>,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
}

impl fmt::Debug for RedisUserActionSequenceConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The URL may carry credentials; never let a debug dump leak them.
        formatter
            .debug_struct("RedisUserActionSequenceConfig")
            .field("url", &"<redacted>")
            .field("key_prefix", &self.key_prefix)
            .field("max_actions", &self.max_actions)
            .field("window", &self.window)
            .field("max_future_skew", &self.max_future_skew)
            .field("ttl_secs", &self.ttl_secs)
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
}

impl RedisUserActionSequenceConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            key_prefix: DEFAULT_UAS_KEY_PREFIX.to_string(),
            max_actions: crate::params::UAS_STORE_MAX_ACTIONS,
            window: Duration::from_millis(crate::params::UAS_WINDOW_TIME_MS),
            max_future_skew: Duration::from_millis(crate::params::UAS_MAX_FUTURE_SKEW_MS),
            ttl_secs: Some(DEFAULT_UAS_TTL_SECS),
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_millis(crate::params::UAS_FETCH_TIMEOUT_MS),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.url.trim().is_empty() {
            return Err("UAS Redis URL must not be empty".to_string());
        }
        redis::Client::open(self.url.as_str()).map_err(|_| "invalid UAS Redis URL".to_string())?;
        if self.key_prefix.trim().is_empty() || self.key_prefix.contains(['{', '}']) {
            return Err(
                "UAS Redis key prefix must be non-empty and must not contain braces".to_string(),
            );
        }
        if self.max_actions == 0 || self.max_actions > i64::MAX as usize {
            return Err("UAS max_actions must be between 1 and i64::MAX".to_string());
        }
        if self.window.is_zero()
            || self.window.as_millis() > i64::MAX as u128
            || self.connect_timeout.is_zero()
            || self.request_timeout.is_zero()
        {
            return Err("UAS durations must be positive and fit in milliseconds".to_string());
        }
        if self.max_future_skew.as_millis() > i64::MAX as u128 {
            return Err("UAS future skew tolerance must fit in milliseconds".to_string());
        }
        if self
            .ttl_secs
            .is_some_and(|value| value == 0 || value > i64::MAX as u64)
        {
            return Err("UAS Redis TTL is out of range".to_string());
        }
        Ok(())
    }
}

/// Redis ZSET-backed UAS adapter used by both the projection job and
/// home-mixer. One key is kept per member: `prefix:{user_id}:actions`, scored
/// by action time; the member is the JSON [`StoredUserAction`].
pub struct RedisUserActionSequenceStore {
    connection: ConnectionManager,
    key_prefix: String,
    max_actions: usize,
    window: Duration,
    max_future_skew: Duration,
    ttl_secs: Option<u64>,
    request_timeout: Duration,
}

impl RedisUserActionSequenceStore {
    pub async fn new(config: RedisUserActionSequenceConfig) -> Result<Self, String> {
        config.validate()?;
        let client = redis::Client::open(config.url.as_str())
            .map_err(|_| "invalid UAS Redis URL".to_string())?;
        let manager_config = ConnectionManagerConfig::new()
            .set_number_of_retries(0)
            .set_connection_timeout(config.connect_timeout)
            .set_response_timeout(config.request_timeout);
        let connection = tokio::time::timeout(
            config.connect_timeout,
            ConnectionManager::new_with_config(client, manager_config),
        )
        .await
        .map_err(|_| "UAS Redis connection timed out".to_string())?
        .map_err(|e| redis_error("UAS Redis connection", &e))?;
        let mut health = connection.clone();
        let pong = tokio::time::timeout(
            config.request_timeout,
            redis::cmd("PING").query_async::<String>(&mut health),
        )
        .await
        .map_err(|_| "UAS Redis health check timed out".to_string())?
        .map_err(|e| redis_error("UAS Redis health check", &e))?;
        if pong != "PONG" {
            return Err(format!("UAS Redis health check returned {pong}"));
        }
        Ok(Self {
            connection,
            key_prefix: config.key_prefix,
            max_actions: config.max_actions,
            window: config.window,
            max_future_skew: config.max_future_skew,
            ttl_secs: config.ttl_secs,
            request_timeout: config.request_timeout,
        })
    }

    fn key(&self, user_id: UserId) -> String {
        // The hash tag keeps a user's key addressable by cluster-aware proxies
        // and matches the feed-state key layout.
        format!("{}:{{{user_id}}}:actions", self.key_prefix)
    }

    /// Run an idempotent pipeline, retrying once if the first attempt failed
    /// because the connection manager discarded its connection. Both the read
    /// and the projection write qualify: `ZADD` of an identical member, the
    /// range trims and `EXPIRE` all converge to the same state when repeated.
    /// The caller bounds both attempts with one `request_timeout`; a timeout
    /// is not retried because the budget is spent.
    async fn run_idempotent<T: redis::FromRedisValue>(
        &self,
        pipeline: &redis::Pipeline,
    ) -> redis::RedisResult<T> {
        let mut connection = self.connection.clone();
        match pipeline.query_async(&mut connection).await {
            Err(error) if connection_replaced(&error) => {
                pipeline.query_async(&mut connection).await
            }
            result => result,
        }
    }
}

#[async_trait]
impl UserActionEventSink for RedisUserActionSequenceStore {
    async fn record(&self, action: &ValidatedUserAction) -> Result<RecordOutcome, String> {
        let now = current_time_ms();
        let cutoff = now.saturating_sub(duration_ms(self.window));
        if action.action_time_ms < cutoff {
            return Ok(RecordOutcome::Skipped(SkipReason::OutsideWindow));
        }
        if action.action_time_ms > now.saturating_add(duration_ms(self.max_future_skew)) {
            return Ok(RecordOutcome::Skipped(SkipReason::FutureTimestamp));
        }
        let payload = serde_json::to_string(&StoredUserAction::from_validated(action))
            .map_err(|e| format!("serialize UAS action: {e}"))?;
        let key = self.key(action.user_id);
        let mut pipeline = redis::pipe();
        pipeline
            .atomic()
            .cmd("ZADD")
            .arg(&key)
            .arg(action.action_time_ms)
            .arg(payload)
            // Drop what fell out of the window; `(` keeps an action exactly
            // at the cutoff, matching the inclusive lower bound of the read.
            .cmd("ZREMRANGEBYSCORE")
            .arg(&key)
            .arg("-inf")
            .arg(format!("({cutoff}"))
            // Keep only the `max_actions` highest scores (newest actions).
            .cmd("ZREMRANGEBYRANK")
            .arg(&key)
            .arg(0)
            .arg(-(self.max_actions as i64) - 1);
        match self.ttl_secs {
            Some(ttl) => {
                pipeline.cmd("EXPIRE").arg(&key).arg(ttl as i64);
            }
            None => {
                pipeline.cmd("PERSIST").arg(&key);
            }
        }
        tokio::time::timeout(self.request_timeout, self.run_idempotent::<()>(&pipeline))
            .await
            .map_err(|_| "UAS Redis write timed out".to_string())?
            .map_err(|e| redis_error("UAS Redis write", &e))?;
        Ok(RecordOutcome::Stored)
    }
}

#[async_trait]
impl UserActionSequenceOps for RedisUserActionSequenceStore {
    async fn get_by_user_id(
        &self,
        user_id: UserId,
    ) -> Result<uas_compat::UserActionSequence, anyhow::Error> {
        let now = current_time_ms();
        let cutoff = now.saturating_sub(duration_ms(self.window));
        let mut pipeline = redis::pipe();
        // Newest first, so LIMIT keeps the most recent actions when the stored
        // set is larger than this reader's bound (for example when the
        // projection job runs with a higher UAS_MAX_ACTIONS). The result is
        // reversed back into time order below.
        pipeline
            .cmd("ZREVRANGEBYSCORE")
            .arg(self.key(user_id))
            .arg(now)
            .arg(cutoff)
            .arg("LIMIT")
            .arg(0)
            .arg(self.max_actions as i64);
        let (members,): (Vec<String>,) =
            tokio::time::timeout(self.request_timeout, self.run_idempotent(&pipeline))
                .await
                .map_err(|_| anyhow::anyhow!("UAS Redis read timed out"))?
                .map_err(|e| anyhow::anyhow!("{}", redis_error("UAS Redis read", &e)))?;

        let mut actions = Vec::with_capacity(members.len());
        let mut undecodable = 0usize;
        for member in members.iter().rev() {
            match decode_stored_member(member) {
                Ok(action) => actions.push(action),
                Err(error) => {
                    undecodable += 1;
                    log::debug!("skipping UAS member for user {user_id}: {error}");
                }
            }
        }
        if undecodable > 0 {
            // One line per read, not per member: a bad member can stay in
            // Redis for the whole window and the user may request often.
            log::warn!(
                "skipped {undecodable} undecodable UAS members for user {user_id}; {} usable actions remain",
                actions.len()
            );
        }
        // The projection stores no publish time of its own (that would make
        // replayed members differ), so both metadata fields approximate it
        // with the newest action time.
        let last_modified = actions
            .iter()
            .filter_map(|action| action.action_time_ms)
            .max()
            .unwrap_or(0);
        Ok(uas_compat::UserActionSequence {
            metadata: Some(uas_compat::UserActionSequenceMeta {
                last_modified_epoch_ms: Some(last_modified),
                last_kafka_publish_epoch_ms: Some(last_modified),
            }),
            user_actions: Some(actions),
        })
    }
}

fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

fn duration_ms(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn connection_replaced(error: &redis::RedisError) -> bool {
    !error.is_timeout() && (error.is_io_error() || error.is_unrecoverable_error())
}

fn redis_error(operation: &str, error: &redis::RedisError) -> String {
    // Redis errors may carry connection details. Reporting only the kind keeps
    // credentials from a configured URL out of logs and API responses.
    format!("{operation} failed ({:?})", error.kind())
}

/// 禁用的 UAS 降级实现。
///
/// 返回空行为序列。装配层在 `UasConfig::Disabled` 时使用它，也用于测试桩；
/// 空序列会让 Phoenix 召回/精排整体跳过、走规则兜底打分。
pub struct DisabledUserActionSequenceFetcher;

impl DisabledUserActionSequenceFetcher {
    /// 创建 UAS Fetcher
    ///
    /// 原始实现在此处初始化到 UAS 存储服务的连接
    /// （通常是 Manhattan KV 或类似的分布式存储）。
    pub fn new() -> Result<Self, anyhow::Error> {
        Ok(Self)
    }
}

#[async_trait]
impl UserActionSequenceOps for DisabledUserActionSequenceFetcher {
    async fn get_by_user_id(
        &self,
        _user_id: UserId,
    ) -> Result<uas_compat::UserActionSequence, anyhow::Error> {
        // Stub: 返回空的行为序列
        // 这意味着 Phoenix 模型将无法使用个性化行为特征，
        // 但管道仍然可以正常运行（使用默认权重打分）
        Ok(uas_compat::UserActionSequence {
            metadata: Some(uas_compat::UserActionSequenceMeta {
                last_modified_epoch_ms: Some(0),
                last_kafka_publish_epoch_ms: Some(0),
            }),
            user_actions: Some(vec![]),
        })
    }
}

/// 演示环境 UAS 获取器
///
/// 返回一段合成的行为序列：过去 6 小时内浏览/互动过 32 条帖子，
/// 帖子 ID 用 Snowflake 格式合成，作者在演示账号集合中轮转。
/// 没有行为序列时 Phoenix 召回/精排会被整体跳过，
/// 所以这是打通模型链路的必要输入。由装配层在 `HOME_MIXER_MODE=demo` 时注入。
pub struct DemoUserActionSequenceFetcher;

#[async_trait]
impl UserActionSequenceOps for DemoUserActionSequenceFetcher {
    async fn get_by_user_id(
        &self,
        _user_id: UserId,
    ) -> Result<uas_compat::UserActionSequence, anyhow::Error> {
        let now_ms = x_algorithm_proto::demo::now_ms();
        let count = 32;
        let step_ms = 6 * 60 * 60 * 1000 / count;

        let user_actions = (0..count)
            .map(|i| {
                let action_time_ms = now_ms - (count - i) * step_ms;
                let ts_secs = u32::try_from((action_time_ms / 1000).max(0)).unwrap_or(0);
                let authors = x_algorithm_proto::demo::DEMO_AUTHOR_IDS;
                let author = authors[(i as usize) % authors.len()];
                uas_compat::UserAction {
                    tweet_id: Some(ObjectId::from_parts(ts_secs, 1000 + i as u64)),
                    author_id: Some(ObjectId::from_u64_be_padded(
                        u64::try_from(author).unwrap_or(0),
                    )),
                    action_time_ms: Some(action_time_ms),
                    action_type: Some(1),
                }
            })
            .collect();

        Ok(uas_compat::UserActionSequence {
            metadata: Some(uas_compat::UserActionSequenceMeta {
                last_modified_epoch_ms: Some(now_ms),
                last_kafka_publish_epoch_ms: Some(now_ms),
            }),
            user_actions: Some(user_actions),
        })
    }
}

#[cfg(test)]
mod redis_tests {
    use super::*;

    fn event(action_time_ms: i64, action_type: i32) -> UserActionEvent {
        UserActionEvent {
            user_id: "000000000000000000000007".to_string(),
            tweet_id: "000000000000000000000009".to_string(),
            author_id: "00000000000000000000000b".to_string(),
            action_time_ms,
            action_type,
        }
    }

    #[test]
    fn event_boundary_validates_ids_timestamp_and_action_type() {
        let action = event(1_700_000_000_000, 3).validate().expect("valid event");
        assert_eq!(action.user_id(), crate::models::uid(7));
        assert_eq!(action.tweet_id, crate::models::pid(9));
        assert_eq!(action.author_id, crate::models::uid(11));
        assert_eq!(action.action_time_ms(), 1_700_000_000_000);

        assert!(event(0, 3).validate().is_err(), "non-positive timestamp");
        assert!(event(1, 0).validate().is_err(), "UNSPECIFIED action");
        assert!(
            event(1, MAX_SUPPORTED_ACTION_TYPE + 1).validate().is_err(),
            "action type outside the model mask"
        );
        assert!(event(1, MAX_SUPPORTED_ACTION_TYPE).validate().is_ok());
        let mut nil_user = event(1, 1);
        nil_user.user_id = "000000000000000000000000".to_string();
        assert!(nil_user.validate().is_err(), "nil user id");
        let mut bad_author = event(1, 1);
        bad_author.author_id = "not-an-object-id".to_string();
        assert!(bad_author.validate().is_err(), "malformed author id");
    }

    #[test]
    fn stored_member_layout_is_pinned() {
        // Redelivery relies on byte-identical members. Changing this layout
        // is a schema change and must bump VERSION.
        let action = event(1_700_000_000_000, 3).validate().unwrap();
        let member = serde_json::to_string(&StoredUserAction::from_validated(&action)).unwrap();
        assert_eq!(
            member,
            r#"{"version":1,"tweet_id":"000000000000000000000009","author_id":"00000000000000000000000b","action_time_ms":1700000000000,"action_type":3}"#
        );
        let decoded = decode_stored_member(&member).expect("round trip");
        assert_eq!(decoded.tweet_id, Some(crate::models::pid(9)));
        assert_eq!(decoded.author_id, Some(crate::models::uid(11)));
        assert_eq!(decoded.action_time_ms, Some(1_700_000_000_000));
        assert_eq!(decoded.action_type, Some(3));
    }

    #[test]
    fn undecodable_members_are_reported_individually() {
        for member in [
            "not json",
            r#"{"version":2,"tweet_id":"000000000000000000000009","author_id":"00000000000000000000000b","action_time_ms":1,"action_type":3}"#,
            r#"{"version":1,"tweet_id":"bad","author_id":"00000000000000000000000b","action_time_ms":1,"action_type":3}"#,
            r#"{"version":1,"tweet_id":"000000000000000000000009","author_id":"00000000000000000000000b","action_time_ms":1,"action_type":99}"#,
        ] {
            assert!(decode_stored_member(member).is_err(), "{member}");
        }
    }

    #[test]
    fn redis_config_has_bounded_defaults() {
        let config = RedisUserActionSequenceConfig::new("redis://localhost/");
        assert_eq!(config.max_actions, crate::params::UAS_STORE_MAX_ACTIONS);
        assert_eq!(
            config.window,
            Duration::from_millis(crate::params::UAS_WINDOW_TIME_MS)
        );
        assert_eq!(
            config.max_future_skew,
            Duration::from_millis(crate::params::UAS_MAX_FUTURE_SKEW_MS)
        );
        assert_eq!(config.ttl_secs, Some(DEFAULT_UAS_TTL_SECS));
        assert_eq!(config.key_prefix, DEFAULT_UAS_KEY_PREFIX);
        assert_eq!(config.validate(), Ok(()));
    }

    #[test]
    fn redis_config_rejects_invalid_values_without_echoing_url() {
        let secret_url = "redis://secret-user:secret-password@localhost/";
        let mut config = RedisUserActionSequenceConfig::new(secret_url);
        assert!(!format!("{config:?}").contains("secret-password"));

        config.max_actions = 0;
        let error = config.validate().expect_err("zero limit must fail");
        assert!(!error.contains("secret-password"));

        config.max_actions = 1;
        config.key_prefix = "a{b}".to_string();
        assert!(config.validate().is_err());
        config.key_prefix = "ok".to_string();
        config.ttl_secs = Some(0);
        assert!(config.validate().is_err());
        config.ttl_secs = None;
        config.request_timeout = Duration::ZERO;
        assert!(config.validate().is_err());
        config.request_timeout = Duration::from_millis(1);
        config.max_future_skew = Duration::ZERO;
        assert_eq!(config.validate(), Ok(()), "zero skew tolerance is strict");
        config.url = "invalid://localhost/".to_string();
        assert_eq!(config.validate(), Err("invalid UAS Redis URL".to_string()));
    }
}
