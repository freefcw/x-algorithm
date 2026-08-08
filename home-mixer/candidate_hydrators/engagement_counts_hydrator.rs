//! 帖子互动数（点赞 / 回复 / 转发 / 引用）补全，带进程内缓存。
//!
//! 上游用独立 TES API (`get_api_counts`) + MokaCache 按帖子年龄 TTL 缓存。
//! 本地 TESClient 只有 `get_tweet_core_datas`，这里复用该接口提取 counts，
//! 并用进程内 HashMap + RwLock 做简单缓存——同一帖子在 TTL 内不重复调 TES。
//!
//! 当前未接入 pipeline：CoreDataCandidateHydrator 已在无缓存模式下获取 counts。
//! 启用前需要从 CoreDataCandidateHydrator 移除 counts 写入以避免字段所有权冲突。
//! 启用方式：在 phoenix_candidate_pipeline.rs 的 hydrators 列表中注册此 hydrator。

#![allow(dead_code)]

use crate::candidate_pipeline::candidate::PostCandidate;
use crate::candidate_pipeline::query::ScoredPostsQuery;
use crate::clients::tweet_entity_service_client::TESClient;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tonic::async_trait;
use xai_candidate_pipeline::hydrator::{CacheStore, CachedHydrator};

/// 缓存的互动数快照。
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CachedCounts {
    pub favorite_count: Option<i64>,
    pub reply_count: Option<i64>,
    pub repost_count: Option<i64>,
    pub quote_count: Option<i64>,
}

/// 进程内 TTL 缓存，按帖子 ID 索引。
///
/// 简易实现：遍历过期清理。帖子量级在单机 Demo 下足够；
/// 生产规模应替换为 moka 或 Redis（见 P5-B Integration Backlog）。
pub struct TtlCountsCache {
    ttl: Duration,
    entries: std::sync::RwLock<HashMap<u64, (Instant, CachedCounts)>>,
}

impl TtlCountsCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: Default::default(),
        }
    }
}

#[async_trait]
impl CacheStore<u64, CachedCounts> for TtlCountsCache {
    async fn get(&self, key: &u64) -> Option<CachedCounts> {
        let entries = self.entries.read().expect("cache lock");
        let (inserted_at, value) = entries.get(key)?;
        if inserted_at.elapsed() < self.ttl {
            Some(value.clone())
        } else {
            None
        }
    }

    async fn insert(&self, key: u64, value: CachedCounts) {
        let mut entries = self.entries.write().expect("cache lock");
        entries.insert(key, (Instant::now(), value));
    }
}

/// 从 TES 获取帖子互动数并缓存的 hydrator。
///
/// 缓存键为帖子原始 ID（转发帖用源帖 ID），TTL 默认 5 分钟。
/// 新帖和旧帖在上游有不同的 TTL（5min / 10min），本地统一用一个 TTL。
pub struct EngagementCountsHydrator {
    pub tes_client: Arc<dyn TESClient + Send + Sync>,
    cache: Arc<TtlCountsCache>,
}

impl EngagementCountsHydrator {
    pub fn new(tes_client: Arc<dyn TESClient + Send + Sync>) -> Self {
        Self::with_cache(tes_client, Duration::from_secs(5 * 60))
    }

    pub fn with_cache(tes_client: Arc<dyn TESClient + Send + Sync>, ttl: Duration) -> Self {
        Self {
            tes_client,
            cache: Arc::new(TtlCountsCache::new(ttl)),
        }
    }

    fn original_tweet_id(candidate: &PostCandidate) -> u64 {
        candidate
            .retweeted_tweet_id
            .unwrap_or(candidate.tweet_id as u64)
    }
}

#[async_trait]
impl CachedHydrator<ScoredPostsQuery, PostCandidate> for EngagementCountsHydrator {
    type CacheKey = u64;
    type CacheValue = CachedCounts;

    fn enable(&self, _query: &ScoredPostsQuery) -> bool {
        false
    }

    fn cache_store(&self) -> &dyn CacheStore<Self::CacheKey, Self::CacheValue> {
        self.cache.as_ref()
    }

    fn cache_key(&self, candidate: &PostCandidate) -> Self::CacheKey {
        Self::original_tweet_id(candidate)
    }

    fn cache_value(&self, hydrated: &PostCandidate) -> Self::CacheValue {
        CachedCounts {
            favorite_count: hydrated.favorite_count,
            reply_count: hydrated.reply_count,
            repost_count: hydrated.repost_count,
            quote_count: hydrated.quote_count,
        }
    }

    fn hydrate_from_cache(&self, value: Self::CacheValue) -> PostCandidate {
        PostCandidate {
            favorite_count: value.favorite_count,
            reply_count: value.reply_count,
            repost_count: value.repost_count,
            quote_count: value.quote_count,
            ..Default::default()
        }
    }

    async fn hydrate_from_client(
        &self,
        _query: &ScoredPostsQuery,
        candidates: &[PostCandidate],
    ) -> Result<Vec<PostCandidate>, String> {
        let tweet_ids: Vec<i64> = candidates
            .iter()
            .map(|c| Self::original_tweet_id(c) as i64)
            .collect();

        let core_by_tweet = self
            .tes_client
            .get_tweet_core_datas(tweet_ids.clone())
            .await
            .map_err(|e| e.to_string())?;

        Ok(tweet_ids
            .into_iter()
            .map(|tweet_id| {
                let counts = core_by_tweet.get(&tweet_id).and_then(Option::as_ref);
                PostCandidate {
                    favorite_count: counts.and_then(|c| c.favorite_count),
                    reply_count: counts.and_then(|c| c.reply_count),
                    repost_count: counts.and_then(|c| c.repost_count),
                    quote_count: counts.and_then(|c| c.quote_count),
                    ..Default::default()
                }
            })
            .collect())
    }

    fn update(&self, candidate: &mut PostCandidate, hydrated: PostCandidate) {
        candidate.favorite_count = hydrated.favorite_count;
        candidate.reply_count = hydrated.reply_count;
        candidate.repost_count = hydrated.repost_count;
        candidate.quote_count = hydrated.quote_count;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::candidate_pipeline::candidate_features::{MediaEntities, PureCoreData};
    use crate::candidate_pipeline::query::ScoredPostsQuery;

    struct FakeTES {
        counts: HashMap<i64, PureCoreData>,
    }

    #[async_trait]
    impl TESClient for FakeTES {
        async fn get_tweet_core_datas(
            &self,
            tweet_ids: Vec<i64>,
        ) -> Result<HashMap<i64, Option<PureCoreData>>, anyhow::Error> {
            Ok(tweet_ids
                .into_iter()
                .map(|id| (id, self.counts.get(&id).cloned()))
                .collect())
        }

        async fn get_tweet_media_entities(
            &self,
            _tweet_ids: Vec<i64>,
        ) -> Result<HashMap<i64, Option<MediaEntities>>, anyhow::Error> {
            Ok(HashMap::new())
        }

        async fn get_subscription_author_ids(
            &self,
            _tweet_ids: Vec<i64>,
        ) -> Result<HashMap<i64, Option<u64>>, anyhow::Error> {
            Ok(HashMap::new())
        }
    }

    fn make_candidate(tweet_id: i64) -> PostCandidate {
        PostCandidate {
            tweet_id,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn cached_hydrator_reuses_values_within_ttl() {
        let mut counts = HashMap::new();
        counts.insert(
            100,
            PureCoreData {
                favorite_count: Some(42),
                reply_count: Some(3),
                repost_count: Some(1),
                quote_count: Some(0),
                ..Default::default()
            },
        );
        let tes = Arc::new(FakeTES { counts });
        let hydrator = EngagementCountsHydrator::new(tes);

        // 第一次 hydrate：缓存未命中，调 TES
        let first = hydrator
            .hydrate_from_client(&ScoredPostsQuery::default(), &[make_candidate(100)])
            .await
            .expect("first hydration");
        assert_eq!(first[0].favorite_count, Some(42));

        // 写入缓存
        hydrator
            .cache_store()
            .insert(100, hydrator.cache_value(&first[0]))
            .await;

        // 第二次从缓存读
        let cached = hydrator.cache_store().get(&100).await.expect("cache hit");
        assert_eq!(cached.favorite_count, Some(42));
        assert_eq!(cached.reply_count, Some(3));
    }

    #[tokio::test]
    async fn cache_key_uses_original_tweet_id_for_retweets() {
        let tes = Arc::new(FakeTES {
            counts: HashMap::new(),
        });
        let hydrator = EngagementCountsHydrator::new(tes);

        let retweet = PostCandidate {
            tweet_id: 200,
            retweeted_tweet_id: Some(100),
            ..Default::default()
        };
        assert_eq!(hydrator.cache_key(&retweet), 100);
    }

    #[tokio::test]
    async fn enable_returns_false_by_default() {
        let tes = Arc::new(FakeTES {
            counts: HashMap::new(),
        });
        let hydrator = EngagementCountsHydrator::new(tes);
        assert!(!hydrator.enable(&ScoredPostsQuery::default()));
    }
}
