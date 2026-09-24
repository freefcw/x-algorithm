//! Home Mixer ports backed by mrpyq — the MP 圈子 backend (feed, relations,
//! content) and this pipeline's only source of business data.
//!
//! Every identity crossing this boundary is a member ("皮"), never an account.
//! mrpyq still carries `account_id`, `user_id` and `user_no` on some messages;
//! none of them is the pipeline's `UserId`. Author identity is
//! `creator_member_id`. See `models::ids::UserId`.
//!
//! `RecommendationDataService` is the feed-side recommendation contract:
//! NETWORK inbox, FALLBACK pool, and first-stage content/eligibility.
//! `ViewerRelationService` adds the viewer half of admission: block, mute and
//! muted keywords. UAS, the follow graph, author profiles and served/feedback
//! state have no mrpyq contract, so those ports stay Disabled.

use crate::clients::in_network_posts_client::{InNetworkPost, InNetworkPostsClient};
use crate::clients::mrpyq_recommendation_data_client::{
    client_from_config_with_calls, CandidatePage, CandidateSource, IneligibleReason,
    MrpyqClientError, MrpyqRecommendationDataClient, MrpyqRecommendationDataConfig,
    RecommendationContent, MAX_FEED_IDS,
};
use crate::clients::mrpyq_viewer_relation_client::{
    viewer_relation_client_from_config_with_calls, MrpyqViewerRelationClient,
};
use crate::clients::strato_client::StratoClient;
use crate::clients::tweet_entity_service_client::TESClient;
use crate::id::{
    EntityKind, IdentityAllocator, IdentityReader, IdentityRegistrationContext, SnowflakeId,
};
use crate::metrics::ClientCallRecorder;
use crate::models::candidate_features::{
    MediaEntities, MediaEntity, MediaInfo, PureCoreData, VideoInfo,
};
use crate::models::ids::{PostId, UserId};
use crate::models::query::ScoredPostsQuery;
use crate::models::user_features::UserFeatures;
use crate::params::{MAX_POST_AGE, MRPYQ_RECALL_BUDGET_MS};
use crate::visibility::models::{Action, DropAction, FilteredReason, SafetyResult};
use crate::visibility::vf_client::{SafetyLevel, TwitterContextViewer, VisibilityFilteringClient};
use anyhow::Context;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, OnceCell};
use tonic::async_trait;

const MAX_LIST_PAGES: usize = 10;
const RECALL_BUDGET: Duration = Duration::from_millis(MRPYQ_RECALL_BUDGET_MS);
const CONTENT_CACHE_TTL: Duration = Duration::from_secs(2);
const CONTENT_CACHE_MAX_ENTRIES: usize = 20_000;

pub struct MrpyqPipelineAdapters {
    pub tes: Arc<dyn TESClient + Send + Sync>,
    pub in_network: Arc<dyn InNetworkPostsClient>,
    pub vf: Arc<dyn VisibilityFilteringClient + Send + Sync>,
    pub strato: Arc<dyn StratoClient + Send + Sync>,
}

/// `Ok(None)` means mrpyq was not configured, which leaves the ports Disabled.
/// A configured-but-unusable backend is an error instead: silently degrading it
/// to Disabled would start the service with an empty feed and no startup
/// signal.
pub fn pipeline_adapters_from_env(
    demo_mode: bool,
) -> anyhow::Result<Option<MrpyqPipelineAdapters>> {
    pipeline_adapters_from_env_with_calls(demo_mode, ClientCallRecorder::default())
}

pub fn pipeline_adapters_from_env_with_calls(
    demo_mode: bool,
    calls: ClientCallRecorder,
) -> anyhow::Result<Option<MrpyqPipelineAdapters>> {
    if demo_mode {
        return Ok(None);
    }
    let config = MrpyqRecommendationDataConfig::from_env()
        .context("invalid mrpyq recommendation data config")?;
    if config.address.is_none() {
        return Ok(None);
    }
    let relations = viewer_relation_client_from_config_with_calls(&config, calls.clone())
        .context("failed to create mrpyq viewer relation client")?;
    let client = client_from_config_with_calls(config, calls)
        .context("failed to create mrpyq recommendation data client")?;
    log::info!(
        "using mrpyq RecommendationDataService for TES / in-network / fallback / first-stage eligibility, and ViewerRelationService for block / mute"
    );
    Ok(Some(pipeline_adapters(client, relations)))
}

pub fn pipeline_adapters(
    client: Arc<dyn MrpyqRecommendationDataClient>,
    relations: Arc<dyn MrpyqViewerRelationClient>,
) -> MrpyqPipelineAdapters {
    let cache = Arc::new(ContentCache::new(Arc::clone(&client)));
    MrpyqPipelineAdapters {
        tes: Arc::new(MrpyqTESClient::with_cache(Arc::clone(&cache))),
        in_network: Arc::new(MrpyqInNetworkPostsClient::new(client)),
        vf: Arc::new(MrpyqFirstStageEligibilityClient::with_cache(cache)),
        strato: Arc::new(MrpyqStratoClient::new(relations)),
    }
}

/// Reverses internal numeric IDs back to external ObjectIds at the mrpyq
/// boundary. An unmapped ID fails the whole batch: a guessed string would hit
/// mrpyq's stores as an ID nobody allocated.
async fn reverse_external_ids(
    identity: &dyn IdentityReader,
    kind: EntityKind,
    ids: impl IntoIterator<Item = u64>,
) -> Result<Vec<String>, String> {
    let pairs = ids
        .into_iter()
        .map(|id| {
            SnowflakeId::new(id)
                .map(|snowflake| (snowflake, kind))
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    identity
        .reverse_batch(&pairs)
        .await
        .map_err(|error| error.to_string())
}

/// Shares one hydration pass between the TES and VF ports.
///
/// Home Mixer polls its candidate hydrators as a single concurrent stage, so
/// core data, media entities and both visibility groups ask for the same feeds
/// at the same time. mrpyq answers all of them from one
/// `BatchGetRecommendationContents`, whose `BatchGetFeeds` fans out one store
/// read per ID, so hydrating each port on its own multiplies that fan-out by
/// the number of ports.
///
/// Concurrent ports within one request asking for the same feeds share one
/// fetch through an in-flight cell rather than through the state lock, so the
/// lock is only ever held to inspect or update the map — never across an RPC.
/// A `Weak` handle keeps a failed fetch out of the cache: the last waiter drops
/// the cell and the next call starts a new one.
struct ContentCache {
    client: Arc<dyn MrpyqRecommendationDataClient>,
    state: Mutex<CacheState>,
}

/// Result of one hydration pass, shared by everyone who joined it.
type FetchCell = OnceCell<Result<Arc<Vec<(PostId, HydratedContent)>>, MrpyqClientError>>;

#[derive(Clone, Eq, Hash, PartialEq)]
struct InflightKey {
    request_identity: usize,
    missing: Vec<PostId>,
}

/// A fetched feed plus its author resolved into the internal numeric domain.
/// Author resolution is part of the fetch: `creator_member_id` is an external
/// ObjectId, so ports downstream only ever see numeric IDs.
#[derive(Clone)]
struct HydratedContent {
    content: RecommendationContent,
    author_id: Option<UserId>,
}

#[derive(Default)]
struct CacheState {
    /// `None` records a feed mrpyq did not return, so a missing feed is not
    /// re-requested by every port.
    contents: HashMap<PostId, Option<HydratedContent>>,
    /// Keyed by request identity and the sorted feed set a caller is about to
    /// fetch. Ports within one request join one RPC batch, while independent
    /// requests never inherit the initializing request's deadline or stats.
    inflight: HashMap<InflightKey, Weak<FetchCell>>,
    filled_at: Option<Instant>,
}

impl CacheState {
    fn evict_if_stale(&mut self) {
        if self
            .filled_at
            .is_some_and(|at| at.elapsed() > CONTENT_CACHE_TTL)
            || self.contents.len() > CONTENT_CACHE_MAX_ENTRIES
        {
            self.contents.clear();
            self.filled_at = None;
        }
    }
}

impl ContentCache {
    fn new(client: Arc<dyn MrpyqRecommendationDataClient>) -> Self {
        Self {
            client,
            state: Mutex::new(CacheState::default()),
        }
    }

    async fn contents_by_id(
        &self,
        post_ids: &[PostId],
        identity: Arc<IdentityRegistrationContext>,
    ) -> Result<HashMap<PostId, HydratedContent>, MrpyqClientError> {
        let mut missing = {
            let mut state = self.state.lock().await;
            state.evict_if_stale();
            let mut queued = HashSet::new();
            post_ids
                .iter()
                .filter(|id| !state.contents.contains_key(id) && queued.insert(**id))
                .copied()
                .collect::<Vec<_>>()
        };
        missing.sort_unstable();

        if !missing.is_empty() {
            // IdentityContext owns the request deadline, request-local cache
            // and stats. Its Arc allocation stays alive through `identity`,
            // so the address is stable for the lifetime of this in-flight
            // operation without retaining request state in the process cache.
            let request_identity = identity.reader();
            let inflight_key = InflightKey {
                request_identity: Arc::as_ptr(&request_identity) as usize,
                missing: missing.clone(),
            };
            let cell = {
                let mut state = self.state.lock().await;
                // A caller can be cancelled while awaiting `get_or_init`,
                // before the normal removal below runs. Since request identity
                // is part of the key, a later request would not overwrite that
                // dead entry; prune abandoned weak cells on every miss.
                state
                    .inflight
                    .retain(|_, existing| existing.strong_count() > 0);
                match state.inflight.get(&inflight_key).and_then(Weak::upgrade) {
                    Some(cell) => cell,
                    None => {
                        let cell = Arc::new(FetchCell::new());
                        state
                            .inflight
                            .insert(inflight_key.clone(), Arc::downgrade(&cell));
                        cell
                    }
                }
            };

            // Chunks cover disjoint ID sets, so they run concurrently: the
            // hydration budget in `TES_REQUEST_TIMEOUT_MS` covers the whole
            // call, not one chunk at a time.
            let fetched = cell
                .get_or_init(|| async {
                    let started = Instant::now();
                    let chunks = missing.chunks(MAX_FEED_IDS).len();
                    let result = self.fetch_contents(&missing, Arc::clone(&identity)).await;
                    match &result {
                        Ok(contents) => log::info!(
                            "mrpyq adapter hydrate elapsed_ms={} missing={} chunks={} contents={}",
                            started.elapsed().as_millis(),
                            missing.len(),
                            chunks,
                            contents.len(),
                        ),
                        Err(error) => log::warn!(
                            "mrpyq adapter hydrate elapsed_ms={} missing={} chunks={} error={error}",
                            started.elapsed().as_millis(),
                            missing.len(),
                            chunks,
                        ),
                    }
                    result
                })
                .await
                .clone();

            let mut state = self.state.lock().await;
            if state
                .inflight
                .get(&inflight_key)
                .and_then(Weak::upgrade)
                .is_some_and(|cached| Arc::ptr_eq(&cached, &cell))
            {
                state.inflight.remove(&inflight_key);
            }

            let fetched = fetched?;
            state.filled_at.get_or_insert_with(Instant::now);
            state.contents.extend(missing.iter().map(|id| (*id, None)));
            state.contents.extend(
                fetched
                    .iter()
                    .cloned()
                    .map(|(id, content)| (id, Some(content))),
            );
        }

        let state = self.state.lock().await;
        Ok(post_ids
            .iter()
            .filter_map(|id| Some((*id, state.contents.get(id)?.clone()?)))
            .collect())
    }

    /// Fetches one disjoint set of internal post IDs: reversed to external
    /// ObjectIds for mrpyq, then validated back into the requested set — a
    /// `feed_id` the registry did not hand out for this batch is dropped rather
    /// than trusted. `creator_member_id` is resolved in one batch for the whole
    /// page; feeds whose author cannot be represented still hydrate, they just
    /// carry `author_id: None` like a missing feed does.
    async fn fetch_contents(
        &self,
        missing: &[PostId],
        identity: Arc<IdentityRegistrationContext>,
    ) -> Result<Arc<Vec<(PostId, HydratedContent)>>, MrpyqClientError> {
        let reader: &dyn IdentityReader = identity.as_ref();
        let external = reverse_external_ids(reader, EntityKind::Post, missing.iter().copied())
            .await
            .map_err(MrpyqClientError::Unavailable)?;
        let external_to_internal: HashMap<crate::models::ObjectId, PostId> = missing
            .iter()
            .copied()
            .zip(external.iter())
            .filter_map(|(internal, raw)| {
                crate::models::ObjectId::parse(raw)
                    .ok()
                    .map(|id| (id, internal))
            })
            .collect();
        let pages = futures::future::try_join_all(
            external
                .chunks(MAX_FEED_IDS)
                .map(|chunk| self.client.batch_get_contents(chunk.to_vec())),
        )
        .await?;
        let fetched: Vec<(PostId, RecommendationContent)> = pages
            .concat()
            .into_iter()
            .filter_map(|content| {
                let Some(feed) = crate::models::ObjectId::parse(&content.feed_id)
                    .ok()
                    .filter(|id| !id.is_nil())
                else {
                    log::warn!(
                        "dropping mrpyq content with invalid feed_id {}",
                        content.feed_id
                    );
                    return None;
                };
                match external_to_internal.get(&feed) {
                    Some(internal) => Some((*internal, content)),
                    None => {
                        log::warn!(
                            "dropping mrpyq content with unrequested feed_id {}",
                            content.feed_id
                        );
                        None
                    }
                }
            })
            .collect();
        let mut author_ids = HashMap::new();
        let author_requests: Vec<(String, EntityKind)> = fetched
            .iter()
            .filter_map(|(_, content)| {
                crate::models::ObjectId::parse(&content.creator_member_id)
                    .ok()
                    .filter(|id| !id.is_nil())
                    .map(|_| (content.creator_member_id.clone(), EntityKind::User))
            })
            .collect();
        let allocator: &dyn IdentityAllocator = identity.as_ref();
        let resolved = allocator
            .allocate_batch(&author_requests)
            .await
            .map_err(|error| MrpyqClientError::Unavailable(format!("allocate authors: {error}")))?;
        for ((raw, _), snowflake) in author_requests.iter().zip(resolved) {
            author_ids.insert(raw.clone(), snowflake.get());
        }
        Ok(Arc::new(
            fetched
                .into_iter()
                .map(|(id, content)| {
                    let author_id = author_ids.get(&content.creator_member_id).copied();
                    if author_id.is_none() {
                        log::warn!(
                            "mrpyq content {} has an unresolvable creator_member_id {}",
                            content.feed_id,
                            content.creator_member_id
                        );
                    }
                    (id, HydratedContent { content, author_id })
                })
                .collect(),
        ))
    }
}

pub struct MrpyqInNetworkPostsClient {
    client: Arc<dyn MrpyqRecommendationDataClient>,
    recall_budget: Duration,
}

impl MrpyqInNetworkPostsClient {
    pub fn new(client: Arc<dyn MrpyqRecommendationDataClient>) -> Self {
        Self {
            client,
            recall_budget: RECALL_BUDGET,
        }
    }

    #[cfg(test)]
    fn with_recall_budget(mut self, budget: Duration) -> Self {
        self.recall_budget = budget;
        self
    }

    async fn viewer_account_id(&self, query: &ScoredPostsQuery) -> Result<String, String> {
        let identity = query.registration_context();
        let reader: &dyn IdentityReader = identity.as_ref();
        let reversed = reverse_external_ids(reader, EntityKind::User, [query.user_id]).await?;
        Ok(reversed[0].clone())
    }

    async fn list_posts(
        &self,
        identity: &dyn IdentityAllocator,
        account_id: String,
        source: CandidateSource,
        max_results: u32,
    ) -> Result<Vec<InNetworkPost>, String> {
        let started = Instant::now();
        let limit = (max_results as usize).clamp(1, MAX_FEED_IDS.saturating_mul(MAX_LIST_PAGES));
        let deadline = Instant::now() + self.recall_budget;
        let min_created_secs = min_created_at_secs();
        let mut posts: Vec<crate::models::ObjectId> = Vec::new();
        let mut seen = HashSet::new();
        let mut page_token = String::new();
        let mut pages = 0usize;
        let mut ready = true;
        for _ in 0..MAX_LIST_PAGES {
            let remaining = limit.saturating_sub(posts.len());
            if remaining == 0 {
                break;
            }
            if Instant::now() >= deadline {
                log::warn!(
                    "mrpyq {source:?} recall budget exhausted after {} posts ({} ms)",
                    posts.len(),
                    started.elapsed().as_millis()
                );
                break;
            }
            let page_size = remaining.min(MAX_FEED_IDS);
            let page = match self
                .client
                .list_candidates(account_id.clone(), source, page_size, page_token.clone())
                .await
            {
                Ok(page) => {
                    pages += 1;
                    page
                }
                Err(error) if posts.is_empty() => {
                    log::warn!(
                        "mrpyq adapter recall source={source:?} elapsed_ms={} pages=0 posts=0 error={error}",
                        started.elapsed().as_millis(),
                    );
                    return Err(error.to_string());
                }
                Err(error) => {
                    log::warn!(
                        "mrpyq {source:?} candidate page failed after {} posts ({} ms): {error}",
                        posts.len(),
                        started.elapsed().as_millis()
                    );
                    break;
                }
            };
            ready = page.source_ready;
            if !page.source_ready {
                if posts.is_empty() {
                    log::info!(
                        "mrpyq adapter recall source={source:?} elapsed_ms={} pages={pages} posts=0 ready=false",
                        started.elapsed().as_millis(),
                    );
                    return Ok(Vec::new());
                }
                break;
            }
            let saw_stale =
                append_valid_posts(&page, &mut posts, &mut seen, limit, min_created_secs);
            // NETWORK 的 inbox 按发帖时间倒序，第一条超龄候选之后不会再有新帖，
            // 继续翻页只是把 `AgeFilter` 待会儿要丢的东西拉来水合一遍。FALLBACK
            // 池按入池时间排序，老帖也可能排在顶部，不能据此停。
            if saw_stale && source == CandidateSource::Network {
                break;
            }
            if page.next_page_token.is_empty() || page.next_page_token == page_token {
                break;
            }
            page_token = page.next_page_token;
        }
        log::info!(
            "mrpyq adapter recall source={source:?} elapsed_ms={} pages={pages} posts={} ready={ready}",
            started.elapsed().as_millis(),
            posts.len(),
        );
        let requests: Vec<(String, EntityKind)> = posts
            .iter()
            .map(|id| (id.to_string(), EntityKind::Post))
            .collect();
        let resolved = identity
            .allocate_batch(&requests)
            .await
            .map_err(|error| format!("allocate posts: {error}"))?;
        Ok(resolved
            .into_iter()
            .map(|snowflake| InNetworkPost {
                tweet_id: snowflake.get(),
                ..Default::default()
            })
            .collect())
    }
}

/// Oldest creation second a candidate may carry and still survive `AgeFilter`.
///
/// Candidate references arrive unhydrated, so age comes from the ObjectId
/// timestamp rather than `created_at_ms` — the same fallback `TweetMixerSource`
/// uses. A pre-epoch clock yields 0, which filters nothing.
fn min_created_at_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
        .saturating_sub(MAX_POST_AGE)
}

/// mrpyq pages both sources by a score cursor over a Redis ZSET, and the next
/// page re-reads the cursor score inclusively. Feeds sharing that score are
/// therefore returned again on the following page, so page-crossing repeats are
/// expected and must not consume the caller's budget twice.
///
/// Returns whether the page held a candidate `AgeFilter` would drop.
fn append_valid_posts(
    page: &CandidatePage,
    posts: &mut Vec<crate::models::ObjectId>,
    seen: &mut HashSet<crate::models::ObjectId>,
    limit: usize,
    min_created_secs: u64,
) -> bool {
    let mut saw_stale = false;
    for candidate in &page.candidates {
        if posts.len() >= limit {
            break;
        }
        let Some(tweet_id) = parse_object_id(&candidate.feed_id) else {
            log::warn!(
                "dropping mrpyq candidate with invalid feed_id {}",
                candidate.feed_id
            );
            continue;
        };
        if u64::from(tweet_id.timestamp_secs()) < min_created_secs {
            saw_stale = true;
            continue;
        }
        if !seen.insert(tweet_id) {
            continue;
        }
        posts.push(tweet_id);
    }
    saw_stale
}

#[async_trait]
impl InNetworkPostsClient for MrpyqInNetworkPostsClient {
    async fn get_in_network_posts(
        &self,
        query: &ScoredPostsQuery,
        max_results: u32,
    ) -> Result<Vec<InNetworkPost>, String> {
        let identity = query.registration_context();
        let allocator: &dyn IdentityAllocator = identity.as_ref();
        self.list_posts(
            allocator,
            self.viewer_account_id(query).await?,
            CandidateSource::Network,
            max_results,
        )
        .await
    }

    async fn get_fallback_posts(
        &self,
        query: &ScoredPostsQuery,
        max_results: u32,
    ) -> Result<Vec<InNetworkPost>, String> {
        let identity = query.registration_context();
        let allocator: &dyn IdentityAllocator = identity.as_ref();
        self.list_posts(
            allocator,
            self.viewer_account_id(query).await?,
            CandidateSource::Fallback,
            max_results,
        )
        .await
    }
}

pub struct MrpyqTESClient {
    cache: Arc<ContentCache>,
}

impl MrpyqTESClient {
    /// `new` is the service-free test path; production assembly uses
    /// `pipeline_adapters`, which shares the registry-backed identity.
    pub fn new(client: Arc<dyn MrpyqRecommendationDataClient>) -> Self {
        Self::with_cache(Arc::new(ContentCache::new(client)))
    }

    fn with_cache(cache: Arc<ContentCache>) -> Self {
        Self { cache }
    }
}

#[async_trait]
impl TESClient for MrpyqTESClient {
    async fn get_tweet_core_datas(
        &self,
        tweet_ids: Vec<PostId>,
        identity: Arc<IdentityRegistrationContext>,
    ) -> Result<HashMap<PostId, Option<PureCoreData>>, anyhow::Error> {
        let started = Instant::now();
        let requested = tweet_ids.len();
        let by_id = self.cache.contents_by_id(&tweet_ids, identity).await?;
        log::info!(
            "mrpyq adapter tes_core elapsed_ms={} requested={requested} hydrated={}",
            started.elapsed().as_millis(),
            by_id.len(),
        );
        Ok(tweet_ids
            .into_iter()
            .map(|id| {
                (
                    id,
                    by_id.get(&id).and_then(|hydrated| {
                        core_data_from_content(&hydrated.content, hydrated.author_id)
                    }),
                )
            })
            .collect())
    }

    async fn get_tweet_media_entities(
        &self,
        tweet_ids: Vec<PostId>,
        identity: Arc<IdentityRegistrationContext>,
    ) -> Result<HashMap<PostId, Option<MediaEntities>>, anyhow::Error> {
        let started = Instant::now();
        let requested = tweet_ids.len();
        let by_id = self.cache.contents_by_id(&tweet_ids, identity).await?;
        log::info!(
            "mrpyq adapter tes_media elapsed_ms={} requested={requested} hydrated={}",
            started.elapsed().as_millis(),
            by_id.len(),
        );
        Ok(tweet_ids
            .into_iter()
            .map(|id| {
                (
                    id,
                    by_id
                        .get(&id)
                        .map(|hydrated| media_entities_from_content(&hydrated.content)),
                )
            })
            .collect())
    }

    async fn get_subscription_author_ids(
        &self,
        tweet_ids: Vec<PostId>,
    ) -> Result<HashMap<PostId, Option<UserId>>, anyhow::Error> {
        Ok(tweet_ids.into_iter().map(|id| (id, None)).collect())
    }
}

/// Fills the VF port with mrpyq's **first-stage** eligibility flag only.
///
/// `recommendation_eligible` covers deletion, business visibility and text /
/// video audit state. It is a property of the post, identical for every viewer,
/// and `FirstStageEligibleFilter` already consumes the same flag earlier in the
/// pipeline — so mounting this adapter does not add a second, viewer-aware
/// admission decision. `safety_level`, `for_user_id` and the viewer context are
/// accepted to satisfy the port and then ignored, because mrpyq has no
/// viewer-specific verdict to give.
///
/// Viewer-dimension admission comes from `MrpyqStratoClient` (block / mute /
/// muted keywords) instead.
pub struct MrpyqFirstStageEligibilityClient {
    cache: Arc<ContentCache>,
}

impl MrpyqFirstStageEligibilityClient {
    /// `new` is the service-free test path; production assembly uses
    /// `pipeline_adapters`, which shares the registry-backed identity.
    pub fn new(client: Arc<dyn MrpyqRecommendationDataClient>) -> Self {
        Self::with_cache(Arc::new(ContentCache::new(client)))
    }

    fn with_cache(cache: Arc<ContentCache>) -> Self {
        Self { cache }
    }
}

#[async_trait]
impl VisibilityFilteringClient for MrpyqFirstStageEligibilityClient {
    async fn get_result(
        &self,
        tweet_ids: Vec<PostId>,
        _safety_level: SafetyLevel,
        _for_user_id: UserId,
        _context: Option<TwitterContextViewer>,
        identity: Arc<IdentityRegistrationContext>,
    ) -> Result<HashMap<PostId, Option<FilteredReason>>, anyhow::Error> {
        let started = Instant::now();
        let requested = tweet_ids.len();
        let by_id = self.cache.contents_by_id(&tweet_ids, identity).await?;
        let mut results = HashMap::new();
        for id in tweet_ids {
            let Some(hydrated) = by_id.get(&id) else {
                continue;
            };
            results.insert(id, visibility_reason(&hydrated.content));
        }
        log::info!(
            "mrpyq adapter vf elapsed_ms={} requested={requested} decided={}",
            started.elapsed().as_millis(),
            results.len(),
        );
        Ok(results)
    }
}

/// Fills the Strato port with the viewer's block / mute state.
///
/// This is the half of admission that `MrpyqFirstStageEligibilityClient` cannot
/// answer: `AuthorSocialgraphFilter` and `ViewerMutedKeywordFilter` read it off
/// `query.user_features`. The remaining `UserFeatures` fields stay empty —
/// mrpyq has no follow graph, subscription or follower-count contract, and
/// filling them with defaults would be inventing data.
///
/// The viewer sent below is a member, but `GetViewerRelations` still names its
/// parameter `account_id` and answers from account-keyed storage. A failed
/// read (service not shipped, timeout, transport error) now fails closed at
/// `AuthorSocialgraphFilter` / `ViewerMutedKeywordFilter`. A successful empty
/// answer still reads as "blocks nobody", so this port must move to VF and
/// revert to Disabled before mrpyq deploys account-keyed storage against a
/// member-id query — see `docs/implementation/mrpyq-member-dimension-requirements.md` §6.1.
pub struct MrpyqStratoClient {
    client: Arc<dyn MrpyqViewerRelationClient>,
}

impl MrpyqStratoClient {
    pub fn new(client: Arc<dyn MrpyqViewerRelationClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl StratoClient for MrpyqStratoClient {
    async fn get_user_features(
        &self,
        user_id: UserId,
        identity: Arc<IdentityRegistrationContext>,
    ) -> Result<Vec<u8>, anyhow::Error> {
        self.get_user_features_with_identity_inner(user_id, identity.as_ref())
            .await
    }

    async fn store_request_info(
        &self,
        _user_id: UserId,
        _post_ids: Vec<PostId>,
    ) -> Result<Vec<u8>, anyhow::Error> {
        anyhow::bail!("mrpyq has no served-state write contract")
    }
}

impl MrpyqStratoClient {
    async fn get_user_features_with_identity_inner(
        &self,
        user_id: UserId,
        identity: &dyn crate::id::IdentityIngress,
    ) -> Result<Vec<u8>, anyhow::Error> {
        let started = Instant::now();
        let account_id = reverse_external_ids(identity, EntityKind::User, [user_id])
            .await
            .map_err(anyhow::Error::msg)?
            .remove(0);
        let relations = match self.client.get_viewer_relations(account_id).await {
            Ok(relations) => relations,
            Err(error) => {
                log::warn!(
                    "mrpyq adapter viewer_features elapsed_ms={} error={error:#}",
                    started.elapsed().as_millis(),
                );
                return Err(error);
            }
        };
        let blocked = self.member_requests(relations.blocked_account_ids, "blocked");
        let blocked_by = self.member_requests(relations.blocked_by_account_ids, "blocked_by");
        let muted = self.member_requests(relations.muted_account_ids, "muted");
        let mut all = Vec::with_capacity(blocked.len() + blocked_by.len() + muted.len());
        all.extend(blocked.iter().cloned());
        all.extend(blocked_by.iter().cloned());
        all.extend(muted.iter().cloned());
        let resolved = identity.allocate_batch(&all).await?;
        let mut offset = 0;
        let take = |resolved: &[SnowflakeId], offset: &mut usize, len: usize| {
            let result = resolved[*offset..*offset + len]
                .iter()
                .map(|id| id.get())
                .collect::<Vec<_>>();
            *offset += len;
            result
        };
        let features = UserFeatures {
            muted_keywords: relations.muted_keywords,
            blocked_user_ids: take(&resolved, &mut offset, blocked.len()),
            blocked_by_user_ids: take(&resolved, &mut offset, blocked_by.len()),
            muted_user_ids: take(&resolved, &mut offset, muted.len()),
            ..Default::default()
        };
        log::info!(
            "mrpyq adapter viewer_features elapsed_ms={} blocked={} blocked_by={} muted={} keywords={}",
            started.elapsed().as_millis(),
            features.blocked_user_ids.len(),
            features.blocked_by_user_ids.len(),
            features.muted_user_ids.len(),
            features.muted_keywords.len(),
        );
        Ok(serde_json::to_vec(&features)?)
    }

    fn member_requests(&self, raw: Vec<String>, relation: &str) -> Vec<(String, EntityKind)> {
        raw.into_iter()
            .filter_map(|id| {
                let trimmed = id.trim();
                match crate::models::ObjectId::parse(trimmed) {
                    Ok(parsed) if !parsed.is_nil() => Some((trimmed.to_string(), EntityKind::User)),
                    _ => {
                        log::warn!(
                            "mrpyq {relation} relation has an unusable member ID; not enforced"
                        );
                        None
                    }
                }
            })
            .collect()
    }
}

fn parse_object_id(raw: &str) -> Option<crate::models::ObjectId> {
    crate::models::ObjectId::parse(raw.trim())
        .ok()
        .filter(|id| !id.is_nil())
}

fn core_data_from_content(
    content: &RecommendationContent,
    author_id: Option<UserId>,
) -> Option<PureCoreData> {
    Some(PureCoreData {
        author_id: author_id?,
        text: content.text.clone(),
        created_at_ms: u64::try_from(content.created_at_ms)
            .ok()
            .filter(|ms| *ms > 0),
        recommendation_eligible: Some(content.recommendation_eligible),
        favorite_count: Some(i64::from(content.like_count)),
        reply_count: Some(i64::from(content.comment_count)),
        ..Default::default()
    })
}

fn media_entities_from_content(content: &RecommendationContent) -> MediaEntities {
    let mut entities = Vec::new();
    if content.has_image {
        entities.push(MediaEntity { media_info: None });
    }
    if content.has_video {
        entities.push(MediaEntity {
            media_info: Some(MediaInfo::VideoInfo(VideoInfo {
                duration_millis: content.video_duration_ms,
            })),
        });
    }
    entities
}

fn visibility_reason(content: &RecommendationContent) -> Option<FilteredReason> {
    if content.recommendation_eligible {
        return None;
    }
    let reason = if content.ineligible_reason == IneligibleReason::Unspecified {
        "ineligible"
    } else {
        content.ineligible_reason.as_str()
    };
    Some(FilteredReason::SafetyResult(SafetyResult {
        action: Action::Drop(DropAction {
            reason_code: content.ineligible_reason.reason_code(),
            description: reason.to_string(),
        }),
        description: Some(reason.to_string()),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::mrpyq_recommendation_data_client::MrpyqClientError;
    use crate::clients::mrpyq_viewer_relation_client::{
        DisabledMrpyqViewerRelationClient, ViewerRelations,
    };
    use crate::id::{IdentityAllocator, IdentityReader};
    use crate::models::ids::ObjectId;
    use crate::models::{pid, uid};
    use std::sync::Mutex;

    /// External ObjectId string for a fixture numeric ID.
    fn ext(n: u64) -> String {
        ObjectId::from_u64_be_padded(n).to_string()
    }

    /// In-network fixtures carry real timestamp ObjectIds, which the padded
    /// resolver cannot express; this test double assigns fresh numerics and
    /// remembers them so reverse round-trips. Unknown reverse lookups fall back
    /// to the padded form so the viewer path needs no fixture setup.
    #[derive(Default)]
    struct TestResolver {
        state: Mutex<TestResolverState>,
    }

    #[derive(Default)]
    struct TestResolverState {
        next: u64,
        fwd: HashMap<(EntityKind, String), u64>,
        rev: HashMap<u64, String>,
        reverse_calls: usize,
        allocate_batches: Vec<Vec<(String, EntityKind)>>,
    }

    impl TestResolver {
        fn numeric(&self, kind: EntityKind, object_id: ObjectId) -> u64 {
            self.state.lock().expect("resolver").fwd[&(kind, object_id.to_string())]
        }

        fn reverse_call_count(&self) -> usize {
            self.state.lock().expect("resolver").reverse_calls
        }

        fn allocate_batch_count(&self) -> usize {
            self.state.lock().expect("resolver").allocate_batches.len()
        }

        fn last_allocate_batch_len(&self) -> usize {
            self.state
                .lock()
                .expect("resolver")
                .allocate_batches
                .last()
                .map_or(0, Vec::len)
        }
    }

    #[async_trait]
    impl IdentityReader for TestResolver {
        async fn resolve_batch(
            &self,
            ids: &[(String, EntityKind)],
        ) -> anyhow::Result<Vec<SnowflakeId>> {
            let mut state = self.state.lock().expect("resolver");
            let mut resolved = Vec::with_capacity(ids.len());
            for (raw, kind) in ids {
                let key = (*kind, raw.clone());
                let value = match state.fwd.get(&key) {
                    Some(value) => *value,
                    None => {
                        state.next += 1;
                        let value = state.next;
                        state.fwd.insert(key, value);
                        state.rev.insert(value, raw.clone());
                        value
                    }
                };
                resolved.push(SnowflakeId::new(value)?);
            }
            Ok(resolved)
        }

        async fn reverse_batch(
            &self,
            ids: &[(SnowflakeId, EntityKind)],
        ) -> anyhow::Result<Vec<String>> {
            let mut state = self.state.lock().expect("resolver");
            state.reverse_calls += 1;
            Ok(ids
                .iter()
                .map(|(id, _)| match state.rev.get(&id.get()) {
                    Some(raw) => raw.clone(),
                    None => ObjectId::from_u64_be_padded(id.get()).to_string(),
                })
                .collect())
        }
    }

    #[async_trait]
    impl IdentityAllocator for TestResolver {
        async fn allocate_batch(
            &self,
            ids: &[(String, EntityKind)],
        ) -> anyhow::Result<Vec<SnowflakeId>> {
            self.state
                .lock()
                .expect("resolver")
                .allocate_batches
                .push(ids.to_vec());
            self.resolve_batch(ids).await
        }
    }

    struct FakeMrpyq {
        pages: Mutex<HashMap<(CandidateSource, String), CandidatePage>>,
        contents: Mutex<Vec<RecommendationContent>>,
        /// Keyed by the page token that should fail, so a test can break the
        /// first page or only a later one.
        list_error: Mutex<HashMap<String, MrpyqClientError>>,
        content_error: Mutex<Option<MrpyqClientError>>,
        content_calls: Mutex<Vec<Vec<String>>>,
        content_delay: Duration,
        list_delay: Duration,
    }

    impl FakeMrpyq {
        fn new() -> Self {
            Self {
                pages: Mutex::new(HashMap::new()),
                contents: Mutex::new(Vec::new()),
                list_error: Mutex::new(HashMap::new()),
                content_error: Mutex::new(None),
                content_calls: Mutex::new(Vec::new()),
                content_delay: Duration::ZERO,
                list_delay: Duration::ZERO,
            }
        }

        fn with_content_error(self, error: MrpyqClientError) -> Self {
            *self.content_error.lock().expect("content error") = Some(error);
            self
        }

        fn with_list_error(self, page_token: &str, error: MrpyqClientError) -> Self {
            self.list_error
                .lock()
                .expect("list error")
                .insert(page_token.to_string(), error);
            self
        }

        fn with_content_delay(mut self, delay: Duration) -> Self {
            self.content_delay = delay;
            self
        }

        fn with_list_delay(mut self, delay: Duration) -> Self {
            self.list_delay = delay;
            self
        }

        fn content_call_count(&self) -> usize {
            self.content_calls.lock().expect("content calls").len()
        }

        fn with_page(self, source: CandidateSource, token: &str, page: CandidatePage) -> Self {
            self.pages
                .lock()
                .expect("pages")
                .insert((source, token.to_string()), page);
            self
        }

        fn with_contents(self, contents: Vec<RecommendationContent>) -> Self {
            *self.contents.lock().expect("contents") = contents;
            self
        }
    }

    #[async_trait]
    impl MrpyqRecommendationDataClient for FakeMrpyq {
        async fn list_candidates(
            &self,
            _account_id: String,
            source: CandidateSource,
            _page_size: usize,
            page_token: String,
        ) -> Result<CandidatePage, MrpyqClientError> {
            if !self.list_delay.is_zero() {
                tokio::time::sleep(self.list_delay).await;
            }
            if let Some(error) = self
                .list_error
                .lock()
                .expect("list error")
                .get(&page_token)
                .cloned()
            {
                return Err(error);
            }
            Ok(self
                .pages
                .lock()
                .expect("pages")
                .get(&(source, page_token))
                .cloned()
                .unwrap_or(CandidatePage {
                    candidates: Vec::new(),
                    next_page_token: String::new(),
                    source_ready: true,
                }))
        }

        async fn batch_get_contents(
            &self,
            feed_ids: Vec<String>,
        ) -> Result<Vec<RecommendationContent>, MrpyqClientError> {
            self.content_calls
                .lock()
                .expect("content calls")
                .push(feed_ids.clone());
            if !self.content_delay.is_zero() {
                tokio::time::sleep(self.content_delay).await;
            }
            if let Some(error) = self.content_error.lock().expect("content error").clone() {
                return Err(error);
            }
            let requested: HashSet<_> = feed_ids.into_iter().collect();
            Ok(self
                .contents
                .lock()
                .expect("contents")
                .iter()
                .filter(|content| requested.contains(&content.feed_id))
                .cloned()
                .collect())
        }
    }

    /// The content-cache tests only drive the TES / VF ports, so they leave the
    /// relation source unconfigured.
    fn content_adapters(client: Arc<dyn MrpyqRecommendationDataClient>) -> MrpyqPipelineAdapters {
        pipeline_adapters(client, Arc::new(DisabledMrpyqViewerRelationClient))
    }

    fn candidate_page(
        source: CandidateSource,
        feed_ids: &[&str],
        next: &str,
        ready: bool,
    ) -> CandidatePage {
        CandidatePage {
            candidates: feed_ids
                .iter()
                .map(|feed_id| {
                    crate::clients::mrpyq_recommendation_data_client::CandidateReference {
                        feed_id: (*feed_id).to_string(),
                        source,
                        source_score: 1,
                    }
                })
                .collect(),
            next_page_token: next.to_string(),
            source_ready: ready,
        }
    }

    /// `list_posts` 按 `MAX_POST_AGE` 裁候选，所以列表 fixture 的 feed_id 必须带
    /// 真实时间戳；`pid(n)` 对应的外部填充 ID 时间戳是 0，只能用在水合路径上。
    fn pid_aged(age_secs: u64, seq: u64) -> ObjectId {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_secs();
        ObjectId::from_parts((now_secs - age_secs) as u32, seq)
    }

    fn fresh_pid(seq: u64) -> ObjectId {
        pid_aged(60, seq)
    }

    fn stale_pid(seq: u64) -> ObjectId {
        pid_aged(MAX_POST_AGE + 3_600, seq)
    }

    fn viewer_query_with_identity(resolver: Arc<TestResolver>) -> ScoredPostsQuery {
        let context = Arc::new(crate::id::IdentityContext::new(
            resolver.clone() as crate::id::SharedIdentityReader
        ));
        let registration = Arc::new(crate::id::IdentityRegistrationContext::new(
            Arc::clone(&context),
            resolver as crate::id::SharedIdentityIngress,
        ));
        ScoredPostsQuery {
            user_id: uid(9),
            ..ScoredPostsQuery::test_default()
        }
        .with_request_identity(context, registration)
    }

    fn viewer_query() -> ScoredPostsQuery {
        ScoredPostsQuery {
            user_id: uid(9),
            ..ScoredPostsQuery::test_default()
        }
    }

    fn test_registration() -> Arc<IdentityRegistrationContext> {
        let resolver = Arc::new(crate::id::PaddedIdentityResolver::new());
        let reader = Arc::new(crate::id::IdentityContext::new(
            resolver.clone() as crate::id::SharedIdentityReader
        ));
        Arc::new(IdentityRegistrationContext::new(
            reader,
            resolver as crate::id::SharedIdentityIngress,
        ))
    }

    fn eligible_content(feed: PostId, author: UserId) -> RecommendationContent {
        RecommendationContent {
            feed_id: ext(feed),
            creator_member_id: ext(author),
            text: "post body".to_string(),
            created_at_ms: 1_700_000_000_000,
            like_count: 4,
            comment_count: 2,
            gift_value: 9,
            has_image: true,
            has_video: true,
            video_duration_ms: 8_000,
            recommendation_eligible: true,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn in_network_maps_valid_ids_skips_invalid_and_stops_when_inbox_not_ready() {
        let (first, second) = (fresh_pid(1), fresh_pid(2));
        let ready = Arc::new(FakeMrpyq::new().with_page(
            CandidateSource::Network,
            "",
            candidate_page(
                CandidateSource::Network,
                &[&first.to_string(), "not-an-id", &second.to_string()],
                "",
                true,
            ),
        ));
        let resolver = Arc::new(TestResolver::default());
        let posts = MrpyqInNetworkPostsClient::new(ready)
            .get_in_network_posts(&viewer_query_with_identity(Arc::clone(&resolver)), 10)
            .await
            .expect("network posts");
        assert_eq!(
            posts.iter().map(|post| post.tweet_id).collect::<Vec<_>>(),
            vec![
                resolver.numeric(EntityKind::Post, first),
                resolver.numeric(EntityKind::Post, second),
            ]
        );
        assert!(posts.iter().all(|post| post.author_id == 0));

        let not_ready = Arc::new(FakeMrpyq::new().with_page(
            CandidateSource::Network,
            "",
            candidate_page(CandidateSource::Network, &[&first.to_string()], "", false),
        ));
        let empty = MrpyqInNetworkPostsClient::new(not_ready)
            .get_in_network_posts(&viewer_query_with_identity(Arc::clone(&resolver)), 10)
            .await
            .expect("not ready");
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn in_network_uses_the_query_identity_context_for_viewer_and_posts() {
        let feed = fresh_pid(11);
        let fake = Arc::new(FakeMrpyq::new().with_page(
            CandidateSource::Network,
            "",
            candidate_page(CandidateSource::Network, &[&feed.to_string()], "", true),
        ));
        let request_identity = Arc::new(TestResolver::default());
        let context = Arc::new(crate::id::IdentityContext::new(
            request_identity.clone() as crate::id::SharedIdentityReader
        ));
        let registration = Arc::new(crate::id::IdentityRegistrationContext::new(
            Arc::clone(&context),
            request_identity.clone() as crate::id::SharedIdentityIngress,
        ));
        let query = ScoredPostsQuery {
            user_id: uid(9),
            ..ScoredPostsQuery::test_default()
        }
        .with_request_identity(Arc::clone(&context), registration);

        MrpyqInNetworkPostsClient::new(fake)
            .get_in_network_posts(&query, 10)
            .await
            .expect("network posts");

        assert_eq!(request_identity.reverse_call_count(), 1);
        assert_eq!(request_identity.allocate_batch_count(), 1);
        assert_eq!(request_identity.last_allocate_batch_len(), 1);
    }

    #[tokio::test]
    async fn in_network_paginates_until_limit() {
        let (first, second) = (fresh_pid(1), fresh_pid(2));
        let fake = Arc::new(
            FakeMrpyq::new()
                .with_page(
                    CandidateSource::Network,
                    "",
                    candidate_page(CandidateSource::Network, &[&first.to_string()], "p2", true),
                )
                .with_page(
                    CandidateSource::Network,
                    "p2",
                    candidate_page(CandidateSource::Network, &[&second.to_string()], "", true),
                ),
        );
        let resolver = Arc::new(TestResolver::default());
        let posts = MrpyqInNetworkPostsClient::new(fake)
            .get_in_network_posts(&viewer_query_with_identity(Arc::clone(&resolver)), 2)
            .await
            .expect("pages");
        assert_eq!(
            posts.iter().map(|post| post.tweet_id).collect::<Vec<_>>(),
            vec![
                resolver.numeric(EntityKind::Post, first),
                resolver.numeric(EntityKind::Post, second),
            ]
        );
    }

    #[tokio::test]
    async fn tes_maps_content_and_leaves_missing_ids_none() {
        let fake = Arc::new(FakeMrpyq::new().with_contents(vec![eligible_content(pid(1), uid(7))]));
        let tes = MrpyqTESClient::new(fake);
        let core = tes
            .get_tweet_core_datas(vec![pid(1), pid(2)], test_registration())
            .await
            .expect("core");
        let post = core.get(&pid(1)).cloned().flatten().expect("hydrated");
        assert_eq!(post.author_id, uid(7));
        assert_eq!(post.text, "post body");
        assert_eq!(post.created_at_ms, Some(1_700_000_000_000));
        assert_eq!(post.favorite_count, Some(4));
        assert_eq!(post.reply_count, Some(2));
        assert_eq!(post.recommendation_eligible, Some(true));
        assert_eq!(core.get(&pid(2)).cloned().flatten(), None);

        let media = tes
            .get_tweet_media_entities(vec![pid(1)], test_registration())
            .await
            .expect("media");
        let entities = media.get(&pid(1)).cloned().flatten().expect("entities");
        assert_eq!(entities.len(), 2);
        assert!(matches!(
            entities[1].media_info,
            Some(MediaInfo::VideoInfo(VideoInfo {
                duration_millis: 8_000
            }))
        ));
    }

    #[tokio::test]
    async fn tes_content_hydration_uses_request_identity_context() {
        let fake = Arc::new(FakeMrpyq::new().with_contents(vec![eligible_content(pid(1), uid(7))]));
        let client_identity = Arc::new(TestResolver::default());
        let request_identity = Arc::new(TestResolver::default());
        let context = Arc::new(crate::id::IdentityContext::new(
            request_identity.clone() as crate::id::SharedIdentityReader
        ));
        let registration = Arc::new(crate::id::IdentityRegistrationContext::new(
            Arc::clone(&context),
            request_identity.clone() as crate::id::SharedIdentityIngress,
        ));
        let cache = ContentCache::new(fake);
        cache
            .contents_by_id(&[pid(1)], Arc::clone(&registration))
            .await
            .expect("core");
        assert!(request_identity.allocate_batch_count() > 0);
        assert_eq!(client_identity.allocate_batch_count(), 0);
    }

    #[tokio::test]
    async fn vf_first_uses_request_identity_context_on_content_cache_miss() {
        let fake = Arc::new(FakeMrpyq::new().with_contents(vec![eligible_content(pid(1), uid(7))]));
        let client_identity = Arc::new(TestResolver::default());
        let request_identity = Arc::new(TestResolver::default());
        let context = Arc::new(crate::id::IdentityContext::new(
            request_identity.clone() as crate::id::SharedIdentityReader
        ));
        let registration = Arc::new(crate::id::IdentityRegistrationContext::new(
            Arc::clone(&context),
            request_identity.clone() as crate::id::SharedIdentityIngress,
        ));
        let cache = Arc::new(ContentCache::new(fake));
        let vf = MrpyqFirstStageEligibilityClient::with_cache(cache);

        vf.get_result(
            vec![pid(1)],
            SafetyLevel::TimelineHomeRecommendations,
            uid(9),
            None,
            registration,
        )
        .await
        .expect("vf");

        assert!(request_identity.allocate_batch_count() > 0);
        assert_eq!(client_identity.allocate_batch_count(), 0);
    }

    #[tokio::test]
    async fn tes_rejects_non_object_id_author_as_missing() {
        let fake = Arc::new(FakeMrpyq::new().with_contents(vec![RecommendationContent {
            feed_id: ext(1),
            creator_member_id: "member-1".to_string(),
            text: "post".to_string(),
            recommendation_eligible: true,
            ..Default::default()
        }]));
        let core = MrpyqTESClient::new(fake)
            .get_tweet_core_datas(vec![pid(1)], test_registration())
            .await
            .expect("core");
        assert_eq!(core.get(&pid(1)).cloned().flatten(), None);
    }

    #[tokio::test]
    async fn tes_drops_content_with_an_unrequested_feed_id() {
        let fake = Arc::new(FakeMrpyq::new().with_contents(vec![eligible_content(pid(2), uid(7))]));
        let core = MrpyqTESClient::new(fake)
            .get_tweet_core_datas(vec![pid(1)], test_registration())
            .await
            .expect("core");
        assert_eq!(core.get(&pid(1)).cloned().flatten(), None);
    }

    #[tokio::test]
    async fn tes_chunks_content_requests() {
        let fake = Arc::new(FakeMrpyq::new());
        let ids = (1..=201).map(pid).collect::<Vec<_>>();
        MrpyqTESClient::new(Arc::clone(&fake) as Arc<dyn MrpyqRecommendationDataClient>)
            .get_tweet_core_datas(ids, test_registration())
            .await
            .expect("chunked");
        let calls = fake.content_calls.lock().expect("calls").clone();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].len(), MAX_FEED_IDS);
        assert_eq!(calls[1].len(), 1);
    }

    /// A full candidate set needs several chunks, and `TES_REQUEST_TIMEOUT_MS`
    /// budgets the whole hydration rather than one chunk, so chunks must not
    /// add up serially.
    #[tokio::test]
    async fn content_chunks_are_fetched_concurrently() {
        let fake = Arc::new(FakeMrpyq::new().with_content_delay(Duration::from_millis(50)));
        let ids = (1..=600).map(pid).collect::<Vec<_>>();
        let started = Instant::now();
        MrpyqTESClient::new(Arc::clone(&fake) as Arc<dyn MrpyqRecommendationDataClient>)
            .get_tweet_core_datas(ids, test_registration())
            .await
            .expect("chunked");
        let elapsed = started.elapsed();
        assert_eq!(fake.content_call_count(), 3);
        assert!(
            elapsed < Duration::from_millis(100),
            "three chunks ran serially: {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn unrelated_hydrations_do_not_queue_behind_each_other() {
        let fake = Arc::new(FakeMrpyq::new().with_content_delay(Duration::from_millis(50)));
        let adapters =
            content_adapters(Arc::clone(&fake) as Arc<dyn MrpyqRecommendationDataClient>);
        let started = Instant::now();
        let (core, vf) = tokio::join!(
            adapters
                .tes
                .get_tweet_core_datas(vec![pid(1)], test_registration()),
            adapters.vf.get_result(
                vec![pid(2)],
                SafetyLevel::TimelineHomeRecommendations,
                uid(9),
                None,
                test_registration(),
            ),
        );
        core.expect("core");
        vf.expect("vf");
        let elapsed = started.elapsed();
        assert_eq!(fake.content_call_count(), 2);
        assert!(
            elapsed < Duration::from_millis(80),
            "disjoint feed sets serialized on the cache lock: {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn concurrent_callers_for_the_same_feeds_share_one_fetch() {
        let fake = Arc::new(
            FakeMrpyq::new()
                .with_contents(vec![eligible_content(pid(1), uid(7))])
                .with_content_delay(Duration::from_millis(30)),
        );
        let adapters =
            content_adapters(Arc::clone(&fake) as Arc<dyn MrpyqRecommendationDataClient>);
        let registration = test_registration();
        let (core, media) = tokio::join!(
            adapters
                .tes
                .get_tweet_core_datas(vec![pid(1)], Arc::clone(&registration)),
            adapters
                .tes
                .get_tweet_media_entities(vec![pid(1)], Arc::clone(&registration)),
        );
        assert!(core
            .expect("core")
            .get(&pid(1))
            .cloned()
            .flatten()
            .is_some());
        assert!(media
            .expect("media")
            .get(&pid(1))
            .cloned()
            .flatten()
            .is_some());
        assert_eq!(fake.content_call_count(), 1);
        let stats = registration.reader().stats();
        assert_eq!(stats.reverse_batches, 1);
        assert_eq!(stats.allocate_batches, 1);
    }

    #[tokio::test]
    async fn concurrent_requests_for_the_same_feeds_do_not_share_identity_bound_fetch() {
        let fake = Arc::new(
            FakeMrpyq::new()
                .with_contents(vec![eligible_content(pid(1), uid(7))])
                .with_content_delay(Duration::from_millis(30)),
        );
        let adapters =
            content_adapters(Arc::clone(&fake) as Arc<dyn MrpyqRecommendationDataClient>);
        let first_request = test_registration();
        let second_request = test_registration();

        let (first, second) = tokio::join!(
            adapters
                .tes
                .get_tweet_core_datas(vec![pid(1)], Arc::clone(&first_request)),
            adapters.vf.get_result(
                vec![pid(1)],
                SafetyLevel::TimelineHomeRecommendations,
                uid(9),
                None,
                Arc::clone(&second_request),
            ),
        );

        first.expect("first request");
        second.expect("second request");
        assert_eq!(fake.content_call_count(), 2);
        for registration in [first_request, second_request] {
            let stats = registration.reader().stats();
            assert_eq!(stats.reverse_batches, 1);
            assert_eq!(stats.allocate_batches, 1);
        }
    }

    #[tokio::test]
    async fn cancelled_hydration_does_not_leave_a_dead_request_inflight_key() {
        let fake = Arc::new(
            FakeMrpyq::new()
                .with_contents(vec![eligible_content(pid(1), uid(7))])
                .with_content_delay(Duration::from_millis(200)),
        );
        let cache = Arc::new(ContentCache::new(
            Arc::clone(&fake) as Arc<dyn MrpyqRecommendationDataClient>
        ));
        let tes = Arc::new(MrpyqTESClient::with_cache(Arc::clone(&cache)));
        let cancelled_request = test_registration();
        let cancelled = tokio::spawn({
            let tes = Arc::clone(&tes);
            let registration = Arc::clone(&cancelled_request);
            async move { tes.get_tweet_core_datas(vec![pid(1)], registration).await }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while fake.content_call_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled fetch must start");
        cancelled.abort();
        let _ = cancelled.await;

        // Keep the cancelled request identity alive so the replacement request
        // cannot accidentally reuse its address and overwrite the stale key.
        assert_eq!(cache.state.lock().await.inflight.len(), 1);
        tes.get_tweet_media_entities(vec![pid(1)], test_registration())
            .await
            .expect("replacement request");
        assert!(cache.state.lock().await.inflight.is_empty());
    }

    #[tokio::test]
    async fn tes_and_vf_hydrate_a_feed_once_whether_or_not_mrpyq_has_it() {
        for contents in [vec![eligible_content(pid(1), uid(7))], Vec::new()] {
            let fake = Arc::new(FakeMrpyq::new().with_contents(contents));
            let adapters =
                content_adapters(Arc::clone(&fake) as Arc<dyn MrpyqRecommendationDataClient>);
            adapters
                .tes
                .get_tweet_core_datas(vec![pid(1)], test_registration())
                .await
                .expect("core");
            adapters
                .tes
                .get_tweet_media_entities(vec![pid(1)], test_registration())
                .await
                .expect("media");
            adapters
                .vf
                .get_result(
                    vec![pid(1)],
                    SafetyLevel::TimelineHomeRecommendations,
                    uid(9),
                    None,
                    test_registration(),
                )
                .await
                .expect("vf");
            assert_eq!(fake.content_call_count(), 1);
        }
    }

    #[tokio::test]
    async fn a_failed_hydration_is_not_cached_as_a_missing_feed() {
        let fake = Arc::new(
            FakeMrpyq::new()
                .with_contents(vec![eligible_content(pid(1), uid(7))])
                .with_content_error(MrpyqClientError::Timeout),
        );
        let tes = MrpyqTESClient::new(Arc::clone(&fake) as Arc<dyn MrpyqRecommendationDataClient>);
        tes.get_tweet_core_datas(vec![pid(1)], test_registration())
            .await
            .expect_err("timeout must surface");
        *fake.content_error.lock().expect("content error") = None;

        let core = tes
            .get_tweet_core_datas(vec![pid(1)], test_registration())
            .await
            .expect("retry");
        assert!(core.get(&pid(1)).cloned().flatten().is_some());
        assert_eq!(fake.content_call_count(), 2);
    }

    #[tokio::test]
    async fn page_crossing_repeats_do_not_consume_the_candidate_budget() {
        let (first, second) = (fresh_pid(1), fresh_pid(2));
        let fake = Arc::new(
            FakeMrpyq::new()
                .with_page(
                    CandidateSource::Network,
                    "",
                    candidate_page(CandidateSource::Network, &[&first.to_string()], "p2", true),
                )
                .with_page(
                    CandidateSource::Network,
                    "p2",
                    candidate_page(
                        CandidateSource::Network,
                        &[&first.to_string(), &second.to_string()],
                        "",
                        true,
                    ),
                ),
        );
        let resolver = Arc::new(TestResolver::default());
        let posts = MrpyqInNetworkPostsClient::new(fake)
            .get_in_network_posts(&viewer_query_with_identity(Arc::clone(&resolver)), 2)
            .await
            .expect("pages");
        assert_eq!(
            posts.iter().map(|post| post.tweet_id).collect::<Vec<_>>(),
            vec![
                resolver.numeric(EntityKind::Post, first),
                resolver.numeric(EntityKind::Post, second),
            ]
        );
    }

    #[tokio::test]
    async fn recall_stops_at_the_page_budget_and_keeps_what_it_read() {
        let (first, second, third) = (fresh_pid(1), fresh_pid(2), fresh_pid(3));
        let fake = Arc::new(
            FakeMrpyq::new()
                .with_list_delay(Duration::from_millis(30))
                .with_page(
                    CandidateSource::Network,
                    "",
                    candidate_page(CandidateSource::Network, &[&first.to_string()], "p2", true),
                )
                .with_page(
                    CandidateSource::Network,
                    "p2",
                    candidate_page(CandidateSource::Network, &[&second.to_string()], "p3", true),
                )
                .with_page(
                    CandidateSource::Network,
                    "p3",
                    candidate_page(CandidateSource::Network, &[&third.to_string()], "", true),
                ),
        );
        let resolver = Arc::new(TestResolver::default());
        let posts = MrpyqInNetworkPostsClient::new(fake)
            .with_recall_budget(Duration::from_millis(50))
            .get_in_network_posts(&viewer_query_with_identity(Arc::clone(&resolver)), 10)
            .await
            .expect("budget exhaustion degrades to the pages already read");
        assert_eq!(
            posts.iter().map(|post| post.tweet_id).collect::<Vec<_>>(),
            vec![
                resolver.numeric(EntityKind::Post, first),
                resolver.numeric(EntityKind::Post, second),
            ]
        );
    }

    #[tokio::test]
    async fn in_network_surfaces_a_first_page_failure() {
        let fake = Arc::new(FakeMrpyq::new().with_list_error("", MrpyqClientError::Timeout));
        let error = MrpyqInNetworkPostsClient::new(fake)
            .get_in_network_posts(&viewer_query(), 10)
            .await
            .expect_err("a failed first page has no partial result to fall back on");
        assert!(error.contains("timed out"), "{error}");
    }

    #[tokio::test]
    async fn in_network_keeps_earlier_pages_when_a_later_page_fails() {
        let first = fresh_pid(1);
        let fake = Arc::new(
            FakeMrpyq::new()
                .with_page(
                    CandidateSource::Network,
                    "",
                    candidate_page(CandidateSource::Network, &[&first.to_string()], "p2", true),
                )
                .with_list_error("p2", MrpyqClientError::Unavailable("down".to_string())),
        );
        let resolver = Arc::new(TestResolver::default());
        let posts = MrpyqInNetworkPostsClient::new(fake)
            .get_in_network_posts(&viewer_query_with_identity(Arc::clone(&resolver)), 10)
            .await
            .expect("a later page failure degrades to the pages already read");
        assert_eq!(
            posts.iter().map(|post| post.tweet_id).collect::<Vec<_>>(),
            vec![resolver.numeric(EntityKind::Post, first)]
        );
    }

    /// inbox 按发帖时间倒序，超龄候选之后不会再有新帖：第二页的新鲜候选不该出现，
    /// 否则说明只做了过滤没有停页。
    #[tokio::test]
    async fn in_network_stops_paging_at_the_first_stale_candidate() {
        let (fresh, stale, unreachable) = (fresh_pid(1), stale_pid(2), fresh_pid(3));
        let fake = Arc::new(
            FakeMrpyq::new()
                .with_page(
                    CandidateSource::Network,
                    "",
                    candidate_page(
                        CandidateSource::Network,
                        &[&fresh.to_string(), &stale.to_string()],
                        "p2",
                        true,
                    ),
                )
                .with_page(
                    CandidateSource::Network,
                    "p2",
                    candidate_page(
                        CandidateSource::Network,
                        &[&unreachable.to_string()],
                        "",
                        true,
                    ),
                ),
        );
        let resolver = Arc::new(TestResolver::default());
        let posts = MrpyqInNetworkPostsClient::new(fake)
            .get_in_network_posts(&viewer_query_with_identity(Arc::clone(&resolver)), 10)
            .await
            .expect("network posts");
        assert_eq!(
            posts.iter().map(|post| post.tweet_id).collect::<Vec<_>>(),
            vec![resolver.numeric(EntityKind::Post, fresh)]
        );
    }

    #[tokio::test]
    async fn vf_allows_eligible_drops_ineligible_and_omits_missing() {
        let fake = Arc::new(FakeMrpyq::new().with_contents(vec![
            eligible_content(pid(1), uid(7)),
            RecommendationContent {
                feed_id: ext(2),
                creator_member_id: ext(8),
                recommendation_eligible: false,
                ineligible_reason: IneligibleReason::Deleted,
                ..Default::default()
            },
        ]));
        let result = MrpyqFirstStageEligibilityClient::new(fake)
            .get_result(
                vec![pid(1), pid(2), pid(3)],
                SafetyLevel::TimelineHomeRecommendations,
                uid(9),
                None,
                test_registration(),
            )
            .await
            .expect("vf");
        assert!(matches!(result.get(&pid(1)), Some(None)));
        assert!(matches!(
            result.get(&pid(2)),
            Some(Some(FilteredReason::SafetyResult(SafetyResult {
                action: Action::Drop(_),
                ..
            })))
        ));
        assert!(!result.contains_key(&pid(3)));
    }

    #[test]
    fn demo_mode_does_not_load_env_backed_mrpyq_adapters() {
        assert!(pipeline_adapters_from_env(true).unwrap().is_none());
    }

    #[tokio::test]
    async fn fallback_uses_fallback_source() {
        let only = fresh_pid(9);
        let fake = Arc::new(FakeMrpyq::new().with_page(
            CandidateSource::Fallback,
            "",
            candidate_page(CandidateSource::Fallback, &[&only.to_string()], "", true),
        ));
        let resolver = Arc::new(TestResolver::default());
        let posts = MrpyqInNetworkPostsClient::new(fake)
            .get_fallback_posts(&viewer_query_with_identity(Arc::clone(&resolver)), 10)
            .await
            .expect("fallback");
        assert_eq!(posts[0].tweet_id, resolver.numeric(EntityKind::Post, only));
    }

    /// 兜底池按入池时间排序，老帖也可能排在顶部，所以超龄候选只能逐条裁掉，
    /// 不能像 NETWORK 那样据此停止翻页。
    #[tokio::test]
    async fn fallback_drops_stale_candidates_without_stopping_the_walk() {
        let (stale, fresh) = (stale_pid(1), fresh_pid(2));
        let fake = Arc::new(
            FakeMrpyq::new()
                .with_page(
                    CandidateSource::Fallback,
                    "",
                    candidate_page(CandidateSource::Fallback, &[&stale.to_string()], "p2", true),
                )
                .with_page(
                    CandidateSource::Fallback,
                    "p2",
                    candidate_page(CandidateSource::Fallback, &[&fresh.to_string()], "", true),
                ),
        );
        let resolver = Arc::new(TestResolver::default());
        let posts = MrpyqInNetworkPostsClient::new(fake)
            .get_fallback_posts(&viewer_query_with_identity(Arc::clone(&resolver)), 10)
            .await
            .expect("fallback");
        assert_eq!(
            posts.iter().map(|post| post.tweet_id).collect::<Vec<_>>(),
            vec![resolver.numeric(EntityKind::Post, fresh)]
        );
    }

    struct FakeRelations(ViewerRelations);

    #[async_trait]
    impl MrpyqViewerRelationClient for FakeRelations {
        async fn get_viewer_relations(
            &self,
            _account_id: String,
        ) -> anyhow::Result<ViewerRelations> {
            Ok(self.0.clone())
        }
    }

    async fn viewer_features(relations: ViewerRelations) -> UserFeatures {
        let raw = MrpyqStratoClient::new(Arc::new(FakeRelations(relations)))
            .get_user_features(uid(7), test_registration())
            .await
            .expect("viewer relations");
        serde_json::from_slice(&raw).expect("user features JSON")
    }

    #[tokio::test]
    async fn strato_port_carries_block_mute_and_keyword_state() {
        let features = viewer_features(ViewerRelations {
            blocked_account_ids: vec![ext(2)],
            blocked_by_account_ids: vec![ext(3)],
            muted_account_ids: vec![ext(4)],
            muted_keywords: vec!["spoiler".to_string()],
        })
        .await;

        assert_eq!(features.blocked_user_ids.len(), 1);
        assert_eq!(features.blocked_by_user_ids.len(), 1);
        assert_eq!(features.muted_user_ids.len(), 1);
        assert_eq!(features.muted_keywords, vec!["spoiler".to_string()]);
        // mrpyq has no follow graph contract; the port must not invent one.
        assert!(features.followed_user_ids.is_empty());
        assert_eq!(features.follower_count, None);
    }

    #[tokio::test]
    async fn strato_uses_one_request_identity_batch_for_all_relation_kinds() {
        let request_identity = Arc::new(TestResolver::default());
        let context = Arc::new(crate::id::IdentityContext::new(
            request_identity.clone() as crate::id::SharedIdentityReader
        ));
        let registration = Arc::new(crate::id::IdentityRegistrationContext::new(
            Arc::clone(&context),
            request_identity.clone() as crate::id::SharedIdentityIngress,
        ));
        let strato = MrpyqStratoClient::new(Arc::new(FakeRelations(ViewerRelations {
            blocked_account_ids: vec![ext(2)],
            blocked_by_account_ids: vec![ext(3)],
            muted_account_ids: vec![ext(4)],
            ..Default::default()
        })));

        let raw = strato
            .get_user_features(uid(7), registration)
            .await
            .expect("viewer relations");
        let features: UserFeatures = serde_json::from_slice(&raw).expect("user features JSON");

        assert_eq!(features.blocked_user_ids.len(), 1);
        assert_eq!(features.blocked_by_user_ids.len(), 1);
        assert_eq!(features.muted_user_ids.len(), 1);
        assert_eq!(request_identity.reverse_call_count(), 1);
        assert_eq!(request_identity.allocate_batch_count(), 1);
        assert_eq!(request_identity.last_allocate_batch_len(), 3);
    }

    #[tokio::test]
    async fn one_unusable_member_id_does_not_discard_the_other_blocks() {
        let features = viewer_features(ViewerRelations {
            blocked_account_ids: vec!["not-an-object-id".to_string(), String::new(), ext(2)],
            ..Default::default()
        })
        .await;

        assert_eq!(features.blocked_user_ids, vec![uid(2)]);
    }

    #[tokio::test]
    async fn a_failed_relation_read_is_not_reported_as_an_empty_block_list() {
        let error = MrpyqStratoClient::new(Arc::new(DisabledMrpyqViewerRelationClient))
            .get_user_features(uid(7), test_registration())
            .await
            .expect_err("an unreachable relation source has no verdict to give");
        assert!(error.to_string().contains("not configured"), "{error}");
    }
}
