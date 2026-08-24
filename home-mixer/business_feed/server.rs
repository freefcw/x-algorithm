use crate::business_feed::model::{
    BusinessCandidate, BusinessFeedOutput, BusinessFeedQuery, CandidateReference, CandidateSource,
};
use crate::business_feed::ranker::{
    BusinessRuleRanker, CANDIDATE_OVERFETCH_FACTOR, DEFAULT_BUSINESS_FEED_PAGE_SIZE,
    MAX_BUSINESS_FEED_PAGE_SIZE, MAX_UPSTREAM_CANDIDATES,
};
use crate::clients::mrpyq_recommendation_data_client::{
    MrpyqClientError, MrpyqRecommendationDataClient,
};
use crate::util::request_util::{current_time_ms, generate_request_id};
use log::{info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tonic::{Request, Response, Status};
use x_algorithm_proto::home_mixer as pb;

pub struct RuleBasedBusinessFeedServer {
    client: Arc<dyn MrpyqRecommendationDataClient>,
    ranker: BusinessRuleRanker,
}

impl RuleBasedBusinessFeedServer {
    pub fn new(client: Arc<dyn MrpyqRecommendationDataClient>) -> Self {
        Self {
            client,
            ranker: BusinessRuleRanker,
        }
    }

    pub async fn get_business_feed(
        &self,
        query: BusinessFeedQuery,
    ) -> Result<BusinessFeedOutput, MrpyqClientError> {
        let upstream_size = query
            .page_size
            .saturating_mul(CANDIDATE_OVERFETCH_FACTOR)
            .min(MAX_UPSTREAM_CANDIDATES);
        let requested_network_page_token = query.network_page_token.clone();
        let (network_result, fallback_result) = tokio::join!(
            self.client.list_candidates(
                query.viewer_account_id.clone(),
                CandidateSource::Network,
                upstream_size,
                query.network_page_token,
            ),
            self.client.list_candidates(
                query.viewer_account_id.clone(),
                CandidateSource::Fallback,
                upstream_size,
                query.fallback_page_token,
            )
        );
        // The fallback pool is the minimum viable source. A transient failure
        // in the optional network inbox must not turn an otherwise safe page
        // into an error; keep its cursor unchanged so a later request can retry.
        let fallback_page = fallback_result?;
        let network_page = match network_result {
            Ok(page) => page,
            Err(error) => {
                warn!("Business Feed network source degraded: {error}");
                crate::business_feed::model::CandidatePage {
                    candidates: Vec::new(),
                    next_page_token: requested_network_page_token,
                    source_ready: false,
                }
            }
        };
        if !fallback_page.source_ready {
            return Err(MrpyqClientError::InvalidResponse(
                "fallback source unexpectedly reported not ready".to_string(),
            ));
        }

        let references = merge_references(
            network_page.source_ready.then_some(network_page.candidates),
            fallback_page.candidates,
        );
        let contents = if references.is_empty() {
            Vec::new()
        } else {
            self.client
                .batch_get_contents(
                    references
                        .iter()
                        .map(|candidate| candidate.feed_id.clone())
                        .collect(),
                )
                .await?
        };
        let references_by_id: HashMap<String, CandidateReference> = references
            .into_iter()
            .map(|candidate| (candidate.feed_id.clone(), candidate))
            .collect();
        let candidates = contents
            .into_iter()
            .filter_map(|content| {
                references_by_id
                    .get(&content.feed_id)
                    .cloned()
                    .map(|reference| BusinessCandidate { reference, content })
            })
            .collect();
        let request_id = format!("business-{}", generate_request_id());
        let items = self.ranker.rank(
            &query.viewer_account_id,
            &query.seen_feed_ids,
            candidates,
            query.page_size,
            current_time_ms(),
        );
        // This records returned recommendations, not client-confirmed exposures.
        info!(
            "Business Feed response - request_id {} returned_items {} network_ready {}",
            request_id,
            items.len(),
            network_page.source_ready
        );
        Ok(BusinessFeedOutput {
            request_id,
            items,
            next_network_page_token: network_page.next_page_token,
            next_fallback_page_token: fallback_page.next_page_token,
        })
    }
}

fn merge_references(
    network: Option<Vec<CandidateReference>>,
    fallback: Vec<CandidateReference>,
) -> Vec<CandidateReference> {
    let network = network.unwrap_or_default();
    let network_ids: HashSet<String> = network
        .iter()
        .map(|candidate| candidate.feed_id.clone())
        .collect();
    let mut network = network.into_iter();
    let mut fallback = fallback
        .into_iter()
        .filter(|candidate| !network_ids.contains(candidate.feed_id.as_str()));
    let mut seen = HashSet::new();
    let mut merged = Vec::with_capacity(MAX_UPSTREAM_CANDIDATES);
    loop {
        let network_candidate = network.next();
        let fallback_candidate = fallback.next();
        if network_candidate.is_none() && fallback_candidate.is_none() {
            break;
        }
        for candidate in [network_candidate, fallback_candidate]
            .into_iter()
            .flatten()
        {
            if seen.insert(candidate.feed_id.clone()) {
                merged.push(candidate);
                if merged.len() == MAX_UPSTREAM_CANDIDATES {
                    return merged;
                }
            }
        }
    }
    merged
}

#[tonic::async_trait]
impl pb::business_feed_service_server::BusinessFeedService for RuleBasedBusinessFeedServer {
    async fn get_business_feed(
        &self,
        request: Request<pb::BusinessFeedQuery>,
    ) -> Result<Response<pb::BusinessFeedResponse>, Status> {
        let request = request.into_inner();
        if request.viewer_account_id.trim().is_empty() {
            return Err(Status::invalid_argument(
                "viewer_account_id must be non-empty",
            ));
        }
        let page_size = if request.page_size == 0 {
            DEFAULT_BUSINESS_FEED_PAGE_SIZE
        } else {
            usize::try_from(request.page_size).unwrap_or(usize::MAX)
        };
        if page_size > MAX_BUSINESS_FEED_PAGE_SIZE {
            return Err(Status::invalid_argument(format!(
                "page_size must be at most {MAX_BUSINESS_FEED_PAGE_SIZE}"
            )));
        }
        let output = RuleBasedBusinessFeedServer::get_business_feed(
            self,
            BusinessFeedQuery {
                viewer_account_id: request.viewer_account_id,
                page_size,
                network_page_token: request.network_page_token,
                fallback_page_token: request.fallback_page_token,
                seen_feed_ids: request.seen_feed_ids,
            },
        )
        .await
        .map_err(client_error_to_status)?;

        Ok(Response::new(pb::BusinessFeedResponse {
            request_id: output.request_id,
            items: output
                .items
                .into_iter()
                .map(|item| pb::BusinessFeedResultItem {
                    feed_id: item.feed_id,
                    creator_account_id: item.creator_account_id,
                    creator_member_id: item.creator_member_id,
                    score: item.score,
                    source: item.source.as_str().to_string(),
                    reason: item.reason,
                    position: u32::try_from(item.position).unwrap_or(u32::MAX),
                })
                .collect(),
            next_network_page_token: output.next_network_page_token,
            next_fallback_page_token: output.next_fallback_page_token,
        }))
    }
}

fn client_error_to_status(error: MrpyqClientError) -> Status {
    match error {
        MrpyqClientError::NotConfigured => Status::failed_precondition(error.to_string()),
        MrpyqClientError::Timeout => Status::deadline_exceeded(error.to_string()),
        MrpyqClientError::Unavailable(_) => Status::unavailable(error.to_string()),
        MrpyqClientError::InvalidResponse(_) => Status::data_loss(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::business_feed::model::{CandidatePage, RecommendationContent};
    use crate::clients::mrpyq_recommendation_data_client::DisabledMrpyqRecommendationDataClient;
    use std::sync::Mutex;
    use x_algorithm_proto::home_mixer::business_feed_service_server::BusinessFeedService;

    struct FakeClient {
        network_ready: bool,
        network_error: bool,
        calls: Mutex<Vec<(CandidateSource, String, usize)>>,
    }

    #[tonic::async_trait]
    impl MrpyqRecommendationDataClient for FakeClient {
        async fn list_candidates(
            &self,
            _account_id: String,
            source: CandidateSource,
            page_size: usize,
            page_token: String,
        ) -> Result<CandidatePage, MrpyqClientError> {
            self.calls
                .lock()
                .expect("calls lock")
                .push((source, page_token, page_size));
            if source == CandidateSource::Network && self.network_error {
                return Err(MrpyqClientError::Unavailable(
                    "network inbox unavailable".to_string(),
                ));
            }
            let (source_ready, candidates, next_page_token) = match source {
                CandidateSource::Network => (
                    self.network_ready,
                    vec![reference("duplicate", source), reference("network", source)],
                    "next-network",
                ),
                CandidateSource::Fallback => (
                    true,
                    vec![
                        reference("duplicate", source),
                        reference("fallback", source),
                    ],
                    "next-fallback",
                ),
            };
            Ok(CandidatePage {
                candidates,
                next_page_token: next_page_token.to_string(),
                source_ready,
            })
        }

        async fn batch_get_contents(
            &self,
            feed_ids: Vec<String>,
        ) -> Result<Vec<RecommendationContent>, MrpyqClientError> {
            Ok(feed_ids
                .into_iter()
                .map(|feed_id| RecommendationContent {
                    creator_account_id: if feed_id == "network" {
                        "viewer".to_string()
                    } else {
                        "account-creator".to_string()
                    },
                    creator_member_id: "member-creator".to_string(),
                    created_at_ms: current_time_ms(),
                    like_count: 1,
                    comment_count: 0,
                    gift_value: 0,
                    recommendation_eligible: feed_id != "duplicate",
                    feed_id,
                })
                .collect())
        }
    }

    fn reference(feed_id: &str, source: CandidateSource) -> CandidateReference {
        CandidateReference {
            feed_id: feed_id.to_string(),
            source,
            source_score: 0,
        }
    }

    #[tokio::test]
    async fn network_not_ready_uses_fallback_and_preserves_object_ids_and_tokens() {
        let client = Arc::new(FakeClient {
            network_ready: false,
            network_error: false,
            calls: Mutex::new(Vec::new()),
        });
        let server = RuleBasedBusinessFeedServer::new(client.clone());
        let response = BusinessFeedService::get_business_feed(
            &server,
            Request::new(pb::BusinessFeedQuery {
                viewer_account_id: "64f123456789abcdef012345".to_string(),
                page_size: 10,
                network_page_token: "network-token".to_string(),
                fallback_page_token: "fallback-token".to_string(),
                seen_feed_ids: vec![],
            }),
        )
        .await
        .expect("fallback response")
        .into_inner();

        assert_eq!(response.items.len(), 1);
        assert_eq!(response.items[0].feed_id, "fallback");
        assert_eq!(response.next_network_page_token, "next-network");
        assert_eq!(response.next_fallback_page_token, "next-fallback");
        assert!(response.request_id.starts_with("business-"));
        assert!(!response.request_id.contains("64f123456789abcdef012345"));
        let calls = client.calls.lock().expect("calls lock");
        assert!(calls.contains(&(CandidateSource::Network, "network-token".to_string(), 30)));
        assert!(calls.contains(&(CandidateSource::Fallback, "fallback-token".to_string(), 30)));
    }

    #[tokio::test]
    async fn network_error_degrades_to_fallback_without_advancing_network_cursor() {
        let server = RuleBasedBusinessFeedServer::new(Arc::new(FakeClient {
            network_ready: true,
            network_error: true,
            calls: Mutex::new(Vec::new()),
        }));

        let output = server
            .get_business_feed(BusinessFeedQuery {
                viewer_account_id: "viewer".to_string(),
                page_size: 10,
                network_page_token: "retry-network-token".to_string(),
                fallback_page_token: String::new(),
                seen_feed_ids: vec![],
            })
            .await
            .expect("fallback survives network failure");

        assert_eq!(output.items.len(), 1);
        assert_eq!(output.items[0].feed_id, "fallback");
        assert_eq!(output.next_network_page_token, "retry-network-token");
        assert_eq!(output.next_fallback_page_token, "next-fallback");
    }

    #[tokio::test]
    async fn configured_slice_deduplicates_and_filters_seen_self_and_ineligible() {
        let server = RuleBasedBusinessFeedServer::new(Arc::new(FakeClient {
            network_ready: true,
            network_error: false,
            calls: Mutex::new(Vec::new()),
        }));
        let output = server
            .get_business_feed(BusinessFeedQuery {
                viewer_account_id: "viewer".to_string(),
                page_size: 10,
                network_page_token: String::new(),
                fallback_page_token: String::new(),
                seen_feed_ids: vec!["fallback".to_string()],
            })
            .await
            .expect("business feed");

        assert!(output.items.is_empty());
    }

    #[tokio::test]
    async fn missing_configuration_maps_to_failed_precondition() {
        let server =
            RuleBasedBusinessFeedServer::new(Arc::new(DisabledMrpyqRecommendationDataClient));
        let error = BusinessFeedService::get_business_feed(
            &server,
            Request::new(pb::BusinessFeedQuery {
                viewer_account_id: "viewer".to_string(),
                ..Default::default()
            }),
        )
        .await
        .expect_err("disabled client");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn merge_prefers_network_duplicate_and_honors_not_ready() {
        let merged = merge_references(
            Some(vec![reference("same", CandidateSource::Network)]),
            vec![reference("same", CandidateSource::Fallback)],
        );
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].source, CandidateSource::Network);

        let fallback_only =
            merge_references(None, vec![reference("same", CandidateSource::Fallback)]);
        assert_eq!(fallback_only[0].source, CandidateSource::Fallback);
    }
}
