use log::{debug, info, warn};
use rand::Rng;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tonic::{Request, Response, Status};

use x_algorithm_proto::vm_ranker::{
    vm_ranker_service_server::{VmRankerService, VmRankerServiceServer},
    RankRequest, RankResponse, RankedCandidate,
};

use crate::internal_id::SnowflakeId;
use crate::metrics::{
    Timer, IN_FLIGHT_REQUESTS, REJECTED_REQUESTS, SCORE_CANDIDATES_IN, SCORE_DURATION,
    SCORE_ERRORS, SCORE_REQUESTS,
};
use crate::scoring::DppContext;

pub struct VMRankerServiceImpl {
    request_semaphore: Arc<Semaphore>,
    dpp: Option<DppContext>,
}

impl VMRankerServiceImpl {
    pub fn new(max_concurrent_requests: usize, dpp: Option<DppContext>) -> Self {
        info!(
            "Initializing VMRankerService with max_concurrent_requests={}, dpp={}",
            max_concurrent_requests,
            if dpp.is_some() { "enabled" } else { "disabled" },
        );
        Self {
            request_semaphore: Arc::new(Semaphore::new(max_concurrent_requests)),
            dpp,
        }
    }

    pub fn server(self) -> VmRankerServiceServer<Self> {
        VmRankerServiceServer::new(self)
            .max_decoding_message_size(20 * 1024 * 1024)
            .max_encoding_message_size(20 * 1024 * 1024)
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
            .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
            .send_compressed(tonic::codec::CompressionEncoding::Gzip)
            .send_compressed(tonic::codec::CompressionEncoding::Zstd)
    }
}

#[tonic::async_trait]
impl VmRankerService for VMRankerServiceImpl {
    async fn rank(&self, request: Request<RankRequest>) -> Result<Response<RankResponse>, Status> {
        let req = request.into_inner();
        let value_model_id = req.value_model_id.clone();
        let model_id = if value_model_id.is_empty() {
            "unknown"
        } else {
            &value_model_id
        };

        SCORE_REQUESTS.with_label_values(&[model_id]).inc();

        let viewer_id = SnowflakeId::new(req.viewer_id)
            .map_err(|error| Status::invalid_argument(format!("invalid viewer_id: {error}")))?;
        for candidate in &req.candidates {
            SnowflakeId::new(candidate.tweet_id)
                .map_err(|error| Status::invalid_argument(format!("invalid tweet_id: {error}")))?;
            SnowflakeId::new(candidate.author_id)
                .map_err(|error| Status::invalid_argument(format!("invalid author_id: {error}")))?;
            if candidate.retweeted_tweet_id != 0 {
                SnowflakeId::new(candidate.retweeted_tweet_id).map_err(|error| {
                    Status::invalid_argument(format!("invalid retweeted_tweet_id: {error}"))
                })?;
            }
        }

        let _permit = match self.request_semaphore.try_acquire() {
            Ok(permit) => {
                IN_FLIGHT_REQUESTS.inc();
                permit
            }
            Err(_) => {
                REJECTED_REQUESTS.with_label_values(&[model_id]).inc();
                return Err(Status::resource_exhausted(
                    "Server at capacity, please retry",
                ));
            }
        };

        struct InFlightGuard;
        impl Drop for InFlightGuard {
            fn drop(&mut self) {
                IN_FLIGHT_REQUESTS.dec();
            }
        }
        let _guard = InFlightGuard;
        let _timer = Timer::new(SCORE_DURATION.with_label_values(&[model_id]).clone());

        let num_candidates = req.candidates.len();
        SCORE_CANDIDATES_IN
            .with_label_values(&[model_id])
            .observe(num_candidates as f64);
        let result = crate::scoring::rank(req, self.dpp.as_ref())
            .await
            .map_err(|e| {
                warn!(
                    "rank failed: viewer={} value_model_id='{}': {}",
                    viewer_id, model_id, e
                );
                SCORE_ERRORS.with_label_values(&[model_id]).inc();
                Status::internal(e)
            })?;

        if rand::rng().random_ratio(1, 1000) {
            info!(
                "rank: viewer={} model={} candidates={}",
                viewer_id, model_id, num_candidates
            );
        }

        let mut top: Vec<&RankedCandidate> = result.iter().collect();
        top.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        for s in top.iter().take(50) {
            debug!("  tweet={} score={:.6}", s.tweet_id, s.score);
        }

        Ok(Response::new(RankResponse { candidates: result }))
    }
}
