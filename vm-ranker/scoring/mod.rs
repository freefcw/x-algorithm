pub mod dpp_model;

use std::sync::Arc;

use log::error;

use x_algorithm_proto::vm_ranker::{RankRequest, RankedCandidate};

use crate::dpp::DppConfig;
use crate::embedding_store::EmbeddingStore;

#[derive(Clone)]
pub struct DppContext {
    pub store: Arc<EmbeddingStore>,
    pub config: DppConfig,
}

pub async fn rank(
    req: RankRequest,
    dpp: Option<&DppContext>,
) -> Result<Vec<RankedCandidate>, String> {
    if let Some(ctx) = dpp {
        if req.value_model_id != "dpp" {
            error!(
                "DPP context provided but value_model_id='{}' is not 'dpp'",
                req.value_model_id
            );
        }
        let mut ctx = ctx.clone();

        if let Some(params) = &req.dpp_params {
            if params.theta != 0.0 {
                ctx.config.theta = params.theta;
            }
            if params.max_selected_rank != 0 {
                ctx.config.max_selected_rank = params.max_selected_rank as usize;
            }
        }

        let dpp_result = tokio::task::spawn_blocking(move || dpp_model::rank(&req, &ctx))
            .await
            .map_err(|e| format!("DPP spawn_blocking failed: {e}"))?;
        return Ok(dpp_result);
    }

    Ok(req
        .candidates
        .iter()
        .map(|c| RankedCandidate {
            tweet_id: c.tweet_id,
            score: c.score.unwrap_or(0.0),
        })
        .collect())
}
