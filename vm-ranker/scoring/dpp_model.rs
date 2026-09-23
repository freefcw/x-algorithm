use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use half::f16;
use log::info;
use rand::Rng;
use x_algorithm_proto::vm_ranker::{RankCandidate, RankRequest, RankedCandidate};

use super::DppContext;
use crate::dpp::{self, DppInput};
use crate::internal_id::SnowflakeId;

fn l2_norm(v: &[f16]) -> f64 {
    v.iter()
        .map(|x| {
            let xf = x.to_f32();
            xf * xf
        })
        .sum::<f32>()
        .sqrt() as f64
}

fn random_unit_embedding(dim: usize) -> Arc<Vec<f16>> {
    let mut rng = rand::rng();
    let v: Vec<f32> = (0..dim)
        .map(|_| rng.random_range(-1.0f32..1.0f32))
        .collect();
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    let emb = if norm > 0.0 {
        v.into_iter().map(|x| f16::from_f32(x / norm)).collect()
    } else {
        let mut u = vec![f16::ZERO; dim];
        u[0] = f16::ONE;
        u
    };
    Arc::new(emb)
}

#[derive(Default)]
struct EmbeddingMemo(HashMap<SnowflakeId, (Arc<Vec<f16>>, bool)>);

impl EmbeddingMemo {
    fn resolve(&mut self, embedding_id: SnowflakeId, ctx: &DppContext) -> (Arc<Vec<f16>>, bool) {
        let (embedding, missing) = self.0.entry(embedding_id).or_insert_with(|| {
            match ctx.store.client.get(embedding_id) {
                Some(embedding) => (embedding, false),
                None => (random_unit_embedding(ctx.store.dim()), true),
            }
        });
        (Arc::clone(embedding), *missing)
    }
}

fn build_dpp_inputs(
    req: &RankRequest,
    ctx: &DppContext,
    memo: &mut EmbeddingMemo,
) -> Vec<DppInput> {
    let max_rank = ctx.config.max_selected_rank;

    let mut scored: Vec<(usize, &RankCandidate, f64)> = req
        .candidates
        .iter()
        .enumerate()
        .filter_map(|(i, c)| c.score.map(|s| (i, c, s)))
        .collect();

    scored.sort_unstable_by(|(i_a, _, s_a), (i_b, _, s_b)| {
        s_b.partial_cmp(s_a)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| i_a.cmp(i_b))
    });

    scored
        .into_iter()
        .take(max_rank)
        .map(|(_, c, score)| {
            let embedding_id = if c.retweeted_tweet_id != 0 {
                c.retweeted_tweet_id
            } else {
                c.tweet_id
            };
            let embedding_id = SnowflakeId::new(embedding_id)
                .expect("RankRequest IDs are validated at the service boundary");
            let (embedding, embedding_missing) = memo.resolve(embedding_id, ctx);
            let norm = if embedding_missing {
                1.0
            } else {
                l2_norm(&embedding)
            };
            DppInput {
                id: SnowflakeId::new(c.tweet_id)
                    .expect("RankRequest IDs are validated at the service boundary"),
                score,
                embedding,
                norm,
                embedding_missing,
            }
        })
        .collect()
}

pub fn rank(req: &RankRequest, ctx: &DppContext) -> Vec<RankedCandidate> {
    let inputs = build_dpp_inputs(req, ctx, &mut EmbeddingMemo::default());
    let viewer_id = SnowflakeId::new(req.viewer_id)
        .expect("RankRequest viewer ID is validated at the service boundary");
    let results = dpp::rescore(&inputs, None, &ctx.config, viewer_id);

    let debug = ctx.config.debug_viewer_id == Some(viewer_id);
    let selected_ids: HashSet<SnowflakeId> = results.iter().map(|r| r.id).collect();

    if debug {
        let mut sorted_inputs: Vec<&DppInput> = inputs.iter().collect();
        sorted_inputs.sort_unstable_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let dropped: Vec<(usize, SnowflakeId)> = sorted_inputs
            .iter()
            .enumerate()
            .filter(|(_, inp)| !selected_ids.contains(&inp.id))
            .map(|(rank, inp)| (rank + 1, inp.id))
            .collect();

        if !dropped.is_empty() {
            let dropped_ids: Vec<String> = dropped.iter().map(|(_, id)| id.to_string()).collect();
            info!(
                "DPP filtered {}/{} posts. Dropped (score_rank, postId): {:?}\nDropped postIds: {}",
                dropped.len(),
                inputs.len(),
                dropped,
                dropped_ids.join(","),
            );
        }

        let limit = sorted_inputs.len().min(50);
        let mut before_log = format!("Top {} BEFORE DPP (rank, postId, score):\n", limit);
        for (rank, inp) in sorted_inputs.iter().enumerate().take(limit) {
            before_log.push_str(&format!(
                "  #{:<3} postId={:<20} score={:.6}\n",
                rank + 1,
                inp.id,
                inp.score
            ));
        }
        info!("{}", before_log);

        let after_limit = results.len().min(50);
        let mut after_log = format!("Top {} AFTER DPP (rank, postId, score):\n", after_limit);
        for (rank, r) in results.iter().enumerate().take(after_limit) {
            after_log.push_str(&format!(
                "  #{:<3} postId={:<20} score={:.6}\n",
                rank + 1,
                r.id,
                r.score
            ));
        }
        info!("{}", after_log);
    }

    req.candidates
        .iter()
        .map(|c| RankedCandidate {
            tweet_id: c.tweet_id,
            score: if selected_ids
                .contains(&SnowflakeId::new(c.tweet_id).expect("validated candidate ID"))
            {
                c.score.unwrap_or(0.0)
            } else {
                0.0
            },
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::dpp::DppConfig;
    use crate::embedding_store::{init_store_with, MmEmbeddingsLookup};

    #[derive(Default)]
    struct CountingMissingLookup {
        calls: AtomicUsize,
    }

    impl MmEmbeddingsLookup for CountingMissingLookup {
        fn get(&self, _id: SnowflakeId) -> Option<Arc<Vec<f16>>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    fn context(lookup: Arc<CountingMissingLookup>) -> DppContext {
        let (store, _) = init_store_with(4, lookup).expect("test store must initialize");
        DppContext {
            store,
            config: DppConfig {
                top_k: 4,
                theta: 0.5,
                max_selected_rank: 4,
                debug_viewer_id: None,
            },
        }
    }

    fn candidate(tweet_id: u64, retweeted_tweet_id: u64, score: f64) -> RankCandidate {
        RankCandidate {
            tweet_id,
            retweeted_tweet_id,
            score: Some(score),
            ..Default::default()
        }
    }

    #[test]
    fn missing_original_embedding_is_shared_by_original_and_retweets() {
        let lookup = Arc::new(CountingMissingLookup::default());
        let ctx = context(Arc::clone(&lookup));
        let req = RankRequest {
            candidates: vec![
                candidate(900, 0, 3.0),
                candidate(901, 900, 2.0),
                candidate(902, 900, 1.0),
            ],
            ..Default::default()
        };

        let inputs = build_dpp_inputs(&req, &ctx, &mut EmbeddingMemo::default());

        assert_eq!(inputs.len(), 3);
        assert!(inputs.iter().all(|input| input.embedding_missing));
        assert!(Arc::ptr_eq(&inputs[0].embedding, &inputs[1].embedding));
        assert!(Arc::ptr_eq(&inputs[0].embedding, &inputs[2].embedding));
        assert_eq!(*inputs[0].embedding, *inputs[1].embedding);
        assert_eq!(lookup.calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn distinct_missing_embedding_ids_do_not_share_fallbacks() {
        let lookup = Arc::new(CountingMissingLookup::default());
        let ctx = context(Arc::clone(&lookup));
        let req = RankRequest {
            candidates: vec![candidate(900, 0, 2.0), candidate(901, 0, 1.0)],
            ..Default::default()
        };

        let inputs = build_dpp_inputs(&req, &ctx, &mut EmbeddingMemo::default());

        assert_eq!(inputs.len(), 2);
        assert!(!Arc::ptr_eq(&inputs[0].embedding, &inputs[1].embedding));
        assert_eq!(lookup.calls.load(Ordering::Relaxed), 2);
    }
}
