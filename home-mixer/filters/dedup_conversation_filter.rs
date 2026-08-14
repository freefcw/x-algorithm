use crate::models::candidate::{CandidateHelpers, PostCandidate};
use crate::models::query::ScoredPostsQuery;
use std::collections::HashMap;
use xai_candidate_pipeline::filter::{Filter, FilterResult};

/// Keeps only the highest-scored candidate per branch of a conversation tree
pub struct DedupConversationFilter;

impl Filter<ScoredPostsQuery, PostCandidate> for DedupConversationFilter {
    fn filter(
        &self,
        _query: &ScoredPostsQuery,
        candidates: Vec<PostCandidate>,
    ) -> FilterResult<PostCandidate> {
        let mut kept: Vec<PostCandidate> = Vec::new();
        let mut removed: Vec<PostCandidate> = Vec::new();
        let mut best_per_convo: HashMap<u64, (usize, f64)> = HashMap::new();

        for candidate in candidates {
            let conversation_id = get_conversation_id(&candidate);
            let score = candidate.score.unwrap_or(0.0);

            if let Some((kept_idx, best_score)) = best_per_convo.get_mut(&conversation_id) {
                if score > *best_score {
                    let previous = std::mem::replace(&mut kept[*kept_idx], candidate);
                    removed.push(previous);
                    *best_score = score;
                } else {
                    removed.push(candidate);
                }
            } else {
                let idx = kept.len();
                best_per_convo.insert(conversation_id, (idx, score));
                kept.push(candidate);
            }
        }

        FilterResult { kept, removed }
    }
}

/// 无祖先时回落到原帖 ID 而不是自身 ID：转推与该原帖下的回复属于同一会话，
/// 用自身 ID 会让两者落进不同的桶从而双双保留。
fn get_conversation_id(candidate: &PostCandidate) -> u64 {
    candidate
        .ancestors
        .iter()
        .copied()
        .min()
        .unwrap_or_else(|| candidate.get_original_tweet_id())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(tweet_id: u64, score: f64) -> PostCandidate {
        PostCandidate {
            tweet_id,
            score: Some(score),
            ..Default::default()
        }
    }

    #[test]
    fn retweet_and_reply_to_the_same_original_share_one_conversation() {
        let retweet = PostCandidate {
            retweeted_tweet_id: Some(1),
            ..candidate(10, 0.2)
        };
        let reply = PostCandidate {
            ancestors: vec![1],
            ..candidate(11, 0.9)
        };

        let result =
            DedupConversationFilter.filter(&ScoredPostsQuery::default(), vec![retweet, reply]);

        assert_eq!(result.kept.len(), 1);
        assert_eq!(result.kept[0].tweet_id, 11);
        assert_eq!(result.removed.len(), 1);
        assert_eq!(result.removed[0].tweet_id, 10);
    }

    #[test]
    fn unrelated_originals_are_kept_separately() {
        let result = DedupConversationFilter.filter(
            &ScoredPostsQuery::default(),
            vec![candidate(1, 0.1), candidate(2, 0.2)],
        );

        assert_eq!(result.kept.len(), 2);
        assert!(result.removed.is_empty());
    }
}
