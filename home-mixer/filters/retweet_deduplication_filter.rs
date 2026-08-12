use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use std::collections::HashSet;
use xai_candidate_pipeline::filter::{Filter, FilterResult};

/// Deduplicates retweets, keeping only the first occurrence of a tweet
/// (whether as an original or as a retweet).
pub struct RetweetDeduplicationFilter;

impl Filter<ScoredPostsQuery, PostCandidate> for RetweetDeduplicationFilter {
    fn filter(
        &self,
        _query: &ScoredPostsQuery,
        candidates: Vec<PostCandidate>,
    ) -> FilterResult<PostCandidate> {
        let mut seen_tweet_ids: HashSet<u64> = HashSet::new();
        let mut kept = Vec::new();
        let mut removed = Vec::new();

        for candidate in candidates {
            match candidate.retweeted_tweet_id {
                Some(retweeted_id) => {
                    // Remove if we've already seen this tweet (as original or retweet)
                    if seen_tweet_ids.insert(retweeted_id) {
                        kept.push(candidate);
                    } else {
                        removed.push(candidate);
                    }
                }
                None => {
                    // Mark this original tweet ID as seen so retweets of it get filtered
                    seen_tweet_ids.insert(candidate.tweet_id);
                    kept.push(candidate);
                }
            }
        }

        FilterResult { kept, removed }
    }
}
