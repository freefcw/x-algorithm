use crate::candidate_pipeline::candidate::PostCandidate;
use crate::candidate_pipeline::query::ScoredPostsQuery;
use crate::post_text::{MatchTweetGroup, TokenSequence, TweetTokenizer, UserMutes};
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::filter::{Filter, FilterResult};

pub struct MutedKeywordFilter {
    pub tokenizer: Arc<TweetTokenizer>,
}

impl MutedKeywordFilter {
    pub fn new() -> Self {
        let tokenizer = TweetTokenizer::new();
        Self {
            tokenizer: Arc::new(tokenizer),
        }
    }
}

#[async_trait]
impl Filter<ScoredPostsQuery, PostCandidate> for MutedKeywordFilter {
    fn filter(
        &self,
        query: &ScoredPostsQuery,
        candidates: Vec<PostCandidate>,
    ) -> Result<FilterResult<PostCandidate>, String> {
        let muted_keywords = query.user_features.muted_keywords.clone();

        if muted_keywords.is_empty() {
            return Ok(FilterResult {
                kept: candidates,
                removed: vec![],
            });
        }

        let tokenized = muted_keywords.iter().map(|k| self.tokenizer.tokenize(k));
        let token_sequences: Vec<TokenSequence> = tokenized.collect::<Vec<_>>();
        let user_mutes = UserMutes::new(token_sequences);
        let matcher = MatchTweetGroup::new(user_mutes);

        let mut kept = Vec::new();
        let mut removed = Vec::new();

        for candidate in candidates {
            let tweet_text_token_sequence = self.tokenizer.tokenize(&candidate.tweet_text);
            let quoted_text_token_sequence = self.tokenizer.tokenize(&candidate.quoted_tweet_text);
            if matcher.matches(&tweet_text_token_sequence)
                || matcher.matches(&quoted_text_token_sequence)
            {
                // Matches muted keywords - should be removed/filtered out
                removed.push(candidate);
            } else {
                // Does not match muted keywords - keep it
                kept.push(candidate);
            }
        }

        Ok(FilterResult { kept, removed })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn removes_candidate_when_quoted_text_matches_muted_keyword() {
        let mut query = ScoredPostsQuery::default();
        query.user_features.muted_keywords = vec!["spoiler".to_string()];
        let candidates = vec![
            PostCandidate {
                tweet_id: 1,
                tweet_text: "safe main text".to_string(),
                quoted_tweet_text: "contains spoiler details".to_string(),
                ..Default::default()
            },
            PostCandidate {
                tweet_id: 2,
                tweet_text: "safe".to_string(),
                quoted_tweet_text: "also safe".to_string(),
                ..Default::default()
            },
        ];

        let result = MutedKeywordFilter::new()
            .filter(&query, candidates)
            .expect("muted keyword filter");

        assert_eq!(result.kept[0].tweet_id, 2);
        assert_eq!(result.removed[0].tweet_id, 1);
    }
}
