use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use crate::post_text::{MatchTweetGroup, TokenSequence, TweetTokenizer, UserMutes};
use std::sync::Arc;
use xai_candidate_pipeline::filter::{Filter, FilterResult};

pub struct ViewerMutedKeywordFilter {
    pub tokenizer: Arc<TweetTokenizer>,
}

impl ViewerMutedKeywordFilter {
    pub fn new() -> Self {
        let tokenizer = TweetTokenizer::new();
        Self {
            tokenizer: Arc::new(tokenizer),
        }
    }
}

impl Filter<ScoredPostsQuery, PostCandidate> for ViewerMutedKeywordFilter {
    fn filter(
        &self,
        query: &ScoredPostsQuery,
        candidates: Vec<PostCandidate>,
    ) -> FilterResult<PostCandidate> {
        if !query.viewer_relations_hydrated {
            log::warn!(
                "request_id={} filter=ViewerMutedKeywordFilter dropping {} candidates because viewer relations were not hydrated",
                query.request_id,
                candidates.len()
            );
            return FilterResult {
                kept: Vec::new(),
                removed: candidates,
            };
        }

        let muted_keywords = query.user_features.muted_keywords.clone();

        if muted_keywords.is_empty() {
            return FilterResult {
                kept: candidates,
                removed: vec![],
            };
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

        FilterResult { kept, removed }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::user_features::UserFeatures;

    fn create_test_candidate(
        tweet_id: impl Into<crate::models::PostId>,
        tweet_text: &str,
    ) -> PostCandidate {
        PostCandidate {
            tweet_id: tweet_id.into(),
            tweet_text: tweet_text.to_string(),
            author_id: 12345.into(),
            ..Default::default()
        }
    }

    fn create_test_query(muted_keywords: Vec<String>) -> ScoredPostsQuery {
        ScoredPostsQuery {
            viewer_relations_hydrated: true,
            user_features: UserFeatures {
                muted_keywords,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn removed_ids(result: &FilterResult<PostCandidate>) -> Vec<crate::models::PostId> {
        result.removed.iter().map(|c| c.tweet_id).collect()
    }

    #[test]
    fn removes_candidate_when_quoted_text_matches_muted_keyword() {
        let mut query = ScoredPostsQuery {
            viewer_relations_hydrated: true,
            ..Default::default()
        };
        query.user_features.muted_keywords = vec!["spoiler".to_string()];
        let candidates = vec![
            PostCandidate {
                tweet_id: 1.into(),
                tweet_text: "safe main text".to_string(),
                quoted_tweet_text: "contains spoiler details".to_string(),
                ..Default::default()
            },
            PostCandidate {
                tweet_id: 2.into(),
                tweet_text: "safe".to_string(),
                quoted_tweet_text: "also safe".to_string(),
                ..Default::default()
            },
        ];

        let result = ViewerMutedKeywordFilter::new().filter(&query, candidates);

        assert_eq!(result.kept[0].tweet_id, crate::models::pid(2));
        assert_eq!(result.removed[0].tweet_id, crate::models::pid(1));
    }

    #[test]
    fn test_no_muted_keywords() {
        let result = ViewerMutedKeywordFilter::new().filter(
            &create_test_query(vec![]),
            vec![
                create_test_candidate(1, "This is spam content"),
                create_test_candidate(2, "This is good content"),
            ],
        );

        assert_eq!(result.kept.len(), 2);
        assert_eq!(result.removed.len(), 0);
    }

    #[test]
    fn test_simple_keyword_match() {
        let result = ViewerMutedKeywordFilter::new().filter(
            &create_test_query(vec!["spam".to_string()]),
            vec![
                create_test_candidate(1, "This is spam content"),
                create_test_candidate(2, "This is good content"),
            ],
        );

        assert_eq!(removed_ids(&result), [crate::models::pid(1)]);
        assert_eq!(result.kept[0].tweet_id, crate::models::pid(2));
    }

    #[test]
    fn test_hashtag_keyword_without_hash() {
        let result = ViewerMutedKeywordFilter::new().filter(
            &create_test_query(vec!["widget".to_string()]),
            vec![
                create_test_candidate(1, "#widget launch event"),
                create_test_candidate(2, "widget speaks at summit"),
                create_test_candidate(3, "unrelated product news"),
            ],
        );

        assert_eq!(
            removed_ids(&result),
            [crate::models::pid(1), crate::models::pid(2)]
        );
        assert_eq!(result.kept[0].tweet_id, crate::models::pid(3));
    }

    #[test]
    fn test_hashtag_keyword_with_hash() {
        let result = ViewerMutedKeywordFilter::new().filter(
            &create_test_query(vec!["#launch".to_string()]),
            vec![
                create_test_candidate(1, "#LAUNCH day"),
                create_test_candidate(2, "support launch movement"),
                create_test_candidate(3, "unrelated tweet"),
            ],
        );

        assert_eq!(removed_ids(&result), [crate::models::pid(1)]);
        assert_eq!(result.kept.len(), 2);
    }

    #[test]
    fn test_mention_keyword() {
        let result = ViewerMutedKeywordFilter::new().filter(
            &create_test_query(vec!["exampleuser".to_string()]),
            vec![
                create_test_candidate(1, "Hey @exampleuser check this out"),
                create_test_candidate(2, "exampleuser posted something"),
                create_test_candidate(3, "different user posting"),
            ],
        );

        assert_eq!(
            removed_ids(&result),
            [crate::models::pid(1), crate::models::pid(2)]
        );
        assert_eq!(result.kept[0].tweet_id, crate::models::pid(3));
    }

    #[test]
    fn test_multi_word_phrase() {
        let result = ViewerMutedKeywordFilter::new().filter(
            &create_test_query(vec!["crypto scam".to_string()]),
            vec![
                create_test_candidate(1, "This is a crypto scam warning"),
                create_test_candidate(2, "crypto is great, scam artists are bad"),
                create_test_candidate(3, "unrelated content"),
            ],
        );

        assert_eq!(removed_ids(&result), [crate::models::pid(1)]);
        assert_eq!(result.kept.len(), 2);
    }

    #[test]
    fn test_multiple_muted_keywords() {
        let result = ViewerMutedKeywordFilter::new().filter(
            &create_test_query(vec![
                "spam".to_string(),
                "scam".to_string(),
                "#blocked".to_string(),
            ]),
            vec![
                create_test_candidate(1, "This is spam content"),
                create_test_candidate(2, "This is a scam alert"),
                create_test_candidate(3, "#blocked user posting"),
                create_test_candidate(4, "This is good content"),
            ],
        );

        assert_eq!(
            removed_ids(&result),
            [
                crate::models::pid(1),
                crate::models::pid(2),
                crate::models::pid(3)
            ]
        );
        assert_eq!(result.kept[0].tweet_id, crate::models::pid(4));
    }

    #[test]
    fn test_case_insensitive_matching() {
        let result = ViewerMutedKeywordFilter::new().filter(
            &create_test_query(vec!["SPAM".to_string()]),
            vec![
                create_test_candidate(1, "This is SPAM content"),
                create_test_candidate(2, "This is spam content"),
                create_test_candidate(3, "This is SpAm content"),
                create_test_candidate(4, "This is good content"),
            ],
        );

        assert_eq!(
            removed_ids(&result),
            [
                crate::models::pid(1),
                crate::models::pid(2),
                crate::models::pid(3)
            ]
        );
        assert_eq!(result.kept[0].tweet_id, crate::models::pid(4));
    }

    #[test]
    fn test_unicode_and_accents() {
        let result = ViewerMutedKeywordFilter::new().filter(
            &create_test_query(vec!["café".to_string()]),
            vec![
                create_test_candidate(1, "I love café culture"),
                create_test_candidate(2, "I love cafe culture"),
                create_test_candidate(3, "I love coffee culture"),
            ],
        );

        assert_eq!(
            removed_ids(&result),
            [crate::models::pid(1), crate::models::pid(2)]
        );
        assert_eq!(result.kept[0].tweet_id, crate::models::pid(3));
    }

    #[test]
    fn test_empty_candidates() {
        let result = ViewerMutedKeywordFilter::new()
            .filter(&create_test_query(vec!["spam".to_string()]), vec![]);

        assert_eq!(result.kept.len(), 0);
        assert_eq!(result.removed.len(), 0);
    }

    #[test]
    fn test_all_candidates_removed() {
        let result = ViewerMutedKeywordFilter::new().filter(
            &create_test_query(vec!["spam".to_string()]),
            vec![
                create_test_candidate(1, "spam spam spam"),
                create_test_candidate(2, "more spam here"),
            ],
        );

        assert_eq!(result.kept.len(), 0);
        assert_eq!(result.removed.len(), 2);
    }

    #[test]
    fn test_cjk_keyword_whole_token_only() {
        let result = ViewerMutedKeywordFilter::new().filter(
            &create_test_query(vec!["京都".to_string()]),
            vec![
                create_test_candidate(1, "東京都に行くのが楽しみ"),
                create_test_candidate(2, "I visited 京都 last week"),
                create_test_candidate(3, "unrelated content"),
            ],
        );

        assert_eq!(removed_ids(&result), [crate::models::pid(2)]);
        assert_eq!(result.kept.len(), 2);
    }

    #[test]
    fn test_punctuation_handling() {
        let result = ViewerMutedKeywordFilter::new().filter(
            &create_test_query(vec!["bitcoin".to_string()]),
            vec![
                create_test_candidate(1, "Buy bitcoin! It's great!!!"),
                create_test_candidate(2, "bitcoin, ethereum, and more"),
                create_test_candidate(3, "(bitcoin is volatile)"),
                create_test_candidate(4, "stocks and bonds"),
            ],
        );

        assert_eq!(
            removed_ids(&result),
            [
                crate::models::pid(1),
                crate::models::pid(2),
                crate::models::pid(3)
            ]
        );
        assert_eq!(result.kept[0].tweet_id, crate::models::pid(4));
    }

    #[test]
    fn substring_of_a_longer_word_is_not_muted() {
        let result = ViewerMutedKeywordFilter::new().filter(
            &create_test_query(vec!["art".to_string()]),
            vec![
                create_test_candidate(1, "party time"),
                create_test_candidate(2, "the art desk"),
            ],
        );

        assert_eq!(removed_ids(&result), [crate::models::pid(2)]);
        assert_eq!(result.kept[0].tweet_id, crate::models::pid(1));
    }

    #[test]
    fn missing_relation_hydration_drops_every_candidate() {
        let result = ViewerMutedKeywordFilter::new().filter(
            &ScoredPostsQuery::default(),
            vec![
                create_test_candidate(1, "safe content"),
                create_test_candidate(2, "also safe"),
            ],
        );

        assert!(result.kept.is_empty());
        assert_eq!(result.removed.len(), 2);
    }
}
