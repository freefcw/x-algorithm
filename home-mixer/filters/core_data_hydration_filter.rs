use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use xai_candidate_pipeline::filter::{Filter, FilterResult};

pub struct CoreDataHydrationFilter;

impl Filter<ScoredPostsQuery, PostCandidate> for CoreDataHydrationFilter {
    fn filter(
        &self,
        _query: &ScoredPostsQuery,
        candidates: Vec<PostCandidate>,
    ) -> FilterResult<PostCandidate> {
        let (kept, removed) = candidates.into_iter().partition(has_hydrated_core);
        FilterResult { kept, removed }
    }
}

/// Author is required. Body text or media is enough: mrpyq treats a post with
/// photos as recommendable even when the caption is empty.
fn has_hydrated_core(candidate: &PostCandidate) -> bool {
    candidate.author_id != 0
        && (!candidate.tweet_text.trim().is_empty() || candidate.has_media == Some(true))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{pid, uid};

    fn candidate(
        tweet_id: u64,
        author_id: crate::models::UserId,
        tweet_text: &str,
        has_media: Option<bool>,
    ) -> PostCandidate {
        PostCandidate {
            tweet_id: pid(tweet_id),
            author_id,
            tweet_text: tweet_text.to_string(),
            has_media,
            ..Default::default()
        }
    }

    #[test]
    fn keeps_text_posts_and_media_only_posts_with_an_author() {
        let result = CoreDataHydrationFilter.filter(
            &ScoredPostsQuery::test_default(),
            vec![
                candidate(1, uid(10), "caption", None),
                candidate(2, uid(11), "   ", Some(true)),
                candidate(3, uid(12), "", Some(true)),
            ],
        );

        let kept: Vec<_> = result.kept.iter().map(|c| c.tweet_id).collect();
        assert_eq!(kept, vec![pid(1), pid(2), pid(3)]);
        assert!(result.removed.is_empty());
    }

    #[test]
    fn drops_authorless_or_empty_posts_without_confirmed_media() {
        let result = CoreDataHydrationFilter.filter(
            &ScoredPostsQuery::test_default(),
            vec![
                candidate(1, 0, "caption", Some(true)),
                candidate(2, uid(11), "   ", None),
                candidate(3, uid(12), "", Some(false)),
                candidate(4, uid(13), "", None),
            ],
        );

        assert!(result.kept.is_empty());
        let removed: Vec<_> = result.removed.iter().map(|c| c.tweet_id).collect();
        assert_eq!(removed, vec![pid(1), pid(2), pid(3), pid(4)]);
    }
}
