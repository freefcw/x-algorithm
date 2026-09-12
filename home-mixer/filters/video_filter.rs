use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use xai_candidate_pipeline::filter::{Filter, FilterResult};

pub struct VideoFilter;

impl Filter<ScoredPostsQuery, PostCandidate> for VideoFilter {
    fn enable(&self, query: &ScoredPostsQuery) -> bool {
        query.exclude_videos
    }

    fn filter(
        &self,
        _query: &ScoredPostsQuery,
        candidates: Vec<PostCandidate>,
    ) -> FilterResult<PostCandidate> {
        let (removed, kept): (Vec<_>, Vec<_>) = candidates
            .into_iter()
            .partition(|candidate| candidate.video_duration_ms.is_some());
        FilterResult { kept, removed }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn removes_video_candidates_when_request_excludes_video() {
        let query = ScoredPostsQuery {
            exclude_videos: true,
            ..Default::default()
        };
        let candidates = vec![
            PostCandidate {
                tweet_id: 1.into(),
                video_duration_ms: Some(30_000),
                ..Default::default()
            },
            PostCandidate {
                tweet_id: 2.into(),
                ..Default::default()
            },
        ];
        let result = VideoFilter.filter(&query, candidates);

        assert_eq!(result.kept[0].tweet_id, crate::models::pid(2));
        assert_eq!(result.removed[0].tweet_id, crate::models::pid(1));
    }
}
