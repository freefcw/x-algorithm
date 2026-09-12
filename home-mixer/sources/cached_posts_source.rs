use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use tonic::async_trait;
use xai_candidate_pipeline::source::Source;

pub struct CachedPostsSource;

#[async_trait]
impl Source<ScoredPostsQuery, PostCandidate> for CachedPostsSource {
    fn enable(&self, query: &ScoredPostsQuery) -> bool {
        query.has_cached_posts
    }

    async fn source(&self, query: &ScoredPostsQuery) -> Result<Vec<PostCandidate>, String> {
        Ok(query.cached_posts.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_request_cache_without_external_lookup() {
        let query = ScoredPostsQuery {
            has_cached_posts: true,
            cached_posts: vec![
                PostCandidate {
                    tweet_id: 11.into(),
                    ..Default::default()
                },
                PostCandidate {
                    tweet_id: 22.into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");

        let candidates = runtime
            .block_on(CachedPostsSource.source(&query))
            .expect("cached candidates");

        assert_eq!(
            candidates
                .iter()
                .map(|candidate| candidate.tweet_id)
                .collect::<Vec<_>>(),
            vec![crate::models::pid(11), crate::models::pid(22)]
        );
    }
}
