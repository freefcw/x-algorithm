use crate::models::candidate::{PostCandidate, RetrievalSource};
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
        Ok(query
            .cached_posts
            .iter()
            .cloned()
            .map(|mut candidate| {
                if candidate.retrieval_sources.is_empty() {
                    let served_type = candidate
                        .served_type
                        .unwrap_or(x_algorithm_proto::home_mixer::ServedType::ForYouCachedPost);
                    candidate.retrieval_sources =
                        vec![RetrievalSource::from_served_type(served_type)];
                }
                candidate
            })
            .collect())
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
                    tweet_id: 11,
                    ..Default::default()
                },
                PostCandidate {
                    tweet_id: 22,
                    ..Default::default()
                },
            ],
            ..ScoredPostsQuery::test_default()
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
