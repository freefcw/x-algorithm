use crate::candidate_pipeline::query::{ScoredPostsQuery, TopicRecallMode};
use crate::clients::user_topic_reader::UserTopicReader;
use std::collections::HashSet;
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::query_hydrator::QueryHydrator;

pub struct UserTopicsQueryHydrator {
    pub reader: Arc<dyn UserTopicReader>,
}

#[async_trait]
impl QueryHydrator<ScoredPostsQuery> for UserTopicsQueryHydrator {
    fn enable(&self, query: &ScoredPostsQuery) -> bool {
        query.topic_recall_mode() == TopicRecallMode::None
            && !query.in_network_only
            && !query.has_cached_posts
    }

    async fn hydrate(&self, query: &ScoredPostsQuery) -> Result<ScoredPostsQuery, String> {
        let topic_ids = self
            .reader
            .get_supplemental_topic_ids(query.user_id)
            .await
            .map_err(|error| format!("failed to read supplemental topics: {error}"))?;
        let excluded: HashSet<i64> = query.excluded_topic_ids.iter().copied().collect();
        let supplemental_topic_ids = eligible_topics(topic_ids, &excluded);

        Ok(ScoredPostsQuery {
            supplemental_topic_ids,
            ..Default::default()
        })
    }

    fn update(&self, query: &mut ScoredPostsQuery, hydrated: ScoredPostsQuery) {
        query.supplemental_topic_ids = hydrated.supplemental_topic_ids;
    }

    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

fn eligible_topics(topic_ids: Vec<i64>, excluded: &HashSet<i64>) -> Vec<i64> {
    let mut seen = HashSet::new();
    topic_ids
        .into_iter()
        .filter(|topic_id| !excluded.contains(topic_id) && seen.insert(*topic_id))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::candidate_pipeline::query::ScoredPostsQuery;
    use crate::clients::user_topic_reader::UserTopicReader;
    use std::sync::Arc;
    use tonic::async_trait;
    use xai_candidate_pipeline::query_hydrator::QueryHydrator;

    enum StubResult {
        Topics(Vec<i64>),
        Failure,
    }

    struct StubUserTopicReader {
        result: StubResult,
    }

    #[async_trait]
    impl UserTopicReader for StubUserTopicReader {
        async fn get_supplemental_topic_ids(
            &self,
            _user_id: i64,
        ) -> Result<Vec<i64>, anyhow::Error> {
            match &self.result {
                StubResult::Topics(topic_ids) => Ok(topic_ids.clone()),
                StubResult::Failure => Err(anyhow::anyhow!("topic profile unavailable")),
            }
        }
    }

    fn hydrate(
        topic_ids: Vec<i64>,
        mut query: ScoredPostsQuery,
    ) -> Result<ScoredPostsQuery, String> {
        let hydrator = UserTopicsQueryHydrator {
            reader: Arc::new(StubUserTopicReader {
                result: StubResult::Topics(topic_ids),
            }),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");
        let hydrated = runtime.block_on(hydrator.hydrate(&query))?;
        hydrator.update(&mut query, hydrated);
        Ok(query)
    }

    #[test]
    fn adapter_selected_topics_are_used_without_home_mixer_policy() {
        let hydrated = hydrate(vec![10, 20], ScoredPostsQuery::default()).expect("topics");

        assert_eq!(hydrated.supplemental_topic_ids, vec![10, 20]);
    }

    #[test]
    fn excluded_and_duplicate_topics_are_removed_at_the_query_boundary() {
        let hydrated = hydrate(
            vec![10, 10, 20],
            ScoredPostsQuery {
                excluded_topic_ids: vec![20],
                ..Default::default()
            },
        )
        .expect("topics");

        assert_eq!(hydrated.supplemental_topic_ids, vec![10]);
    }

    #[test]
    fn existing_topic_origins_skip_supplemental_lookup() {
        let hydrator = UserTopicsQueryHydrator {
            reader: Arc::new(StubUserTopicReader {
                result: StubResult::Failure,
            }),
        };

        assert!(!hydrator.enable(&ScoredPostsQuery {
            topic_ids: vec![10],
            ..Default::default()
        }));
        assert!(!hydrator.enable(&ScoredPostsQuery {
            new_user_topic_ids: vec![20],
            ..Default::default()
        }));
        assert!(!hydrator.enable(&ScoredPostsQuery {
            supplemental_topic_ids: vec![30],
            ..Default::default()
        }));
    }

    #[test]
    fn adapter_failure_is_returned_for_pipeline_level_fail_open() {
        let hydrator = UserTopicsQueryHydrator {
            reader: Arc::new(StubUserTopicReader {
                result: StubResult::Failure,
            }),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");

        let result = runtime.block_on(hydrator.hydrate(&ScoredPostsQuery::default()));

        assert!(result.is_err());
    }
}
