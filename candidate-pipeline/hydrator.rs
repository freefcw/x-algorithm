use crate::candidate_pipeline::{PipelineCandidate, PipelineQuery};
use crate::util;
use log::warn;
use std::any::{type_name_of_val, Any};
use std::hash::Hash;
use tonic::async_trait;

// Hydrators run in parallel and update candidate fields
#[async_trait]
pub trait Hydrator<Q, C>: Any + Send + Sync
where
    Q: PipelineQuery,
    C: PipelineCandidate,
{
    /// Decide if this hydrator should run for the given query
    fn enable(&self, _query: &Q) -> bool {
        true
    }

    /// Hydrate candidates by performing async operations.
    /// Returns one result per input candidate in the same order.
    ///
    /// Dropping candidates in a hydrator is not allowed - use a filter stage instead.
    async fn hydrate(&self, query: &Q, candidates: &[C]) -> Vec<Result<C, String>>;

    /// Validate the cardinality contract before the pipeline applies updates.
    async fn run(&self, query: &Q, candidates: &[C]) -> Vec<Result<C, String>> {
        let hydrated = self.hydrate(query, candidates).await;
        let expected_len = candidates.len();
        if hydrated.len() == expected_len {
            hydrated
        } else {
            let message = format!(
                "Hydrator length_mismatch expected={} got={}",
                expected_len,
                hydrated.len()
            );
            warn!("{}", message);
            vec![Err(message); expected_len]
        }
    }

    /// Update a single candidate with the hydrated fields.
    /// Only the fields this hydrator is responsible for should be copied.
    fn update(&self, candidate: &mut C, hydrated: C);

    /// Update only candidates that hydrated successfully.
    fn update_all(&self, candidates: &mut [C], hydrated: Vec<Result<C, String>>) {
        for (candidate, hydrated) in candidates.iter_mut().zip(hydrated) {
            if let Ok(hydrated) = hydrated {
                self.update(candidate, hydrated);
            }
        }
    }

    fn name(&self) -> &'static str {
        util::short_type_name(type_name_of_val(self))
    }
}

#[async_trait]
pub trait CacheStore<K, V>: Send + Sync {
    async fn get(&self, key: &K) -> Option<V>;
    async fn insert(&self, key: K, value: V);
}

#[async_trait]
pub trait CachedHydrator<Q, C>: Any + Send + Sync
where
    Q: PipelineQuery,
    C: PipelineCandidate,
{
    type CacheKey: Eq + Hash + Send + Sync + 'static;
    type CacheValue: Clone + Send + Sync + 'static;

    fn enable(&self, _query: &Q) -> bool {
        true
    }

    fn cache_store(&self) -> &dyn CacheStore<Self::CacheKey, Self::CacheValue>;
    fn cache_key(&self, candidate: &C) -> Self::CacheKey;
    /// Cache key that may also depend on the query (upstream `47c1bcd`).
    /// Defaults to the candidate-only key.
    fn cache_key_for(&self, _query: &Q, candidate: &C) -> Self::CacheKey {
        self.cache_key(candidate)
    }
    fn cache_value(&self, hydrated: &C) -> Self::CacheValue;
    fn hydrate_from_cache(&self, value: Self::CacheValue) -> C;
    async fn hydrate_from_client(&self, query: &Q, candidates: &[C]) -> Vec<Result<C, String>>;

    /// Skip both the cache and the client when the candidate already carries
    /// this hydrator's fields (upstream `47c1bcd`). Defaults to never.
    fn already_hydrated(&self, _candidate: &C) -> bool {
        false
    }

    fn update(&self, candidate: &mut C, hydrated: C);

    fn name(&self) -> &'static str {
        util::short_type_name(type_name_of_val(self))
    }
}

#[async_trait]
impl<Q, C, T> Hydrator<Q, C> for T
where
    Q: PipelineQuery,
    C: PipelineCandidate,
    T: CachedHydrator<Q, C> + ?Sized,
{
    fn enable(&self, query: &Q) -> bool {
        CachedHydrator::enable(self, query)
    }

    async fn hydrate(&self, query: &Q, candidates: &[C]) -> Vec<Result<C, String>> {
        let mut hydrated: Vec<Option<Result<C, String>>> = vec![None; candidates.len()];
        let mut missing_indices = Vec::new();
        let mut missing_keys = Vec::new();
        let mut missing_candidates = Vec::new();

        for (index, candidate) in candidates.iter().enumerate() {
            if self.already_hydrated(candidate) {
                hydrated[index] = Some(Ok(self.hydrate_from_cache(self.cache_value(candidate))));
                continue;
            }
            let key = self.cache_key_for(query, candidate);
            if let Some(value) = self.cache_store().get(&key).await {
                hydrated[index] = Some(Ok(self.hydrate_from_cache(value)));
            } else {
                missing_indices.push(index);
                missing_keys.push(key);
                missing_candidates.push(candidate.clone());
            }
        }

        if !missing_candidates.is_empty() {
            let client_values = self.hydrate_from_client(query, &missing_candidates).await;
            if client_values.len() != missing_candidates.len() {
                let message = format!(
                    "CachedHydrator length_mismatch expected={} got={}",
                    missing_candidates.len(),
                    client_values.len()
                );
                return vec![Err(message); candidates.len()];
            }

            for ((index, key), value) in missing_indices
                .into_iter()
                .zip(missing_keys)
                .zip(client_values)
            {
                if let Ok(ref hydrated_candidate) = value {
                    self.cache_store()
                        .insert(key, self.cache_value(hydrated_candidate))
                        .await;
                }
                hydrated[index] = Some(value);
            }
        }

        hydrated
            .into_iter()
            .map(|value| {
                value.unwrap_or_else(|| Err("Missing hydration result for candidate".to_string()))
            })
            .collect()
    }

    fn update(&self, candidate: &mut C, hydrated: C) {
        CachedHydrator::update(self, candidate, hydrated);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::candidate_pipeline::HasRequestId;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct MemoryStore {
        values: Mutex<HashMap<i32, i32>>,
    }

    #[async_trait]
    impl CacheStore<i32, i32> for MemoryStore {
        async fn get(&self, key: &i32) -> Option<i32> {
            self.values.lock().expect("cache lock").get(key).copied()
        }

        async fn insert(&self, key: i32, value: i32) {
            self.values.lock().expect("cache lock").insert(key, value);
        }
    }

    #[derive(Clone)]
    struct TestQuery;

    impl HasRequestId for TestQuery {
        fn request_id(&self) -> &str {
            "test-request"
        }
    }

    struct TestCachedHydrator {
        cache: Arc<MemoryStore>,
        client_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl CachedHydrator<TestQuery, i32> for TestCachedHydrator {
        type CacheKey = i32;
        type CacheValue = i32;

        fn cache_store(&self) -> &dyn CacheStore<Self::CacheKey, Self::CacheValue> {
            self.cache.as_ref()
        }

        fn cache_key(&self, candidate: &i32) -> Self::CacheKey {
            *candidate
        }

        fn cache_value(&self, hydrated: &i32) -> Self::CacheValue {
            *hydrated
        }

        fn hydrate_from_cache(&self, value: Self::CacheValue) -> i32 {
            value
        }

        async fn hydrate_from_client(
            &self,
            _query: &TestQuery,
            candidates: &[i32],
        ) -> Vec<Result<i32, String>> {
            self.client_calls.fetch_add(1, Ordering::SeqCst);
            candidates
                .iter()
                .map(|candidate| Ok(candidate * 10))
                .collect()
        }

        fn update(&self, candidate: &mut i32, hydrated: i32) {
            *candidate = hydrated;
        }
    }

    #[test]
    fn cached_hydrator_reuses_values_without_calling_client_again() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");
        let cache = Arc::new(MemoryStore::default());
        let client_calls = Arc::new(AtomicUsize::new(0));
        let hydrator = TestCachedHydrator {
            cache,
            client_calls: Arc::clone(&client_calls),
        };

        let first = runtime
            .block_on(hydrator.hydrate(&TestQuery, &[1, 2, 3]))
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("first hydration");
        let second = runtime
            .block_on(hydrator.hydrate(&TestQuery, &[3, 2, 1]))
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("cached hydration");

        assert_eq!(first, vec![10, 20, 30]);
        assert_eq!(second, vec![30, 20, 10]);
        assert_eq!(client_calls.load(Ordering::SeqCst), 1);
    }

    struct ShortHydrator;

    #[async_trait]
    impl Hydrator<TestQuery, i32> for ShortHydrator {
        async fn hydrate(
            &self,
            _query: &TestQuery,
            _candidates: &[i32],
        ) -> Vec<Result<i32, String>> {
            vec![Ok(10)]
        }

        fn update(&self, candidate: &mut i32, hydrated: i32) {
            *candidate = hydrated;
        }
    }

    #[test]
    fn partial_failure_updates_only_successful_candidates() {
        let hydrator = ShortHydrator;
        let mut candidates = vec![1, 2, 3];

        hydrator.update_all(
            &mut candidates,
            vec![Ok(10), Err("candidate unavailable".to_string()), Ok(30)],
        );

        assert_eq!(candidates, vec![10, 2, 30]);
    }

    #[test]
    fn length_mismatch_becomes_one_error_per_input_candidate() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");
        let results = runtime.block_on(ShortHydrator.run(&TestQuery, &[1, 2, 3]));

        assert_eq!(results.len(), 3);
        assert!(results.iter().all(Result::is_err));
    }

    struct PartiallyFailingCachedHydrator {
        cache: Arc<MemoryStore>,
        client_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl CachedHydrator<TestQuery, i32> for PartiallyFailingCachedHydrator {
        type CacheKey = i32;
        type CacheValue = i32;

        fn cache_store(&self) -> &dyn CacheStore<Self::CacheKey, Self::CacheValue> {
            self.cache.as_ref()
        }

        fn cache_key(&self, candidate: &i32) -> Self::CacheKey {
            *candidate
        }

        fn cache_value(&self, hydrated: &i32) -> Self::CacheValue {
            *hydrated
        }

        fn hydrate_from_cache(&self, value: Self::CacheValue) -> i32 {
            value
        }

        async fn hydrate_from_client(
            &self,
            _query: &TestQuery,
            candidates: &[i32],
        ) -> Vec<Result<i32, String>> {
            self.client_calls.fetch_add(1, Ordering::SeqCst);
            candidates
                .iter()
                .map(|candidate| {
                    if *candidate == 2 {
                        Err("candidate unavailable".to_string())
                    } else {
                        Ok(candidate * 10)
                    }
                })
                .collect()
        }

        fn update(&self, candidate: &mut i32, hydrated: i32) {
            *candidate = hydrated;
        }
    }

    struct AlreadyHydratedAware {
        cache: Arc<MemoryStore>,
        client_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl CachedHydrator<TestQuery, i32> for AlreadyHydratedAware {
        type CacheKey = i32;
        type CacheValue = i32;

        fn cache_store(&self) -> &dyn CacheStore<Self::CacheKey, Self::CacheValue> {
            self.cache.as_ref()
        }

        fn cache_key(&self, candidate: &i32) -> Self::CacheKey {
            *candidate
        }

        fn cache_value(&self, hydrated: &i32) -> Self::CacheValue {
            *hydrated
        }

        fn hydrate_from_cache(&self, value: Self::CacheValue) -> i32 {
            value
        }

        // Negative candidates already carry their hydrated value.
        fn already_hydrated(&self, candidate: &i32) -> bool {
            *candidate < 0
        }

        async fn hydrate_from_client(
            &self,
            _query: &TestQuery,
            candidates: &[i32],
        ) -> Vec<Result<i32, String>> {
            self.client_calls.fetch_add(1, Ordering::SeqCst);
            candidates
                .iter()
                .map(|candidate| Ok(candidate * 10))
                .collect()
        }

        fn update(&self, candidate: &mut i32, hydrated: i32) {
            *candidate = hydrated;
        }
    }

    #[test]
    fn already_hydrated_candidates_skip_cache_and_client() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");
        let cache = Arc::new(MemoryStore::default());
        let client_calls = Arc::new(AtomicUsize::new(0));
        let hydrator = AlreadyHydratedAware {
            cache: Arc::clone(&cache),
            client_calls: Arc::clone(&client_calls),
        };

        let results = runtime.block_on(hydrator.hydrate(&TestQuery, &[-5, 2]));

        // -5 is served from its own value without touching cache or client.
        assert_eq!(results[0].as_ref(), Ok(&-5));
        assert_eq!(results[1].as_ref(), Ok(&20));
        assert_eq!(client_calls.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.block_on(cache.get(&-5)), None);
    }

    #[test]
    fn cached_hydrator_caches_only_successful_results() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");
        let cache = Arc::new(MemoryStore::default());
        let client_calls = Arc::new(AtomicUsize::new(0));
        let hydrator = PartiallyFailingCachedHydrator {
            cache: Arc::clone(&cache),
            client_calls: Arc::clone(&client_calls),
        };

        let first = runtime.block_on(hydrator.hydrate(&TestQuery, &[1, 2]));
        let second = runtime.block_on(hydrator.hydrate(&TestQuery, &[1, 2]));

        assert_eq!(first[0].as_ref(), Ok(&10));
        assert!(first[1].is_err());
        assert_eq!(second[0].as_ref(), Ok(&10));
        assert!(second[1].is_err());
        assert_eq!(client_calls.load(Ordering::SeqCst), 2);
        assert_eq!(runtime.block_on(cache.get(&1)), Some(10));
        assert_eq!(runtime.block_on(cache.get(&2)), None);
    }
}
