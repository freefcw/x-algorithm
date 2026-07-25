use crate::util;
use std::any::{type_name_of_val, Any};
use tonic::async_trait;

// Hydrators run in parallel and update candidate fields
#[async_trait]
pub trait Hydrator<Q, C>: Any + Send + Sync
where
    Q: Clone + Send + Sync + 'static,
    C: Clone + Send + Sync + 'static,
{
    /// Decide if this hydrator should run for the given query
    fn enable(&self, _query: &Q) -> bool {
        true
    }

    /// Hydrate candidates by performing async operations.
    /// Returns candidates with this hydrator's fields populated.
    ///
    /// IMPORTANT: The returned vector must have the same candidates in the same order as the input.
    /// Dropping candidates in a hydrator is not allowed - use a filter stage instead.
    async fn hydrate(&self, query: &Q, candidates: &[C]) -> Result<Vec<C>, String>;

    /// Update a single candidate with the hydrated fields.
    /// Only the fields this hydrator is responsible for should be copied.
    fn update(&self, candidate: &mut C, hydrated: C);

    /// Update all candidates with the hydrated fields from `hydrated`.
    /// Default implementation iterates and calls `update` for each pair.
    fn update_all(&self, candidates: &mut [C], hydrated: Vec<C>) {
        for (c, h) in candidates.iter_mut().zip(hydrated) {
            self.update(c, h);
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
    Q: Clone + Send + Sync + 'static,
    C: Clone + Send + Sync + 'static,
{
    type CacheKey: Clone + Send + Sync + 'static;
    type CacheValue: Clone + Send + Sync + 'static;

    fn enable(&self, _query: &Q) -> bool {
        true
    }

    fn cache_store(&self) -> &dyn CacheStore<Self::CacheKey, Self::CacheValue>;
    fn cache_key(&self, candidate: &C) -> Self::CacheKey;
    fn cache_value(&self, hydrated: &C) -> Self::CacheValue;
    fn hydrate_from_cache(&self, value: Self::CacheValue) -> C;
    async fn hydrate_from_client(&self, query: &Q, candidates: &[C]) -> Result<Vec<C>, String>;
    fn update(&self, candidate: &mut C, hydrated: C);
}

#[async_trait]
impl<Q, C, T> Hydrator<Q, C> for T
where
    Q: Clone + Send + Sync + 'static,
    C: Clone + Send + Sync + 'static,
    T: CachedHydrator<Q, C>,
{
    fn enable(&self, query: &Q) -> bool {
        CachedHydrator::enable(self, query)
    }

    async fn hydrate(&self, query: &Q, candidates: &[C]) -> Result<Vec<C>, String> {
        let mut hydrated: Vec<Option<C>> = vec![None; candidates.len()];
        let mut missing_indices = Vec::new();
        let mut missing_keys = Vec::new();
        let mut missing_candidates = Vec::new();

        for (index, candidate) in candidates.iter().enumerate() {
            let key = self.cache_key(candidate);
            if let Some(value) = self.cache_store().get(&key).await {
                hydrated[index] = Some(self.hydrate_from_cache(value));
            } else {
                missing_indices.push(index);
                missing_keys.push(key);
                missing_candidates.push(candidate.clone());
            }
        }

        if !missing_candidates.is_empty() {
            let client_values = self.hydrate_from_client(query, &missing_candidates).await?;
            if client_values.len() != missing_candidates.len() {
                return Err(format!(
                    "cached hydrator returned {} values for {} misses",
                    client_values.len(),
                    missing_candidates.len()
                ));
            }

            for ((index, key), value) in missing_indices
                .into_iter()
                .zip(missing_keys)
                .zip(client_values)
            {
                self.cache_store()
                    .insert(key, self.cache_value(&value))
                    .await;
                hydrated[index] = Some(value);
            }
        }

        hydrated
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                value.ok_or_else(|| format!("missing hydrated value at index {index}"))
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

    struct TestCachedHydrator {
        cache: Arc<MemoryStore>,
        client_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl CachedHydrator<(), i32> for TestCachedHydrator {
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
            _query: &(),
            candidates: &[i32],
        ) -> Result<Vec<i32>, String> {
            self.client_calls.fetch_add(1, Ordering::SeqCst);
            Ok(candidates.iter().map(|candidate| candidate * 10).collect())
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
            .block_on(hydrator.hydrate(&(), &[1, 2, 3]))
            .expect("first hydration");
        let second = runtime
            .block_on(hydrator.hydrate(&(), &[3, 2, 1]))
            .expect("cached hydration");

        assert_eq!(first, vec![10, 20, 30]);
        assert_eq!(second, vec![30, 20, 10]);
        assert_eq!(client_calls.load(Ordering::SeqCst), 1);
    }
}
