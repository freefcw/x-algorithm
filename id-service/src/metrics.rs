//! Prometheus metrics for the id-service process.
//!
//! One process-wide registry, reached through [`metrics`]: the HTTP layer
//! records per-route outcomes, the application service records conflicts,
//! provided-id imports and allocations, and the Redis adapter records failed
//! commands and orphan reverse entries. Local cache entry counts are gauges
//! refreshed at scrape time from [`crate::RedisIdRegistry::cache_sizes`];
//! cache lookups (hit/miss/expired) and capacity evictions are counters
//! recorded inline by the cache itself.

use prometheus::{
    Encoder, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder,
};
use std::sync::LazyLock;

static METRICS: LazyLock<Metrics> = LazyLock::new(Metrics::new);

/// The process-wide metrics instance.
pub fn metrics() -> &'static Metrics {
    &METRICS
}

pub struct Metrics {
    registry: Registry,
    requests: IntCounterVec,
    conflicts: IntCounterVec,
    provided_imports: IntCounter,
    allocations: IntCounter,
    redis_errors: IntCounter,
    orphan_reverse_mappings: IntCounter,
    cache_entries: IntGaugeVec,
    cache_lookups: IntCounterVec,
    cache_evictions: IntCounterVec,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Registration only fails on duplicate names, which would be a
    /// programming error in this module, so construction panics instead of
    /// returning a `Result`.
    pub fn new() -> Self {
        let registry = Registry::new();
        let build_info = IntGaugeVec::new(
            Opts::new(
                "id_service_build_info",
                "Constant 1, labelled with the running crate version",
            ),
            &["version"],
        )
        .expect("valid build_info opts");
        build_info
            .with_label_values(&[env!("CARGO_PKG_VERSION")])
            .set(1);
        let requests = IntCounterVec::new(
            Opts::new(
                "id_service_requests_total",
                "HTTP and gRPC requests by route/method and status code",
            ),
            &["route", "status"],
        )
        .expect("valid requests opts");
        let conflicts = IntCounterVec::new(
            Opts::new(
                "id_service_conflicts_total",
                "Rejected resolutions by conflict kind (mapping, snowflake_taken, entity_kind)",
            ),
            &["kind"],
        )
        .expect("valid conflicts opts");
        let provided_imports = IntCounter::new(
            "id_service_provided_imports_total",
            "Mappings created from a caller-provided Snowflake",
        )
        .expect("valid provided imports opts");
        let allocations = IntCounter::new(
            "id_service_allocations_total",
            "Mappings created with a newly allocated Snowflake",
        )
        .expect("valid allocations opts");
        let redis_errors = IntCounter::new(
            "id_service_redis_errors_total",
            "Redis commands that failed or timed out",
        )
        .expect("valid redis errors opts");
        let orphan_reverse_mappings = IntCounter::new(
            "id_service_orphan_reverse_mappings_total",
            "Reverse entries whose forward entry was missing or pointed elsewhere; treated as absent",
        )
        .expect("valid orphan opts");
        let cache_entries = IntGaugeVec::new(
            Opts::new(
                "id_service_cache_entries",
                "Entries in the local caches, by lookup direction",
            ),
            &["direction"],
        )
        .expect("valid cache opts");
        let cache_lookups = IntCounterVec::new(
            Opts::new(
                "id_service_cache_lookups_total",
                "Local cache lookups by direction and result (hit, miss, expired)",
            ),
            &["direction", "result"],
        )
        .expect("valid cache lookups opts");
        let cache_evictions = IntCounterVec::new(
            Opts::new(
                "id_service_cache_evictions_total",
                "Local cache entries evicted because the cache exceeded its capacity, by direction",
            ),
            &["direction"],
        )
        .expect("valid cache evictions opts");

        for collector in [
            Box::new(build_info) as Box<dyn prometheus::core::Collector>,
            Box::new(requests.clone()),
            Box::new(conflicts.clone()),
            Box::new(provided_imports.clone()),
            Box::new(allocations.clone()),
            Box::new(redis_errors.clone()),
            Box::new(orphan_reverse_mappings.clone()),
            Box::new(cache_entries.clone()),
            Box::new(cache_lookups.clone()),
            Box::new(cache_evictions.clone()),
        ] {
            registry.register(collector).expect("register metric");
        }

        Self {
            registry,
            requests,
            conflicts,
            provided_imports,
            allocations,
            redis_errors,
            orphan_reverse_mappings,
            cache_entries,
            cache_lookups,
            cache_evictions,
        }
    }

    pub fn record_request(&self, route: &str, status: u16) {
        self.requests
            .with_label_values(&[route, &status.to_string()])
            .inc();
    }

    pub fn record_conflict(&self, kind: &'static str) {
        self.conflicts.with_label_values(&[kind]).inc();
    }

    pub fn record_provided_import(&self) {
        self.provided_imports.inc();
    }

    pub fn record_allocation(&self) {
        self.allocations.inc();
    }

    pub fn record_redis_error(&self) {
        self.redis_errors.inc();
    }

    pub fn record_orphan_reverse_mapping(&self) {
        self.orphan_reverse_mappings.inc();
    }

    /// `result` is `hit`, `miss`, or `expired` (TTL-expired on read).
    pub fn record_cache_lookup(&self, direction: &'static str, result: &'static str) {
        self.cache_lookups
            .with_label_values(&[direction, result])
            .inc();
    }

    pub fn record_cache_eviction(&self, direction: &'static str) {
        self.cache_evictions.with_label_values(&[direction]).inc();
    }

    /// Refresh the cache gauges from `(object_entries, snowflake_entries)`.
    pub fn set_cache_sizes(&self, sizes: (usize, usize)) {
        self.cache_gauge("object").set(sizes.0 as i64);
        self.cache_gauge("snowflake").set(sizes.1 as i64);
    }

    fn cache_gauge(&self, direction: &str) -> IntGauge {
        self.cache_entries.with_label_values(&[direction])
    }

    /// Prometheus text exposition of the whole registry.
    pub fn encode(&self) -> Result<String, prometheus::Error> {
        let encoder = TextEncoder::new();
        let mut buffer = Vec::new();
        encoder.encode(&self.registry.gather(), &mut buffer)?;
        String::from_utf8(buffer).map_err(|error| prometheus::Error::Msg(error.to_string()))
    }

    /// `Content-Type` for [`Self::encode`].
    pub fn content_type() -> &'static str {
        prometheus::TEXT_FORMAT
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposition_contains_every_family() {
        let metrics = Metrics::new();
        metrics.record_request("/v1/resolve", 200);
        metrics.record_conflict("mapping");
        metrics.record_provided_import();
        metrics.record_allocation();
        metrics.record_redis_error();
        metrics.record_orphan_reverse_mapping();
        metrics.set_cache_sizes((3, 4));
        metrics.record_cache_lookup("object", "hit");
        metrics.record_cache_lookup("object", "miss");
        metrics.record_cache_lookup("snowflake", "expired");
        metrics.record_cache_eviction("object");
        let text = metrics.encode().unwrap();
        for expected in [
            &format!(
                "id_service_build_info{{version=\"{}\"}} 1",
                env!("CARGO_PKG_VERSION")
            ),
            "id_service_requests_total{route=\"/v1/resolve\",status=\"200\"} 1",
            "id_service_conflicts_total{kind=\"mapping\"} 1",
            "id_service_provided_imports_total 1",
            "id_service_allocations_total 1",
            "id_service_redis_errors_total 1",
            "id_service_orphan_reverse_mappings_total 1",
            "id_service_cache_entries{direction=\"object\"} 3",
            "id_service_cache_entries{direction=\"snowflake\"} 4",
            "id_service_cache_lookups_total{direction=\"object\",result=\"hit\"} 1",
            "id_service_cache_lookups_total{direction=\"object\",result=\"miss\"} 1",
            "id_service_cache_lookups_total{direction=\"snowflake\",result=\"expired\"} 1",
            "id_service_cache_evictions_total{direction=\"object\"} 1",
        ] {
            assert!(text.contains(expected), "missing {expected}\n{text}");
        }
        assert_eq!(Metrics::content_type(), "text/plain; version=0.0.4");
    }
}
