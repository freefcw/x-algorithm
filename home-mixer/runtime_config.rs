use crate::clients::redis_feed_state_store::RedisFeedStateConfig;
use crate::clients::uas_fetcher::RedisUserActionSequenceConfig;
use crate::feature_policy::{HomeMixerFeatures, VfFailurePolicy};
use std::time::Duration;

/// Which `UserActionSequenceOps` adapter the assembly injects.
///
/// Resolved once at startup from `UAS_REDIS_URL`, falling back to the shared
/// `HOME_MIXER_REDIS_URL` outside demo mode. Demo keeps its synthetic sequence
/// unless a UAS Redis is named explicitly, so enabling Redis feed state in a
/// demo does not silently switch the demo off personalization.
#[derive(Clone, Debug, Default)]
pub enum UasConfig {
    /// No behavior source at all: Phoenix retrieval and ranking are skipped
    /// and every request is ranked by the rule fallback. Chosen only when no
    /// Redis URL is configured on an explicit-mode assembly path.
    #[default]
    Disabled,
    Redis(RedisUserActionSequenceConfig),
}

impl UasConfig {
    pub fn from_env(mode: HomeMixerMode) -> anyhow::Result<Self> {
        Self::from_lookup(mode, |name| std::env::var(name).ok())
    }

    fn from_lookup(
        mode: HomeMixerMode,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> anyhow::Result<Self> {
        let explicit = non_empty(lookup("UAS_REDIS_URL"));
        let cluster = non_empty(lookup("UAS_REDIS_CLUSTER_URLS"))
            .or_else(|| non_empty(lookup("HOME_MIXER_REDIS_CLUSTER_URLS")));
        let url = explicit.or_else(|| non_empty(lookup("HOME_MIXER_REDIS_URL")));
        let config = match (cluster, url) {
            (Some(cluster), url) => Self::Redis(redis_uas_config_from_lookup(
                url.unwrap_or_default(),
                Some(cluster),
                &lookup,
            )?),
            (None, Some(url)) => Self::Redis(redis_uas_config_from_lookup(url, None, &lookup)?),
            (None, None) => Self::Disabled,
        };
        config.validate(mode)?;
        Ok(config)
    }

    /// The projection job has no runtime mode: it always needs the Redis
    /// target and shares the same variables as the server.
    pub fn redis_from_env() -> anyhow::Result<RedisUserActionSequenceConfig> {
        Self::redis_from_lookup(|name| std::env::var(name).ok())
    }

    fn redis_from_lookup(
        lookup: impl Fn(&str) -> Option<String>,
    ) -> anyhow::Result<RedisUserActionSequenceConfig> {
        let cluster = non_empty(lookup("UAS_REDIS_CLUSTER_URLS"))
            .or_else(|| non_empty(lookup("HOME_MIXER_REDIS_CLUSTER_URLS")));
        let url = non_empty(lookup("UAS_REDIS_URL"))
            .or_else(|| non_empty(lookup("HOME_MIXER_REDIS_URL")));
        let (url, cluster) = match (url, cluster) {
            (url, cluster) if url.is_none() && cluster.is_none() => {
                anyhow::bail!(
                    "UAS_REDIS_URL / UAS_REDIS_CLUSTER_URLS (or HOME_MIXER_REDIS_URL / HOME_MIXER_REDIS_CLUSTER_URLS) must be configured"
                )
            }
            (url, cluster) => (url, cluster),
        };
        let config = redis_uas_config_from_lookup(url.unwrap_or_default(), cluster, &lookup)?;
        config.validate().map_err(anyhow::Error::msg)?;
        Ok(config)
    }

    pub fn validate(&self, _mode: HomeMixerMode) -> anyhow::Result<()> {
        match self {
            Self::Disabled => Ok(()),
            Self::Redis(config) => config.validate().map_err(anyhow::Error::msg),
        }
    }
}

fn non_empty(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn redis_uas_config_from_lookup(
    url: String,
    cluster: Option<String>,
    lookup: &impl Fn(&str) -> Option<String>,
) -> anyhow::Result<RedisUserActionSequenceConfig> {
    let mut config = RedisUserActionSequenceConfig::new(url);
    if let Some(cluster) = cluster {
        config = config.with_cluster_urls(&cluster);
    }
    if let Some(prefix) = lookup("UAS_REDIS_KEY_PREFIX") {
        config.key_prefix = prefix.trim().to_string();
    }
    if let Some(value) = lookup("UAS_MAX_ACTIONS") {
        config.max_actions = value
            .trim()
            .parse::<usize>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| anyhow::anyhow!("UAS_MAX_ACTIONS must be a positive integer"))?;
    }
    if let Some(value) = lookup("UAS_REDIS_TTL_SECS") {
        let ttl = value
            .trim()
            .parse::<u64>()
            .map_err(|_| anyhow::anyhow!("UAS_REDIS_TTL_SECS must be a non-negative integer"))?;
        config.ttl_secs = (ttl > 0).then_some(ttl);
    }
    for (name, target) in [
        ("UAS_REDIS_CONNECT_TIMEOUT_MS", &mut config.connect_timeout),
        ("UAS_REDIS_REQUEST_TIMEOUT_MS", &mut config.request_timeout),
    ] {
        if let Some(value) = lookup(name) {
            let millis = value
                .trim()
                .parse::<u64>()
                .ok()
                .filter(|value| *value > 0)
                .ok_or_else(|| anyhow::anyhow!("{name} must be a positive integer"))?;
            *target = Duration::from_millis(millis);
        }
    }
    Ok(config)
}

/// The backend is selected once at startup. Only demo may use local state.
#[derive(Clone, Debug, Default)]
pub enum FeedStateConfig {
    #[default]
    InMemory,
    Redis(RedisFeedStateConfig),
}

impl FeedStateConfig {
    fn from_env(mode: HomeMixerMode) -> anyhow::Result<Self> {
        Self::from_lookup(mode, |name| std::env::var(name).ok())
    }

    fn from_lookup(
        mode: HomeMixerMode,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> anyhow::Result<Self> {
        let cluster = non_empty(lookup("HOME_MIXER_REDIS_CLUSTER_URLS"));
        let url = lookup("HOME_MIXER_REDIS_URL")
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        if cluster.is_none() && url.is_none() {
            let config = Self::InMemory;
            if mode == HomeMixerMode::ProductionReady {
                // Keep the mode-level readiness error as the primary startup
                // message; that mode is rejected before storage is built.
                return Ok(config);
            }
            config.validate(mode)?;
            return Ok(config);
        }
        // Cluster seeds take precedence; the single URL becomes optional.
        let mut redis = RedisFeedStateConfig::new(url.unwrap_or_default());
        if let Some(cluster) = cluster {
            redis = redis.with_cluster_urls(&cluster);
        }
        if let Some(prefix) = lookup("HOME_MIXER_REDIS_KEY_PREFIX") {
            redis.key_prefix = prefix.trim().to_string();
        }
        if let Some(value) = lookup("HOME_MIXER_FEED_STATE_TTL_SECS") {
            let ttl = value.trim().parse::<u64>().map_err(|_| {
                anyhow::anyhow!("HOME_MIXER_FEED_STATE_TTL_SECS must be a non-negative integer")
            })?;
            redis.ttl_secs = (ttl > 0).then_some(ttl);
        }
        for (name, target) in [
            (
                "HOME_MIXER_REDIS_CONNECT_TIMEOUT_MS",
                &mut redis.connect_timeout,
            ),
            (
                "HOME_MIXER_REDIS_REQUEST_TIMEOUT_MS",
                &mut redis.request_timeout,
            ),
        ] {
            if let Some(value) = lookup(name) {
                let millis = value
                    .trim()
                    .parse::<u64>()
                    .ok()
                    .filter(|value| *value > 0)
                    .ok_or_else(|| anyhow::anyhow!("{name} must be a positive integer"))?;
                *target = Duration::from_millis(millis);
            }
        }
        let config = Self::Redis(redis);
        config.validate(mode)?;
        Ok(config)
    }

    pub fn validate(&self, _mode: HomeMixerMode) -> anyhow::Result<()> {
        match self {
            Self::InMemory => anyhow::bail!(
                "HOME_MIXER_REDIS_URL is required; in-memory feed state cannot be shared across instances"
            ),
            Self::Redis(config) => config.validate().map_err(anyhow::Error::msg),
        }
    }
}

/// Runtime intent is explicit so a degraded local assembly cannot be mistaken
/// for a production-ready recommendation service.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum HomeMixerMode {
    #[default]
    Degraded,
    ProductionReady,
}

impl HomeMixerMode {
    fn from_env() -> anyhow::Result<Self> {
        let explicit = std::env::var("HOME_MIXER_MODE").ok();
        let legacy_demo = std::env::var("HOME_MIXER_DEMO").is_ok_and(|value| value == "1");
        Self::from_values(explicit.as_deref(), legacy_demo)
    }

    fn from_values(explicit: Option<&str>, legacy_demo: bool) -> anyhow::Result<Self> {
        if legacy_demo || explicit.is_some_and(|value| value.trim().eq_ignore_ascii_case("demo")) {
            anyhow::bail!(
                "demo mode has been removed; HOME_MIXER_DEMO/HOME_MIXER_MODE=demo is not supported"
            )
        }
        Ok(explicit
            .map(Self::parse)
            .transpose()?
            .unwrap_or(Self::Degraded))
    }

    fn parse(value: &str) -> anyhow::Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "degraded" => Ok(Self::Degraded),
            "production" | "production_ready" | "production-ready" => Ok(Self::ProductionReady),
            value => anyhow::bail!(
                "invalid HOME_MIXER_MODE={value}; expected degraded or production_ready"
            ),
        }
    }
}

/// Local replacement for the upstream service-builder configuration.
#[derive(Clone, Debug)]
pub struct HomeMixerConfig {
    pub mode: HomeMixerMode,
    pub features: HomeMixerFeatures,
    pub debug_token: Option<String>,
    pub feed_state: FeedStateConfig,
    pub uas: UasConfig,
    /// Server-side budget for one RPC after query construction; see
    /// `params::REQUEST_TIMEOUT_MS`. A shorter client `grpc-timeout` wins.
    pub request_timeout: Duration,
    /// Primary low-latency Registry gRPC endpoint.
    pub id_registry_grpc_addr: String,
}

impl Default for HomeMixerConfig {
    fn default() -> Self {
        Self {
            mode: HomeMixerMode::default(),
            features: HomeMixerFeatures::default(),
            debug_token: None,
            feed_state: FeedStateConfig::default(),
            uas: UasConfig::default(),
            request_timeout: Duration::from_millis(crate::params::REQUEST_TIMEOUT_MS),
            id_registry_grpc_addr: "http://127.0.0.1:50072".to_string(),
        }
    }
}

const REQUEST_TIMEOUT_ENV: &str = "HOME_MIXER_REQUEST_TIMEOUT_MS";

fn request_timeout_from_lookup(
    lookup: impl Fn(&str) -> Option<String>,
) -> anyhow::Result<Duration> {
    match non_empty(lookup(REQUEST_TIMEOUT_ENV)) {
        None => Ok(Duration::from_millis(crate::params::REQUEST_TIMEOUT_MS)),
        Some(value) => value
            .parse::<u64>()
            .ok()
            .filter(|millis| *millis > 0)
            .map(Duration::from_millis)
            .ok_or_else(|| anyhow::anyhow!("{REQUEST_TIMEOUT_ENV} must be a positive integer")),
    }
}

impl HomeMixerConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let mode = HomeMixerMode::from_env()?;
        let config = Self {
            mode,
            features: HomeMixerFeatures::from_env(),
            debug_token: std::env::var("HOME_MIXER_DEBUG_TOKEN")
                .ok()
                .map(|token| token.trim().to_string()),
            // Feed state first: outside demo it requires HOME_MIXER_REDIS_URL,
            // which is also the UAS fallback, so its error stays primary.
            feed_state: FeedStateConfig::from_env(mode)?,
            uas: UasConfig::from_env(mode)?,
            request_timeout: request_timeout_from_lookup(|name| std::env::var(name).ok())?,
            id_registry_grpc_addr: std::env::var("HOME_MIXER_ID_REGISTRY_GRPC_ADDR")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "http://127.0.0.1:50072".to_string()),
        };
        config.validate()?;
        Ok(config)
    }

    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        if self.mode == HomeMixerMode::ProductionReady {
            anyhow::bail!(
                "production_ready is unavailable: business adapter contracts are not verified (caller identity, TES, UAS event schema/retention, Strato, VF, in-network/fallback, Phoenix metadata, served persist)"
            );
        }
        self.feed_state.validate(self.mode)?;
        self.uas.validate(self.mode)?;
        if self.request_timeout.is_zero() {
            anyhow::bail!("the request timeout must be positive");
        }
        crate::id::RegistryClient::new_with_grpc(&self.id_registry_grpc_addr)
            .map_err(|error| anyhow::anyhow!("invalid ID Registry endpoints: {error}"))?;
        if self.features.vf_failure_policy == VfFailurePolicy::AllowAll {
            log::warn!(
                "HOME_MIXER_VF_FAILURE_POLICY=allow_all serves candidates whose visibility could not be established; this is an explicit opt-out of the fail-closed default"
            );
        }
        if self.features.debug_rpc
            && self
                .debug_token
                .as_deref()
                .is_none_or(|token| token.trim().is_empty())
        {
            anyhow::bail!(
                "HOME_MIXER_ENABLE_DEBUG_RPC requires a non-empty HOME_MIXER_DEBUG_TOKEN"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feed_state_config(
        mode: HomeMixerMode,
        values: &[(&str, &str)],
    ) -> anyhow::Result<FeedStateConfig> {
        FeedStateConfig::from_lookup(mode, |name| {
            values
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).to_string())
        })
    }

    #[test]
    fn redis_settings_are_explicit_and_preserve_zero_ttl() {
        let config = feed_state_config(
            HomeMixerMode::Degraded,
            &[
                ("HOME_MIXER_REDIS_URL", " redis://localhost:6379/ "),
                ("HOME_MIXER_REDIS_KEY_PREFIX", "feed:test"),
                ("HOME_MIXER_FEED_STATE_TTL_SECS", "0"),
                ("HOME_MIXER_REDIS_CONNECT_TIMEOUT_MS", "200"),
                ("HOME_MIXER_REDIS_REQUEST_TIMEOUT_MS", "75"),
            ],
        )
        .unwrap();
        let FeedStateConfig::Redis(redis) = config else {
            panic!("business deployments must use Redis");
        };
        assert_eq!(redis.url, "redis://localhost:6379/");
        assert_eq!(redis.key_prefix, "feed:test");
        assert_eq!(redis.ttl_secs, None);
        assert_eq!(redis.connect_timeout, Duration::from_millis(200));
        assert_eq!(redis.request_timeout, Duration::from_millis(75));
    }

    #[test]
    fn invalid_redis_settings_cannot_fall_back_to_memory() {
        for (key, value) in [
            ("HOME_MIXER_REDIS_URL", "invalid://localhost"),
            ("HOME_MIXER_REDIS_KEY_PREFIX", " "),
            ("HOME_MIXER_FEED_STATE_TTL_SECS", "-1"),
            ("HOME_MIXER_REDIS_CONNECT_TIMEOUT_MS", "0"),
            ("HOME_MIXER_REDIS_REQUEST_TIMEOUT_MS", "0"),
            ("HOME_MIXER_REDIS_REQUEST_TIMEOUT_MS", "invalid"),
        ] {
            let mut values = vec![("HOME_MIXER_REDIS_URL", "redis://localhost:6379/")];
            if key == "HOME_MIXER_REDIS_URL" {
                values.clear();
            }
            values.push((key, value));
            assert!(
                feed_state_config(HomeMixerMode::Degraded, &values).is_err(),
                "{key}"
            );
        }
    }

    #[test]
    fn programmatic_business_config_also_requires_shared_state() {
        let mut config = HomeMixerConfig::default();
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("HOME_MIXER_REDIS_URL"));
        config.feed_state =
            FeedStateConfig::Redis(RedisFeedStateConfig::new("redis://localhost:6379/"));
        config.uas = UasConfig::Redis(RedisUserActionSequenceConfig::new(
            "redis://localhost:6379/",
        ));
        assert!(config.validate().is_ok());
        config.uas = UasConfig::Disabled;
        assert!(config.validate().is_ok());
    }

    fn uas_config(mode: HomeMixerMode, values: &[(&str, &str)]) -> anyhow::Result<UasConfig> {
        UasConfig::from_lookup(mode, |name| lookup_in(values, name))
    }

    fn lookup_in(values: &[(&str, &str)], name: &str) -> Option<String> {
        values
            .iter()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| (*value).to_string())
    }

    #[test]
    fn cluster_seed_list_selects_cluster_routing_without_a_single_url() {
        let FeedStateConfig::Redis(feed) = feed_state_config(
            HomeMixerMode::Degraded,
            &[
                (
                    "HOME_MIXER_REDIS_CLUSTER_URLS",
                    "redis://127.0.0.1:7000, redis://127.0.0.1:7001",
                ),
                ("HOME_MIXER_REDIS_KEY_PREFIX", "x-algorithm"),
            ],
        )
        .expect("cluster seeds alone must configure Redis feed state") else {
            panic!("cluster seeds must select the Redis adapter");
        };
        assert_eq!(
            feed.cluster_urls,
            Some(vec![
                "redis://127.0.0.1:7000".to_string(),
                "redis://127.0.0.1:7001".to_string(),
            ])
        );
        assert_eq!(feed.url, "");
        assert_eq!(feed.key_prefix, "x-algorithm");
        assert!(feed.validate().is_ok());

        // A blank cluster list keeps the single-endpoint shape.
        let FeedStateConfig::Redis(single) = feed_state_config(
            HomeMixerMode::Degraded,
            &[
                ("HOME_MIXER_REDIS_CLUSTER_URLS", " , "),
                ("HOME_MIXER_REDIS_URL", "redis://127.0.0.1:6379/"),
            ],
        )
        .expect("blank cluster list must not break single-endpoint config") else {
            panic!("a single URL must still select the Redis adapter");
        };
        assert_eq!(single.cluster_urls, None);

        // Invalid seed URLs fail at config time instead of starting
        // half-wired.
        let error = feed_state_config(
            HomeMixerMode::Degraded,
            &[("HOME_MIXER_REDIS_CLUSTER_URLS", "not-a-redis-url")],
        )
        .expect_err("an invalid seed URL must fail configuration");
        assert!(error.to_string().contains("invalid Redis URL"));
    }

    #[test]
    fn uas_cluster_seeds_have_explicit_and_shared_spellings() {
        let UasConfig::Redis(explicit) = uas_config(
            HomeMixerMode::Degraded,
            &[("UAS_REDIS_CLUSTER_URLS", "redis://127.0.0.1:7000")],
        )
        .expect("UAS cluster seeds alone must configure the adapter") else {
            panic!("UAS cluster seeds must select the Redis adapter");
        };
        assert_eq!(
            explicit.cluster_urls,
            Some(vec!["redis://127.0.0.1:7000".to_string()])
        );

        let UasConfig::Redis(shared) = uas_config(
            HomeMixerMode::ProductionReady,
            &[("HOME_MIXER_REDIS_CLUSTER_URLS", "redis://127.0.0.1:7000")],
        )
        .expect("the shared cluster spelling covers business mode too") else {
            panic!("the shared cluster spelling must select the Redis adapter");
        };
        assert!(shared.cluster_urls.is_some());
        assert!(shared.validate().is_ok());
    }

    #[test]
    fn business_uas_falls_back_to_the_shared_redis_and_is_disabled_without_one() {
        let UasConfig::Redis(shared) = uas_config(
            HomeMixerMode::Degraded,
            &[("HOME_MIXER_REDIS_URL", " redis://shared:6379/ ")],
        )
        .unwrap() else {
            panic!("shared Redis must back UAS");
        };
        assert_eq!(shared.url, "redis://shared:6379/");

        let UasConfig::Redis(dedicated) = uas_config(
            HomeMixerMode::Degraded,
            &[
                ("HOME_MIXER_REDIS_URL", "redis://shared:6379/"),
                ("UAS_REDIS_URL", "redis://uas:6379/"),
                ("UAS_REDIS_KEY_PREFIX", " uas:test "),
                ("UAS_MAX_ACTIONS", "42"),
                ("UAS_REDIS_TTL_SECS", "0"),
                ("UAS_REDIS_CONNECT_TIMEOUT_MS", "200"),
                ("UAS_REDIS_REQUEST_TIMEOUT_MS", "75"),
            ],
        )
        .unwrap() else {
            panic!("explicit UAS Redis must win");
        };
        assert_eq!(dedicated.url, "redis://uas:6379/");
        assert_eq!(dedicated.key_prefix, "uas:test");
        assert_eq!(dedicated.max_actions, 42);
        assert_eq!(dedicated.ttl_secs, None);
        assert_eq!(dedicated.connect_timeout, Duration::from_millis(200));
        assert_eq!(dedicated.request_timeout, Duration::from_millis(75));

        // Explicit-mode assembly without any Redis is a deliberate Disabled
        // adapter, never a silent demo sequence.
        assert!(matches!(
            uas_config(HomeMixerMode::Degraded, &[]).unwrap(),
            UasConfig::Disabled
        ));
    }

    #[test]
    fn invalid_uas_settings_fail_instead_of_degrading() {
        for (key, value) in [
            ("UAS_REDIS_URL", "invalid://localhost"),
            ("UAS_REDIS_KEY_PREFIX", " "),
            ("UAS_REDIS_KEY_PREFIX", "a{b}"),
            ("UAS_MAX_ACTIONS", "0"),
            ("UAS_MAX_ACTIONS", "many"),
            ("UAS_REDIS_TTL_SECS", "-1"),
            ("UAS_REDIS_CONNECT_TIMEOUT_MS", "0"),
            ("UAS_REDIS_REQUEST_TIMEOUT_MS", "invalid"),
        ] {
            let mut values = vec![("UAS_REDIS_URL", "redis://localhost:6379/")];
            if key == "UAS_REDIS_URL" {
                values.clear();
            }
            values.push((key, value));
            assert!(
                uas_config(HomeMixerMode::Degraded, &values).is_err(),
                "{key}={value}"
            );
            assert!(
                UasConfig::redis_from_lookup(|name| lookup_in(&values, name)).is_err(),
                "worker: {key}={value}"
            );
        }
    }

    #[test]
    fn the_projection_job_requires_a_redis_target() {
        let error = UasConfig::redis_from_lookup(|_| None).unwrap_err();
        assert!(error.to_string().contains("UAS_REDIS_URL"));
        assert!(error.to_string().contains("HOME_MIXER_REDIS_URL"));

        let config = UasConfig::redis_from_lookup(|name| {
            lookup_in(&[("HOME_MIXER_REDIS_URL", "redis://shared:6379/")], name)
        })
        .unwrap();
        assert_eq!(config.url, "redis://shared:6379/");
    }

    #[test]
    fn removed_demo_configuration_is_rejected() {
        let explicit = HomeMixerMode::from_values(Some("demo"), false).unwrap_err();
        assert!(explicit.to_string().contains("demo mode has been removed"));

        let legacy = HomeMixerMode::from_values(None, true).unwrap_err();
        assert!(legacy.to_string().contains("HOME_MIXER_DEMO"));
        assert!(legacy.to_string().contains("has been removed"));

        assert!(HomeMixerMode::from_values(Some("production_ready"), true).is_err());
        assert!(HomeMixerMode::from_values(Some("degraded"), true).is_err());
    }

    #[test]
    fn request_timeout_defaults_and_rejects_non_positive_overrides() {
        assert_eq!(
            request_timeout_from_lookup(|_| None).unwrap(),
            Duration::from_millis(crate::params::REQUEST_TIMEOUT_MS)
        );
        assert_eq!(
            request_timeout_from_lookup(|name| lookup_in(&[(REQUEST_TIMEOUT_ENV, " 2500 ")], name))
                .unwrap(),
            Duration::from_millis(2_500)
        );
        for value in ["0", "-1", "fast", ""] {
            let result = request_timeout_from_lookup(|name| {
                lookup_in(&[(REQUEST_TIMEOUT_ENV, value)], name)
            });
            if value.is_empty() {
                assert!(result.is_ok(), "blank falls back to the default");
            } else {
                assert!(result.is_err(), "{value:?}");
            }
        }
    }
}
