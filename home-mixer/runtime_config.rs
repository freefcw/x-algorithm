use crate::clients::redis_feed_state_store::RedisFeedStateConfig;
use crate::feature_policy::{HomeMixerFeatures, VfFailurePolicy};
use std::time::Duration;

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
        let url = lookup("HOME_MIXER_REDIS_URL")
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let Some(url) = url else {
            let config = Self::InMemory;
            if mode == HomeMixerMode::ProductionReady {
                // Keep the mode-level readiness error as the primary startup
                // message; that mode is rejected before storage is built.
                return Ok(config);
            }
            config.validate(mode)?;
            return Ok(config);
        };
        let mut redis = RedisFeedStateConfig::new(url);
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

    pub fn validate(&self, mode: HomeMixerMode) -> anyhow::Result<()> {
        match self {
            Self::InMemory if mode != HomeMixerMode::Demo => anyhow::bail!(
                "HOME_MIXER_REDIS_URL is required outside demo mode; in-memory feed state cannot be shared across instances"
            ),
            Self::InMemory => Ok(()),
            Self::Redis(config) => config.validate().map_err(anyhow::Error::msg),
        }
    }
}

/// Runtime intent is explicit so a degraded local assembly cannot be mistaken
/// for a production-ready recommendation service.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum HomeMixerMode {
    Demo,
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
        let explicit_mode = explicit.map(Self::parse).transpose()?;
        if legacy_demo && explicit_mode.is_some_and(|mode| mode != Self::Demo) {
            anyhow::bail!(
                "HOME_MIXER_DEMO=1 conflicts with non-demo HOME_MIXER_MODE; remove the legacy variable"
            );
        }
        Ok(explicit_mode.unwrap_or(if legacy_demo {
            Self::Demo
        } else {
            Self::Degraded
        }))
    }

    fn parse(value: &str) -> anyhow::Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "demo" => Ok(Self::Demo),
            "degraded" => Ok(Self::Degraded),
            "production" | "production_ready" | "production-ready" => Ok(Self::ProductionReady),
            value => anyhow::bail!(
                "invalid HOME_MIXER_MODE={value}; expected demo, degraded, or production_ready"
            ),
        }
    }
}

/// Local replacement for the upstream service-builder configuration.
#[derive(Clone, Debug, Default)]
pub struct HomeMixerConfig {
    pub mode: HomeMixerMode,
    pub features: HomeMixerFeatures,
    pub debug_token: Option<String>,
    pub feed_state: FeedStateConfig,
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
            feed_state: FeedStateConfig::from_env(mode)?,
        };
        config.validate()?;
        Ok(config)
    }

    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        if self.mode == HomeMixerMode::ProductionReady {
            anyhow::bail!(
                "production_ready is unavailable: business adapter contracts are not verified (caller identity, TES, UAS, Strato, VF, in-network/fallback, Phoenix metadata, served persist)"
            );
        }
        self.feed_state.validate(self.mode)?;
        if self.features.unsigned_cached_posts && self.mode != HomeMixerMode::Demo {
            anyhow::bail!("HOME_MIXER_ENABLE_UNSIGNED_CACHED_POSTS is allowed only in demo mode");
        }
        if self.features.vf_failure_policy == VfFailurePolicy::AllowAll
            && self.mode != HomeMixerMode::Demo
        {
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
    fn only_demo_defaults_to_local_feed_state() {
        for values in [
            vec![],
            vec![("HOME_MIXER_REDIS_URL", "")],
            vec![("HOME_MIXER_REDIS_URL", "  ")],
        ] {
            assert!(matches!(
                feed_state_config(HomeMixerMode::Demo, &values).unwrap(),
                FeedStateConfig::InMemory
            ));
            let error = feed_state_config(HomeMixerMode::Degraded, &values).unwrap_err();
            assert!(error.to_string().contains("HOME_MIXER_REDIS_URL"));
        }
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
        assert!(config.validate().is_ok());
    }

    #[test]
    fn explicit_mode_cannot_be_overridden_by_legacy_demo_flag() {
        assert_eq!(
            HomeMixerMode::from_values(Some("demo"), true).expect("same mode"),
            HomeMixerMode::Demo
        );
        assert!(HomeMixerMode::from_values(Some("production_ready"), true).is_err());
        assert!(HomeMixerMode::from_values(Some("degraded"), true).is_err());
        assert_eq!(
            HomeMixerMode::from_values(None, true).expect("legacy demo"),
            HomeMixerMode::Demo
        );
    }

    #[test]
    fn sensitive_or_unready_modes_are_rejected() {
        let mut config = HomeMixerConfig {
            mode: HomeMixerMode::Degraded,
            features: HomeMixerFeatures {
                unsigned_cached_posts: true,
                ..Default::default()
            },
            debug_token: None,
            feed_state: FeedStateConfig::Redis(RedisFeedStateConfig::new("redis://localhost/")),
        };
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("HOME_MIXER_ENABLE_UNSIGNED_CACHED_POSTS"));

        config.mode = HomeMixerMode::Demo;
        assert!(config.validate().is_ok());
        config.mode = HomeMixerMode::ProductionReady;
        assert!(config.validate().is_err());
    }
}
