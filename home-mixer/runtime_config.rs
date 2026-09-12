use crate::feature_policy::HomeMixerFeatures;

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
}

impl HomeMixerConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let config = Self {
            mode: HomeMixerMode::from_env()?,
            features: HomeMixerFeatures::from_env(),
            debug_token: std::env::var("HOME_MIXER_DEBUG_TOKEN")
                .ok()
                .map(|token| token.trim().to_string()),
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
        if self.features.unsigned_cached_posts && self.mode != HomeMixerMode::Demo {
            anyhow::bail!("HOME_MIXER_ENABLE_UNSIGNED_CACHED_POSTS is allowed only in demo mode");
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
        };
        assert!(config.validate().is_err());

        config.mode = HomeMixerMode::Demo;
        assert!(config.validate().is_ok());
        config.mode = HomeMixerMode::ProductionReady;
        assert!(config.validate().is_err());
    }
}
