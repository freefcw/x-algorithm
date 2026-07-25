use std::collections::HashSet;

pub trait FeatureSwitches: Send + Sync {
    fn enabled(&self, key: &str) -> bool;
}

#[derive(Clone, Debug, Default)]
pub struct StaticFeatureSwitches {
    enabled_keys: HashSet<String>,
}

impl StaticFeatureSwitches {
    pub fn new(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            enabled_keys: keys.into_iter().map(Into::into).collect(),
        }
    }
}

impl FeatureSwitches for StaticFeatureSwitches {
    fn enabled(&self, key: &str) -> bool {
        self.enabled_keys.contains(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_switches_are_disabled_unless_explicitly_enabled() {
        let switches = StaticFeatureSwitches::new(["phoenix_topics"]);

        assert!(switches.enabled("phoenix_topics"));
        assert!(!switches.enabled("phoenix_moe"));
        assert!(!StaticFeatureSwitches::default().enabled("phoenix_topics"));
    }
}
