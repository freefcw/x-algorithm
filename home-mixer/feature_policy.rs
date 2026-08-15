/// Optional integrations that are not required for the primary recommendation path.
///
/// Every flag defaults to false. Enabling one is an operator action: the
/// corresponding public service contract, credentials, endpoint, timeout, and
/// fallback behavior must be verified first.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HomeMixerFeatures {
    pub phoenix_moe: bool,
    pub request_cache_side_effect: bool,
    pub debug_rpc: bool,
    pub unsigned_cached_posts: bool,
    /// VM Ranker 二次重排（RANK-03）；还需 `VM_RANKER_GRPC_ADDR` 指向
    /// 本仓库 vm-ranker 服务实例，缺地址时旁路自动禁用。
    pub vm_ranker: bool,
    /// 新作者冷启动提升；缺曝光数或作者粉丝数的候选严格不参与。
    pub author_cold_start: bool,
    /// 仅在 author_cold_start 同时启用时生效。
    pub cold_start_thompson_sampling: bool,
}

impl HomeMixerFeatures {
    pub fn from_env() -> Self {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Self {
        Self {
            phoenix_moe: enabled(lookup("HOME_MIXER_ENABLE_PHOENIX_MOE")),
            request_cache_side_effect: enabled(lookup(
                "HOME_MIXER_ENABLE_REQUEST_CACHE_SIDE_EFFECT",
            )),
            debug_rpc: enabled(lookup("HOME_MIXER_ENABLE_DEBUG_RPC")),
            unsigned_cached_posts: enabled(lookup("HOME_MIXER_ENABLE_UNSIGNED_CACHED_POSTS")),
            vm_ranker: enabled(lookup("HOME_MIXER_ENABLE_VM_RANKER")),
            author_cold_start: enabled(lookup("HOME_MIXER_ENABLE_AUTHOR_COLD_START")),
            cold_start_thompson_sampling: enabled(lookup(
                "HOME_MIXER_ENABLE_COLD_START_THOMPSON_SAMPLING",
            )),
        }
    }
}

fn enabled(value: Option<String>) -> bool {
    value.is_some_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn optional_integrations_are_disabled_by_default() {
        assert_eq!(
            HomeMixerFeatures::default(),
            HomeMixerFeatures::from_lookup(|_| None)
        );
    }

    #[test]
    fn only_explicit_true_values_enable_integrations() {
        let values = HashMap::from([
            ("HOME_MIXER_ENABLE_PHOENIX_MOE", "true".to_string()),
            (
                "HOME_MIXER_ENABLE_REQUEST_CACHE_SIDE_EFFECT",
                "0".to_string(),
            ),
            ("HOME_MIXER_ENABLE_DEBUG_RPC", "yes".to_string()),
            ("HOME_MIXER_ENABLE_UNSIGNED_CACHED_POSTS", "off".to_string()),
            ("HOME_MIXER_ENABLE_AUTHOR_COLD_START", "true".to_string()),
            (
                "HOME_MIXER_ENABLE_COLD_START_THOMPSON_SAMPLING",
                "1".to_string(),
            ),
        ]);
        let features = HomeMixerFeatures::from_lookup(|key| values.get(key).cloned());

        assert!(features.phoenix_moe);
        assert!(!features.request_cache_side_effect);
        assert!(features.debug_rpc);
        assert!(!features.unsigned_cached_posts);
        assert!(features.author_cold_start);
        assert!(features.cold_start_thompson_sampling);
    }
}
