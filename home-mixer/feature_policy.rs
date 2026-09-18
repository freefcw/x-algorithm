/// What the VF filter does with a candidate whose visibility could not be
/// established, either because the adapter failed or because it returned no
/// verdict for that post.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum VfFailurePolicy {
    /// Unverified is not approved: drop the candidate.
    #[default]
    FailClosed,
    /// Keep unverified in-network posts, drop unverified out-of-network ones.
    InNetworkOnly,
    /// Keep every unverified candidate. Only appropriate where no viewer-level
    /// visibility contract is expected to exist at all.
    AllowAll,
}

impl VfFailurePolicy {
    fn from_value(value: Option<String>) -> Self {
        let Some(value) = value else {
            return Self::FailClosed;
        };
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "fail_closed" => Self::FailClosed,
            "in_network_only" => Self::InNetworkOnly,
            "allow_all" => Self::AllowAll,
            value => {
                log::warn!(
                    "invalid HOME_MIXER_VF_FAILURE_POLICY={value:?}; defaulting to fail_closed"
                );
                Self::FailClosed
            }
        }
    }
}

/// Optional integrations that are not required for the primary recommendation path.
///
/// Boolean flags default to false. Enabling one is an operator action: the
/// corresponding public service contract, credentials, endpoint, timeout, and
/// fallback behavior must be verified first.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HomeMixerFeatures {
    pub phoenix_moe: bool,
    pub request_cache_side_effect: bool,
    pub debug_rpc: bool,
    /// VM Ranker 二次重排（RANK-03）；还需 `VM_RANKER_GRPC_ADDR` 指向
    /// 本仓库 vm-ranker 服务实例，缺地址时旁路自动禁用。
    pub vm_ranker: bool,
    /// 新作者冷启动提升；缺曝光数或作者粉丝数的候选严格不参与。
    pub author_cold_start: bool,
    /// 仅在 author_cold_start 同时启用时生效。
    pub cold_start_thompson_sampling: bool,
    pub vf_failure_policy: VfFailurePolicy,
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
            vm_ranker: enabled(lookup("HOME_MIXER_ENABLE_VM_RANKER")),
            author_cold_start: enabled(lookup("HOME_MIXER_ENABLE_AUTHOR_COLD_START")),
            cold_start_thompson_sampling: enabled(lookup(
                "HOME_MIXER_ENABLE_COLD_START_THOMPSON_SAMPLING",
            )),
            vf_failure_policy: VfFailurePolicy::from_value(lookup("HOME_MIXER_VF_FAILURE_POLICY")),
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
        assert!(features.author_cold_start);
        assert!(features.cold_start_thompson_sampling);
    }

    #[test]
    fn vf_failure_policy_defaults_to_fail_closed() {
        assert_eq!(
            VfFailurePolicy::from_value(None),
            VfFailurePolicy::FailClosed
        );
        for value in ["", "   ", "FAIL_CLOSED", " fail_closed "] {
            assert_eq!(
                VfFailurePolicy::from_value(Some(value.to_string())),
                VfFailurePolicy::FailClosed,
                "{value:?}"
            );
        }
        assert_eq!(
            HomeMixerFeatures::from_lookup(|_| None).vf_failure_policy,
            VfFailurePolicy::FailClosed
        );
    }

    #[test]
    fn vf_failure_policy_allow_all_requires_an_explicit_opt_in() {
        for value in ["allow_all", "ALLOW_ALL", " Allow_All "] {
            assert_eq!(
                VfFailurePolicy::from_value(Some(value.to_string())),
                VfFailurePolicy::AllowAll,
                "{value:?}"
            );
        }
    }

    #[test]
    fn vf_failure_policy_parses_in_network_only_case_insensitively() {
        for value in ["in_network_only", "IN_NETWORK_ONLY", " In_Network_Only "] {
            assert_eq!(
                VfFailurePolicy::from_value(Some(value.to_string())),
                VfFailurePolicy::InNetworkOnly,
                "{value:?}"
            );
        }
    }

    #[test]
    fn vf_failure_policy_unknown_value_falls_back_to_fail_closed() {
        assert_eq!(
            VfFailurePolicy::from_value(Some("bogus".to_string())),
            VfFailurePolicy::FailClosed
        );
    }
}
