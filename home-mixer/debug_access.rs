use std::sync::Arc;
use tonic::metadata::MetadataMap;
use tonic::Status;

const DEBUG_TOKEN_HEADER: &str = "x-home-mixer-debug-token";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DebugAccessError {
    Disabled,
    PermissionDenied,
}

impl DebugAccessError {
    pub(crate) fn into_status(self) -> Status {
        match self {
            Self::Disabled => Status::unavailable("DebugScoredPosts is disabled"),
            Self::PermissionDenied => Status::permission_denied("invalid debug access token"),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct DebugAccessPolicy {
    token: Option<Arc<str>>,
}

impl DebugAccessPolicy {
    pub(crate) fn new(enabled: bool, token: Option<String>) -> Self {
        Self {
            token: enabled.then(|| Arc::<str>::from(token.expect("validated debug token"))),
        }
    }

    pub(crate) fn authorize(&self, metadata: &MetadataMap) -> Result<(), DebugAccessError> {
        let Some(expected) = &self.token else {
            return Err(DebugAccessError::Disabled);
        };
        let supplied = metadata
            .get(DEBUG_TOKEN_HEADER)
            .and_then(|value| value.to_str().ok());
        if supplied == Some(expected.as_ref()) {
            Ok(())
        } else {
            Err(DebugAccessError::PermissionDenied)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_and_token_protected_debug_access_are_distinct() {
        let policy = DebugAccessPolicy::default();
        assert_eq!(
            policy
                .authorize(&MetadataMap::new())
                .expect_err("debug disabled"),
            DebugAccessError::Disabled
        );

        let policy = DebugAccessPolicy::new(true, Some("secret".to_string()));
        let mut metadata = MetadataMap::new();
        assert_eq!(
            policy.authorize(&metadata).expect_err("missing token"),
            DebugAccessError::PermissionDenied
        );
        metadata.insert(
            DEBUG_TOKEN_HEADER,
            "secret".parse().expect("metadata token"),
        );
        policy
            .authorize(&metadata)
            .expect("authorized debug request");
    }
}
