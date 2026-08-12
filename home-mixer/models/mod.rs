pub mod brand_safety;
pub mod candidate;
pub mod candidate_features;
pub mod query;
pub mod user_features;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compatibility_paths_resolve_to_canonical_model_types() {
        let candidate = crate::candidate_pipeline::candidate::PostCandidate::default();
        let _: candidate::PostCandidate = candidate;

        let query = crate::candidate_pipeline::query::ScoredPostsQuery::default();
        let _: query::ScoredPostsQuery = query;

        let features = crate::candidate_pipeline::query_features::UserFeatures::default();
        let _: user_features::UserFeatures = features;

        let core_data = crate::candidate_pipeline::candidate_features::PureCoreData::default();
        let _: candidate_features::PureCoreData = core_data;

        let verdict = crate::candidate_pipeline::candidate::BrandSafetyVerdict::Safe;
        let _: brand_safety::BrandSafetyVerdict = verdict;
    }
}
