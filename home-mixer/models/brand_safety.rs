use x_algorithm_proto::home_mixer as pb;

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub enum BrandSafetyVerdict {
    #[default]
    Unspecified,
    Safe,
    LowRisk,
    MediumRisk,
}

impl From<BrandSafetyVerdict> for pb::BrandSafetyVerdict {
    fn from(verdict: BrandSafetyVerdict) -> Self {
        match verdict {
            BrandSafetyVerdict::Unspecified => Self::Unspecified,
            BrandSafetyVerdict::Safe => Self::SafeForAdjacency,
            BrandSafetyVerdict::LowRisk => Self::LowRisk,
            BrandSafetyVerdict::MediumRisk => Self::AvoidAdjacency,
        }
    }
}
