use std::fmt;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SnowflakeId(u64);

impl SnowflakeId {
    pub fn new(value: u64) -> Result<Self, String> {
        if value == 0 || value > i64::MAX as u64 {
            return Err(format!("unsupported Snowflake id {value}"));
        }
        Ok(Self(value))
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for SnowflakeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
