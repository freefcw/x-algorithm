use std::collections::HashMap;
use std::hash::Hash;

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Composition {
    pub size: usize,
    pub unique: usize,
    pub max_count: usize,
    pub hhi: f64,
    pub entropy_norm: f64,
}

impl Composition {
    pub fn from_keys<K: Hash + Eq>(keys: impl IntoIterator<Item = K>) -> Self {
        let mut counts: HashMap<K, usize> = HashMap::new();
        for key in keys {
            *counts.entry(key).or_insert(0) += 1;
        }
        Self::from_counts(counts.values().copied())
    }

    pub fn from_counts(counts: impl IntoIterator<Item = usize>) -> Self {
        let counts: Vec<usize> = counts.into_iter().filter(|&count| count > 0).collect();
        let size: usize = counts.iter().sum();
        if size == 0 {
            return Self::default();
        }

        let unique = counts.len();
        let max_count = counts.iter().copied().max().unwrap_or(0);
        let size_f64 = size as f64;
        let mut hhi = 0.0;
        let mut entropy = 0.0;
        for count in counts {
            let share = count as f64 / size_f64;
            hhi += share * share;
            entropy -= share * share.ln();
        }
        let entropy_norm = if size > 1 {
            entropy / size_f64.ln()
        } else {
            1.0
        };

        Self {
            size,
            unique,
            max_count,
            hhi,
            entropy_norm,
        }
    }

    pub fn unique_ratio(&self) -> f64 {
        if self.size == 0 {
            0.0
        } else {
            self.unique as f64 / self.size as f64
        }
    }

    pub fn max_share(&self) -> f64 {
        if self.size == 0 {
            0.0
        } else {
            self.max_count as f64 / self.size as f64
        }
    }
}
