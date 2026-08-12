// 布隆过滤器（Bloom Filter）
//
// 客户端在发送推荐请求时，可以附带一个布隆过滤器，
// 包含用户最近已经看到的帖子 ID。
// Home Mixer 使用这个过滤器来去重，避免重复推送已读帖子。
//
// Protobuf 定义:
//   message ImpressionBloomFilterEntry {
//     bytes filter = 1;
//     uint32 hash_count = 2;
//   }
//
// 布隆过滤器原理：
//   - 一个 bit 数组 + 多个哈希函数
//   - 查询时，如果所有哈希函数位置的 bit 都为 1，则"可能存在"
//   - 如果任一位置为 0，则"一定不存在"
//   - 允许一定的 false positive，但没有 false negative

use x_algorithm_proto::home_mixer::ImpressionBloomFilterEntry;

/// 布隆过滤器包装
pub struct BloomFilter {
    /// 原始 bit 数组
    filter: Vec<u8>,
    /// 哈希函数数量
    hash_count: u32,
}

impl BloomFilter {
    /// 从 proto 的 ImpressionBloomFilterEntry 创建布隆过滤器
    pub fn from_entry(entry: &ImpressionBloomFilterEntry) -> Self {
        Self {
            filter: entry.data.clone(),
            hash_count: u32::try_from(entry.num_hash_functions).unwrap_or(0),
        }
    }

    /// 检查帖子 ID 是否可能存在于过滤器中
    ///
    /// # Arguments
    /// * `post_id` - 帖子 ID
    ///
    /// # Returns
    /// true 如果帖子可能已被看过（可能有 false positive）
    /// false 如果帖子一定没被看过
    pub fn may_contain(&self, post_id: u64) -> bool {
        if self.filter.is_empty() || self.hash_count == 0 {
            return false;
        }

        let bit_count = self.filter.len() * 8;
        let bit_count_u64 = u64::try_from(bit_count).unwrap_or(u64::MAX);

        // 使用 double hashing: h(i) = h1 + i * h2
        let h1 = murmur_hash(post_id, 0);
        let h2 = murmur_hash(post_id, h1);

        for i in 0..u64::from(self.hash_count) {
            let bit_index_u64 = h1.wrapping_add(i.wrapping_mul(h2)) % bit_count_u64;
            let bit_index = usize::try_from(bit_index_u64).expect("index is below bit_count");
            let byte_index = bit_index / 8;
            let bit_offset = bit_index % 8;

            if (self.filter[byte_index] >> bit_offset) & 1 == 0 {
                return false;
            }
        }
        true
    }
}

/// 简化的 Murmur 哈希
fn murmur_hash(key: u64, seed: u64) -> u64 {
    let mut h = seed;
    let k = key;

    let k = k.wrapping_mul(0xcc9e2d51);
    let k = k.rotate_left(15);
    let k = k.wrapping_mul(0x1b873593);

    h ^= k;
    h = h.rotate_left(13);
    h = h.wrapping_mul(5).wrapping_add(0xe6546b64);

    // Finalization
    h ^= 8;
    h ^= h >> 16;
    h = h.wrapping_mul(0x85ebca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2ae35);
    h ^= h >> 16;

    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_filter() {
        let filter = BloomFilter {
            filter: vec![],
            hash_count: 3,
        };
        assert!(!filter.may_contain(12345));
    }

    #[test]
    fn test_zero_hash_count() {
        let filter = BloomFilter {
            filter: vec![0xFF],
            hash_count: 0,
        };
        assert!(!filter.may_contain(12345));
    }
}
