// Candidate Pipeline 通用工具模块
//
// 提供 pipeline 组件中共用的辅助函数。

/// 从 Rust 完整类型名中提取简短名称
///
/// 例如: `home_mixer::filters::vf_filter::VFFilter` → `VFFilter`
///
/// 用于在日志和 metrics 中显示易读的组件名称。
/// 返回 `&'static str` 因为输入来自 `std::any::type_name`（返回 `&'static str`）。
pub fn short_type_name(full_name: &'static str) -> &'static str {
    full_name.rsplit("::").next().unwrap_or(full_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_short_type_name() {
        // type_name returns &'static str, so we use a literal for testing
        let name: &'static str = "home_mixer::filters::vf_filter::VFFilter";
        assert_eq!(short_type_name(name), "VFFilter");
    }

    #[test]
    fn test_short_type_name_no_colons() {
        let name: &'static str = "VFFilter";
        assert_eq!(short_type_name(name), "VFFilter");
    }
}
