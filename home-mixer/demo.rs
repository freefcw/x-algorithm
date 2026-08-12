// 演示模式开关
//
// 设置 HOME_MIXER_MODE=demo 后，装配层会把 Strato / UAS / TES 等
// 数据依赖换成 Demo 实现，返回一套自洽的演示数据，让整条推荐链路
// 可以在本地跑出非空结果：
//   - DemoStratoClient 返回固定关注列表（与 thunder --demo-seed-posts 的作者一致）
//   - DemoUserActionSequenceFetcher 返回一段模拟的用户行为序列
//   - DemoTESClient 返回模拟帖子文本（否则候选会被 CoreDataHydrationFilter 全部过滤）
//
// 该开关只影响装配时注入哪个实现，不改变管道逻辑本身。
// 演示数据的共享契约（账号集合、Snowflake 工具）在 `x_algorithm_proto::demo`。

/// Demo mode is selected by HOME_MIXER_MODE=demo. HOME_MIXER_DEMO=1 remains
/// as a compatibility alias for existing scripts.
pub fn is_demo_mode() -> bool {
    std::env::var("HOME_MIXER_MODE").is_ok_and(|value| value.trim().eq_ignore_ascii_case("demo"))
        || std::env::var("HOME_MIXER_DEMO").is_ok_and(|value| value == "1")
}
