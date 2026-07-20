// 演示模式开关
//
// 设置环境变量 HOME_MIXER_DEMO=1 后，装配层（phoenix_candidate_pipeline::prod）
// 会把 Strato / UAS / TES 三个客户端换成 Demo 实现，返回一套自洽的演示数据，
// 让整条推荐链路可以在本地跑出非空结果：
//   - DemoStratoClient 返回固定关注列表（与 thunder --demo-seed-posts 的作者一致）
//   - DemoUserActionSequenceFetcher 返回一段模拟的用户行为序列
//   - DemoTESClient 返回模拟帖子文本（否则候选会被 CoreDataHydrationFilter 全部过滤）
//
// 该开关只影响装配时注入哪个实现，不改变管道逻辑本身。
// 演示数据的共享契约（账号集合、Snowflake 工具）在 `x_algorithm_proto::demo`。

/// 演示模式是否开启（HOME_MIXER_DEMO=1）。只应在装配层读取。
pub fn is_demo_mode() -> bool {
    std::env::var("HOME_MIXER_DEMO")
        .map(|v| v == "1")
        .unwrap_or(false)
}
