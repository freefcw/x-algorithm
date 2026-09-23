//! §6.1 静默 fail-open 的金丝雀：验证「皮 A 拉黑了作者 B」这条**已知**关系
//! 能穿过 `GetViewerRelations` 合同出现在 Strato 端口的输出里。
//!
//! 背景见 `docs/implementation/mrpyq-member-dimension-requirements.md` §6.1：
//! `MrpyqStratoClient` 把皮的 `member_id` 放进合同里名叫 `account_id` 的字段位。
//! 如果 mrpyq / rec-bff 任何一侧先改了存储键语义而另一侧没跟上，接口**不报
//! 错**，名单静默变空，被拉黑的作者会重新进入 feed（fail-open）。监控指标分
//! 不出「没人拉黑」和「拉黑了但读不到」——两者都是空名单；只有一对预置了拉
//! 黑关系的测试皮能把这种失效变成显性失败。
//!
//! 需要可达的 rec-bff / `ViewerRelationService` 端点和一对预置好拉黑关系的
//! 测试皮，因此默认 `#[ignore]`。在 staging 做上线顺序校验（§6.1）时运行：
//!
//! ```sh
//! VIEWER_RELATION_CANARY_ADDR=http://<rec-bff>:9000 \
//! VIEWER_RELATION_CANARY_VIEWER_ID=<拉黑方皮的 member_id> \
//! VIEWER_RELATION_CANARY_BLOCKED_AUTHOR_ID=<被拉黑作者的 member_id> \
//! cargo test -p home-mixer --test viewer_relation_canary -- --ignored --nocapture
//! ```
//!
//! 断言失败 = 发生了 §6.1 描述的静默 fail-open（名单非空事实丢失），应立即
//! 停止发布并核对两侧键语义。调用本身报错则是 fail-closed（空 feed），不在
//! 本测试的抓捕范围，按服务可达性排查。

use std::str::FromStr;
use std::time::Duration;

use home_mixer::clients::mrpyq_adapters::MrpyqStratoClient;
use home_mixer::clients::mrpyq_recommendation_data_client::MrpyqRecommendationDataConfig;
use home_mixer::clients::mrpyq_viewer_relation_client::viewer_relation_client_from_config;
use home_mixer::clients::strato_client::StratoClient;
use home_mixer::models::user_features::UserFeatures;
use home_mixer::models::UserId;

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        panic!(
            "缺少环境变量 {name}。金丝雀需要一对预置了拉黑关系的测试皮和一个可达的 \
             rec-bff 地址，运行方式见本文件头部注释。"
        )
    })
}

fn member_id(env_name: &str) -> UserId {
    let hex = required_env(env_name);
    UserId::from_str(&hex).unwrap_or_else(|error| {
        panic!("环境变量 {env_name} 不是合法的皮 member_id（24 位小写 hex）：{error}")
    })
}

#[tokio::test]
#[ignore = "needs a live ViewerRelationService endpoint and a pre-seeded block relation"]
async fn seeded_block_relation_survives_the_account_id_field_position() {
    let address = required_env("VIEWER_RELATION_CANARY_ADDR");
    let viewer = member_id("VIEWER_RELATION_CANARY_VIEWER_ID");
    let blocked_author = member_id("VIEWER_RELATION_CANARY_BLOCKED_AUTHOR_ID");

    let config = MrpyqRecommendationDataConfig {
        address: Some(address),
        timeout: Duration::from_secs(5),
    };
    // 与生产装配同一条路径：同一个 gRPC 客户端、同一个 Strato 端口实现，
    // get_user_features 的返回就是准入过滤器（AuthorSocialgraphFilter 等）
    // 消费的输入，不经过简化或旁路。
    let strato = MrpyqStratoClient::new(
        viewer_relation_client_from_config(&config).expect("viewer relation client builds"),
    );

    let resolver = std::sync::Arc::new(home_mixer::id::PaddedIdentityResolver::new());
    let reader = std::sync::Arc::new(home_mixer::id::IdentityContext::new(
        resolver.clone() as home_mixer::id::SharedIdentityReader
    ));
    let identity = std::sync::Arc::new(home_mixer::id::IdentityRegistrationContext::new(
        reader,
        resolver as home_mixer::id::SharedIdentityIngress,
    ));
    let payload = strato.get_user_features(viewer, identity).await.expect(
        "GetViewerRelations 调用失败。这是 fail-closed（准入过滤器会清空候选、\
             feed 变空），不是本测试要抓的静默 fail-open；先排查端点可达性与 \
             rec-bff 状态，再重跑本测试",
    );
    let features: UserFeatures =
        serde_json::from_slice(&payload).expect("strato payload decodes as UserFeatures");

    assert!(
        features.blocked_user_ids.contains(&blocked_author),
        "已知拉黑关系 {viewer} -> {blocked_author} 没有出现在 blocked_user_ids 中，\
         但 GetViewerRelations 调用成功返回了。这正是 mrpyq-member-dimension-\
         requirements.md §6.1 描述的静默 fail-open：键语义错位导致名单读空、\
         该拦的内容全部放行且无告警。立即停止发布，核对 mrpyq 存储键与 \
         rec-bff 翻译层、推荐侧字段位三者的皮 / 账号语义是否一致",
    );
}
