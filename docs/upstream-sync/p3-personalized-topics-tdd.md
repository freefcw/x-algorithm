# Hai TDD: 话题合同与装配边界修复

## Target Behavior
显式话题页保持严格召回；公开 `new_user_topic_ids` 保持上游冷启动限定语义；首页补充话题只有在显式注入 Adapter 时才使用 Blend，且 Home Mixer 不决定关注、推断、年龄或用户资格策略。

## Cycle 1: 恢复冷启动字段语义

### RED
- **Test added**: Query 的 Strict/ColdStart/Blend 来源优先级、Phoenix/MoE 启用规则、冷启动过滤和 OON 测试。
- **Behavior asserted**: `new_user_topic_ids` 进入独立 ColdStart 模式，关闭普通 Phoenix/MoE，只保留网内或命中冷启动话题的候选；内部补充话题继续混合召回。
- **Command**: `cargo test -p home-mixer candidate_pipeline::query::tests -- --nocapture`
- **Observed failure**: 编译出现 14 项错误，缺少 `new_user_topic_ids`、`supplemental_topic_ids` 和 `TopicRecallMode::ColdStart`。
- **Failure is correct because**: 当前领域模型把公开冷启动字段改名为补充话题，无法区分两种业务来源。

### GREEN
- **Minimal implementation**: Query 增加独立冷启动和补充话题字段；proto 字段直映射回冷启动；Source、Filter、Scorer 按集中派生模式执行对应规则。
- **Command**: `cargo test -p home-mixer new_user_topics -- --nocapture`
- **Observed pass**: 5 项冷启动行为测试通过；Query 模式测试 4 项通过。

### REFACTOR
- **Refactor done**: yes
- **Change**: 所有组件继续只依赖 `topic_recall_mode()` 和 `selected_topic_ids()`，没有把来源判断重新散回字段检查。
- **Command after refactor**: `cargo test -p home-mixer topic -- --nocapture`
- **Observed result**: 25 项话题相关测试通过。
- **Additional RED/GREEN**: 冷启动候选只携带 `retrieval_topic_ids` 时，测试先得到 `[1, 2, 4]` 而期望 `[1, 2]`；改为只信任 `filtered_topic_ids` 后通过，与上游 `NewUserTopicIdsFilter` 一致。

## Cycle 2: 由 Adapter 拥有补充话题策略

### RED
- **Test added**: `adapter_selected_topics_are_used_without_home_mixer_policy`、排除去重、已有话题来源跳过和失败降级。
- **Behavior asserted**: Reader 直接返回已选补充话题，Home Mixer 不接收原始关注/推断画像，也不实现优先级或资格判断。
- **Command**: `cargo test -p home-mixer query_hydrators::user_topics_query_hydrator::tests -- --nocapture`
- **Observed failure**: 2 项编译错误；`get_supplemental_topic_ids` 不属于旧 trait，旧 trait 仍要求 `get_interests`。
- **Failure is correct because**: 产品选择策略仍泄漏在 Home Mixer 的 Reader 数据形状和 Hydrator 中。

### GREEN
- **Minimal implementation**: `UserTopicReader` 收窄为返回最终补充话题 ID；Hydrator 仅执行排除、去重和 fail-open 错误传播。
- **Command**: `cargo test -p home-mixer query_hydrators::user_topics_query_hydrator::tests -- --nocapture`
- **Observed pass**: 4 项 Hydrator 测试通过；外部 Reader 合同测试通过。

### REFACTOR
- **Refactor done**: yes
- **Change**: 删除 `UserTopicInterests` 和关注优先/推断兜底术语，显式依赖注入成为唯一启用开关。
- **Command after refactor**: `cargo test -p home-mixer topic -- --nocapture`
- **Observed result**: 25 项话题相关测试通过。

## Cycle 3: 打通外部 Adapter 到服务的装配链

### RED
- **Test added**: `external_topic_adapters_can_enter_service_assembly`。
- **Behavior asserted**: 外部 Reader/Retriever 可组成原子依赖，进入 Pipeline，再进入 ScoredPosts 和 HomeMixer 服务。
- **Command**: `cargo test -p home-mixer --test user_topic_reader_contract -- --nocapture`
- **Observed failure**: 3 项编译错误；缺少公开 Topic 依赖类型、Pipeline 构造、`with_pipeline` 和 `with_scored_posts_server`。
- **Failure is correct because**: 旧合同只能实现 trait，无法进入真实服务装配。

### GREEN
- **Minimal implementation**: 公开根级 `TopicPersonalizationClients` 和 `PhoenixCandidatePipeline`；增加带 Topic 依赖的生产构造以及 Pipeline 到两个服务层的窄装配入口。
- **Command**: `cargo test -p home-mixer --test user_topic_reader_contract -- --nocapture`
- **Observed pass**: 2 项外部合同测试通过。

### REFACTOR
- **Refactor done**: yes
- **Change**: 十参数 `build_with_clients` 和内部模块继续私有，只公开稳定的原子 Topic 依赖与服务装配接口。
- **Command after refactor**: `cargo test -p home-mixer`
- **Observed result**: 6 个套件、86 项通过。

## Next Behavior
生产 Reader/Retriever 的网络协议、认证、时效和资格规则仍需外部合同；在合同到位前保持默认不装配。
