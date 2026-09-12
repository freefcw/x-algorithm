// 用户行为序列 (UAS) 兼容层
//
// 替代原始 `xai_uas_thrift` 私有 crate。
//
// 原始功能说明：
// X 的用户行为序列 (User Action Sequence, UAS) 系统是推荐算法
// 最核心的特征输入之一。它记录了用户最近的互动行为序列：
//   - 用户看了哪些帖子
//   - 在每条帖子上执行了哪些行为（点赞、回复、转发等）
//   - 行为发生的时间戳
//
// 数据流向:
//   用户行为 → Kafka → UAS 存储服务 → Home Mixer 查询 →
//   聚合处理 → Protobuf 格式 → 发送给 Phoenix 模型
//
// 原始实现使用 Apache Thrift 作为内部序列化格式，
// Home Mixer 从 UAS 存储服务获取 Thrift 格式数据后，
// 聚合处理并转换为 Protobuf 发送给 Phoenix。
//
// 当前为 stub 实现，定义了必要的 Thrift 兼容类型。
// TODO: 当你的平台用户行为追踪系统就绪后，对接真实数据

/// Thrift 格式的用户行为序列元数据
///
/// 记录 UAS 的版本信息和时间戳，用于增量更新和数据一致性校验
#[derive(Clone, Debug, Default)]
pub struct UserActionSequenceMeta {
    /// 序列最后修改时间（毫秒 epoch）
    pub last_modified_epoch_ms: Option<i64>,
    /// 上次 Kafka 发布时间（毫秒 epoch）
    /// 用于判断数据新鲜度
    pub last_kafka_publish_epoch_ms: Option<i64>,
}

/// Thrift 格式的单条用户行为记录
///
/// 代表用户对某条帖子的一次完整互动快照
#[derive(Clone, Debug, Default)]
pub struct UserAction {
    /// 帖子 ID
    pub tweet_id: Option<crate::models::PostId>,
    /// 帖子作者 ID
    pub author_id: Option<crate::models::UserId>,
    /// 曝光/互动时间（毫秒 epoch）
    pub action_time_ms: Option<i64>,
    /// 行为类型代码
    pub action_type: Option<i32>,
}

/// Thrift 格式的聚合用户行为
///
/// 将同一帖子上的多次行为聚合为一条记录
/// 例如：用户先看了帖子（曝光），然后点赞，再转发
/// 这三个行为会被聚合为一条 AggregatedUserAction
#[derive(Clone, Debug, Default)]
pub struct AggregatedUserAction {
    /// 帖子 ID
    pub tweet_id: Option<crate::models::PostId>,
    /// 作者 ID
    pub author_id: Option<crate::models::UserId>,
    /// 最初曝光时间（毫秒 epoch）
    pub impressed_time_ms: Option<i64>,
    /// 行为位掩码 — 每个 bit 对应一种行为类型
    pub action_mask: Vec<bool>,
    /// 产品场景码（Timeline、Search 等）
    pub product_surface: Option<i32>,
}

/// Thrift 格式的完整用户行为序列
///
/// 包含元数据和行为记录列表
#[derive(Clone, Debug, Default)]
pub struct UserActionSequence {
    /// 序列元数据
    pub metadata: Option<UserActionSequenceMeta>,
    /// 原始行为记录（未聚合）
    pub user_actions: Option<Vec<UserAction>>,
}

/// Thrift → Protobuf 聚合行为转换
///
/// 将内部 Thrift 格式的聚合行为记录转换为
/// proto 定义的 `recsys::AggregatedUserAction` 消息
///
/// # Arguments
/// * `thrift_action` - Thrift 格式的聚合行为
///
/// # Returns
/// Protobuf 格式的 `AggregatedUserAction`
///
/// # 原始实现说明
/// X 的原始转换代码位于 `xai_uas_thrift::convert` 模块中，
/// 处理了 Thrift 和 Protobuf 之间的各种类型差异。
/// 当前 stub 实现执行直接字段映射。
pub mod convert {
    use super::AggregatedUserAction;
    use x_algorithm_proto::recsys;

    pub fn thrift_to_proto_aggregated_user_action(
        thrift_action: AggregatedUserAction,
    ) -> Result<recsys::AggregatedUserAction, String> {
        let tweet_id = required_id(thrift_action.tweet_id, "tweet_id")?;
        let author_id = required_id(thrift_action.author_id, "author_id")?;
        let impressed_time_ms = thrift_action
            .impressed_time_ms
            .and_then(|value| u64::try_from(value).ok())
            .unwrap_or(0);
        Ok(recsys::AggregatedUserAction {
            tweet_id: tweet_id.to_string(),
            author_id: author_id.to_string(),
            impressed_time_ms,
            action_mask: thrift_action.action_mask,
            product_surface: thrift_action.product_surface.unwrap_or(0),
        })
    }

    fn required_id(
        value: Option<crate::models::ObjectId>,
        field: &str,
    ) -> Result<crate::models::ObjectId, String> {
        value
            .filter(|value| !value.is_nil())
            .ok_or_else(|| format!("AggregatedUserAction.{field} must be a non-nil ObjectId"))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn rejects_missing_nonpositive_or_negative_identity_fields() {
            for action in [
                AggregatedUserAction {
                    tweet_id: None,
                    author_id: Some(crate::models::uid(1)),
                    ..Default::default()
                },
                AggregatedUserAction {
                    tweet_id: Some(crate::models::pid(1)),
                    author_id: None,
                    ..Default::default()
                },
                AggregatedUserAction {
                    tweet_id: Some(crate::models::PostId::NIL),
                    author_id: Some(crate::models::uid(1)),
                    ..Default::default()
                },
            ] {
                assert!(thrift_to_proto_aggregated_user_action(action).is_err());
            }
        }

        #[test]
        fn negative_impression_time_degrades_to_zero_without_wrapping() {
            let converted = thrift_to_proto_aggregated_user_action(AggregatedUserAction {
                tweet_id: Some(crate::models::pid(1)),
                author_id: Some(crate::models::uid(2)),
                impressed_time_ms: Some(-1),
                ..Default::default()
            })
            .expect("valid identity fields");

            assert_eq!(converted.impressed_time_ms, 0);
        }
    }
}
