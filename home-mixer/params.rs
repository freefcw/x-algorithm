// Home Mixer 参数常量模块
//
// 本模块集中管理 Home Mixer 推荐管道中所有可调参数。
// 参数值从 X (Twitter) 开源代码中逆向推导而来，后续可替换为你平台的配置。
//
// 参数分为以下几组：
//   1. gRPC 服务配置
//   2. 召回相关参数
//   3. Phoenix 精排打分权重（核心排序公式）
//   4. 打分归一化参数
//   5. 多样性与降权参数
//   6. 用户行为序列 (UAS) 参数
//   7. 内容过滤参数
//   8. 管道输出参数

// =============================================================================
// 1. gRPC 服务配置
// =============================================================================

/// gRPC 消息体最大大小（字节）
/// 默认 16 MB，足够容纳大批量候选帖子的请求和响应
/// 用于 tonic Server 的 max_decoding_message_size / max_encoding_message_size
pub const MAX_GRPC_MESSAGE_SIZE: usize = 16 * 1024 * 1024; // 16 MB

// =============================================================================
// 2. 召回相关参数
// =============================================================================

/// Thunder（网络内召回）单次请求最大返回帖子数
/// Thunder 从用户关注列表中的内存缓存检索最新帖子，该值控制上限
/// 典型值: 500-1000，这里使用 500 作为合理默认值
pub const THUNDER_MAX_RESULTS: u32 = 500;
pub const THUNDER_REQUEST_TIMEOUT_MS: u64 = 500;
pub const UAS_FETCH_TIMEOUT_MS: u64 = 500;
pub const USER_FEATURES_FETCH_TIMEOUT_MS: u64 = 500;
pub const USER_TOPIC_READ_TIMEOUT_MS: u64 = 500;
pub const STRATO_WRITE_TIMEOUT_MS: u64 = 500;
pub const TES_REQUEST_TIMEOUT_MS: u64 = 500;
pub const GIZMODOUCK_REQUEST_TIMEOUT_MS: u64 = 500;

/// Phoenix（全局召回 / 双塔检索）单次请求最大返回帖子数
/// Phoenix 使用双塔模型从全局帖子池中检索与用户兴趣匹配的帖子
/// 典型值: 200-500，这里使用 300
pub const PHOENIX_MAX_RESULTS: u32 = 300;
pub const PHOENIX_RETRIEVAL_TIMEOUT_MS: u64 = 3_000;
pub const PHOENIX_PREDICTION_TIMEOUT_MS: u64 = 5_000;
pub const VF_REQUEST_TIMEOUT_MS: u64 = 500;

/// 显式话题和新用户冷启动话题的单次候选上限。
pub const TOPIC_MAX_RESULTS: usize = 100;
pub const TOPIC_RETRIEVAL_TIMEOUT_MS: u64 = 500;

// =============================================================================
// 3. Phoenix 精排打分权重（核心排序公式）
// =============================================================================
//
// 加权打分公式:
//   combined_score = Σ (P(action_i) × weight_i)
//
// 其中 P(action_i) 是 Phoenix 模型预测用户执行该行为的概率。
// 权重反映了平台对各种互动行为的价值评估。
//
// 正向行为（越大表示越重要）:
// - 点赞 (favorite): 核心正向信号，权重最高
// - 回复 (reply): 深度互动信号
// - 转发 (retweet): 扩散信号
// - 引用转发 (quote): 高质量互动，带有用户评论
// - 分享 (share): 外部传播信号
// - 关注 (follow_author): 强正向信号
// - 停留时间 (dwell): 内容消费深度
//
// 负向行为（使用负权重进行惩罚）:
// - 不感兴趣 (not_interested): 明确负信号
// - 拉黑 (block): 强负信号
// - 静音 (mute): 中等负信号
// - 举报 (report): 最强负信号
//
// 权重值参考了 X 公开的博客文章和开源代码中的比例关系。

/// 点赞 (Favorite/Like) 权重
/// X 公开信息中，点赞是最基础的正向信号，被赋予基准权重
pub const FAVORITE_WEIGHT: f64 = 0.5;

/// 回复 (Reply) 权重
/// 回复代表深度互动，在 X 的算法中权重约为点赞的 54 倍（原始值 27.0）
/// 这里使用更保守的值，适合初期平台
pub const REPLY_WEIGHT: f64 = 27.0;

/// 转发 (Retweet) 权重
/// 转发是内容扩散的核心机制，X 算法中给予较高权重
pub const RETWEET_WEIGHT: f64 = 1.0;

/// 图片展开 (Photo Expand) 权重
/// 用户主动展开图片表示对视觉内容的兴趣
pub const PHOTO_EXPAND_WEIGHT: f64 = 0.02;

/// 帖子点击 (Click) 权重
/// 点击进入帖子详情页，表示用户想了解更多
pub const CLICK_WEIGHT: f64 = 0.04;

/// 作者头像点击 (Profile Click) 权重
/// 点击作者头像进入个人主页，表示对作者感兴趣
pub const PROFILE_CLICK_WEIGHT: f64 = 0.02;

/// 视频有效观看 (Video Quality View / VQV) 权重
/// 仅当视频时长超过 MIN_VIDEO_DURATION_MS 时才应用此权重
pub const VQV_WEIGHT: f64 = 0.005;

/// 分享 (Share) 权重
/// 通过任何渠道分享帖子
pub const SHARE_WEIGHT: f64 = 1.0;

/// 通过私信分享 (Share via DM) 权重
/// 私信分享通常表示用户认为内容对特定朋友有价值
pub const SHARE_VIA_DM_WEIGHT: f64 = 1.0;

/// 复制链接分享 (Share via Copy Link) 权重
/// 用户复制链接到外部平台分享
pub const SHARE_VIA_COPY_LINK_WEIGHT: f64 = 1.0;

/// 停留 (Dwell) 权重
/// 用户在帖子上停留超过最低阈值（二值信号）
pub const DWELL_WEIGHT: f64 = 0.001;

/// 引用转发 (Quote Tweet) 权重
/// 带评论的转发，表示高质量互动
pub const QUOTE_WEIGHT: f64 = 1.0;

/// 引用帖点击 (Quoted Tweet Click) 权重
/// 点击引用帖中嵌入的原始帖子
pub const QUOTED_CLICK_WEIGHT: f64 = 0.02;

/// 连续停留时间 (Continuous Dwell Time) 权重
/// 与 DWELL_WEIGHT 不同，这是一个连续时间值（秒）的回归预测
pub const CONT_DWELL_TIME_WEIGHT: f64 = 0.0001;

/// 关注作者 (Follow Author) 权重
/// 用户在看到帖子后关注了该作者，非常强的正向信号
pub const FOLLOW_AUTHOR_WEIGHT: f64 = 1.0;

/// 不感兴趣 (Not Interested) 权重 — 负权重
/// 用户通过"不感兴趣"按钮明确表示不想看到此类内容
pub const NOT_INTERESTED_WEIGHT: f64 = -74.0;

/// 拉黑作者 (Block Author) 权重 — 负权重
/// 用户拉黑帖子作者，极强的负信号
pub const BLOCK_AUTHOR_WEIGHT: f64 = -74.0;

/// 静音作者 (Mute Author) 权重 — 负权重
/// 用户静音帖子作者
pub const MUTE_AUTHOR_WEIGHT: f64 = -74.0;

/// 举报 (Report) 权重 — 负权重
/// 用户举报帖子，最强的负信号
pub const REPORT_WEIGHT: f64 = -369.0;

/// 引用帖视频有效观看 (Quoted VQV) 权重
/// 用户对引用帖中嵌入视频的完整观看。与 VQV_WEIGHT 对应但作用于引用帖。
/// 当前发布 checkpoint 不输出此行为（模型 logits 仅有 0..=18），
/// 对应 PhoenixScores.quoted_vqv_score 为 None，apply(None, w) = 0。
pub const QUOTED_VQV_WEIGHT: f64 = 0.005;

/// 是否对引用帖 VQV 做时长门槛检查
/// 与 MIN_VIDEO_DURATION_MS 配合，时长不足的视频不计 VQV 权重。
pub const ENABLE_QUOTED_VQV_DURATION_CHECK: bool = true;

/// 未停留 (Not Dwelled) 权重 - 负权重
/// 用户快速划过帖子未停留，轻量负向信号（远弱于 NotInterested）。
/// 当前发布 checkpoint 不输出此行为，权重为预留。
pub const NOT_DWELLED_WEIGHT: f64 = -0.001;

/// 点击停留时长 (Click Dwell Time) 权重
/// 与 CONT_DWELL_TIME_WEIGHT 类似，预测点击后连续停留时长（秒）。
/// 当前模型 ContinuousActionName 仅有 DWELL_TIME，此权重为预留。
pub const CONT_CLICK_DWELL_TIME_WEIGHT: f64 = 0.0001;

// =============================================================================
// 4. 打分归一化参数
// =============================================================================

/// 所有正向权重之和，用于 offset_score 归一化计算
///
/// = 0.5 + 27.0 + 1.0 + 0.02 + 0.04 + 0.02 + 0.005 + 1.0 + 1.0 + 1.0 + 0.001
///   + 1.0 + 0.02 + 0.0001 + 1.0  (已启用权重)
///   + 0.005 + 0.0001             (quoted_vqv + cont_click_dwell_time，当前预留)
///
/// ≈ 33.6112
pub const WEIGHTS_SUM: f64 = 33.6112;

/// 所有负向权重之和（负值）
///
/// = -74.0 + -74.0 + -74.0 + -369.0 + -0.001
///   (not_interested + block + mute + report + not_dwelled)
/// = -591.001
pub const NEGATIVE_WEIGHTS_SUM: f64 = -591.001;

/// 负分偏移量
/// 用于将负分候选帖子映射到 [0, NEGATIVE_SCORES_OFFSET] 区间，
/// 确保所有帖子分数 >= 0，同时保持正分帖子始终高于负分帖子
/// 典型值: 一个较小的正数，作为"底线分数"
pub const NEGATIVE_SCORES_OFFSET: f64 = 1.0;

// =============================================================================
// 5. 多样性与降权参数
// =============================================================================

/// 网外 (Out-of-Network) 帖子权重因子
/// 来自用户未关注的作者的帖子，乘以此因子进行降权
/// 值 < 1.0 表示网外帖子得分降低，以优先显示关注者内容
/// X 在不同实验中使用 0.3-0.8 之间的值
pub const OON_WEIGHT_FACTOR: f64 = 0.5;

/// 显式话题 Feed 已由话题约束保证相关性，不额外惩罚网外作者。
pub const TOPIC_OON_WEIGHT_FACTOR: f64 = 1.0;

/// 作者多样性：连续出现同一作者时的衰减因子
/// 每当同一作者的帖子再次出现，分数乘以 decay^n（n 为该作者已出现的次数）
/// 值越小，同一作者的后续帖子被降权得越厉害
/// 0.5 表示第 2 条分数减半，第 3 条减到 1/4
pub const AUTHOR_DIVERSITY_DECAY: f64 = 0.5;

/// 作者多样性衰减的地板值
/// 防止同一作者的帖子分数被完全削减为 0
/// 最终乘数 = max((1-floor) * decay^n + floor, floor)
pub const AUTHOR_DIVERSITY_FLOOR: f64 = 0.1;

// =============================================================================
// 6. 用户行为序列 (UAS) 参数
// =============================================================================

/// UAS 时间窗口（毫秒）
/// 只保留最近 N 毫秒内的用户行为记录
/// 7 天 = 7 * 24 * 60 * 60 * 1000 = 604,800,000 毫秒
pub const UAS_WINDOW_TIME_MS: u64 = 7 * 24 * 60 * 60 * 1000; // 7 天

/// UAS 最大序列长度
/// 经过时间窗口过滤后，最多保留最近 N 条行为记录
/// 序列过长会影响 Phoenix 模型推理速度和内存
pub const UAS_MAX_SEQUENCE_LENGTH: usize = 300;

// =============================================================================
// 7. 内容过滤参数
// =============================================================================

/// 帖子最大年龄（秒）
/// 超过此年龄的帖子会被 AgeFilter 过滤掉
/// 48 小时 = 172800 秒
pub const MAX_POST_AGE: u64 = 48 * 60 * 60; // 48 小时

/// 视频有效播放最短时长（毫秒）
/// 只有超过此时长的视频才会被应用 VQV_WEIGHT
/// 2000 毫秒 = 2 秒
pub const MIN_VIDEO_DURATION_MS: i32 = 2000;

/// TweetMixer 网外召回单次最大候选数
/// 上游由 feature switch 配置；本地取与 Phoenix 召回一致的默认值
pub const TWEET_MIXER_MAX_RESULTS: u32 = 300;

// =============================================================================
// 8. 管道输出参数
// =============================================================================

/// TopK 选择器保留的候选帖子数量
/// 在所有打分完成后，选择得分最高的 N 条帖子
pub const TOP_K_CANDIDATES_TO_SELECT: usize = 100;

/// 最终返回给客户端的帖子数量
/// 即 ScoredPostsResponse 中 scored_posts 的最大长度
pub const RESULT_SIZE: usize = 50;

/// 本地 P5 适配器保留的最近已下发帖子数；生产持久化策略在集成阶段确定。
pub const LOCAL_SERVED_HISTORY_LIMIT: usize = 500;

/// 本地 P5 适配器保留的最近请求时间戳数。
pub const LOCAL_REQUEST_TIMESTAMP_LIMIT: usize = 50;

/// 本地 P5 适配器最多保留的用户数，避免进程内状态随 viewer 数无限增长。
pub const LOCAL_STATE_USER_LIMIT: usize = 10_000;
