// 上游 `47c1bcd` home-mixer/params/config.rs 的本地对应。
//
// 第一区是上游常量真值（U0）；第二区是本地运行环境常量（U1/U2），
// 上游对应实现依赖内部部署或私有服务，本地保留显式适配值并注明差异。
// 上游文件中的 FS_PATH、decider 路径、TEST/TRACE_USER_IDS 依赖内部配置
// 体系，本地不落地（U3）。

// =============================================================================
// 上游常量真值（U0）
// =============================================================================

/// 上游 128 MiB；配合大候选批量的请求与响应。
pub const MAX_GRPC_MESSAGE_SIZE: usize = 128 * 1024 * 1024;

/// TopK 选择器保留的候选数（上游 50）。
pub const TOP_K_CANDIDATES_TO_SELECT: usize = 50;

/// 最终帖子结果条数（上游 35；ForYou 上限为帖子 + 模块槽位）。
pub const RESULT_SIZE: usize = 35;

/// Who to Follow 模块插入位置（上游 6）。
pub const WHO_TO_FOLLOW_POSITION: usize = 6;

/// 帖子最大年龄（秒），超过被 AgeFilter 删除（上游 48 小时）。
pub const MAX_POST_AGE: u64 = 48 * 60 * 60;

/// 新用户网外降权因子（上游 0.00001；需配合 NEW_USER_AGE_THRESHOLD_SECS>0 生效）。
pub const NEW_USER_OON_WEIGHT_FACTOR: f64 = 0.00001;

/// 新用户特判所需的最少关注数（上游 5）。
pub const NEW_USER_MIN_FOLLOWING: usize = 5;

/// 负分候选映射区间的上界（上游 0.001）。
/// 负分归一化进 [0, NEGATIVE_SCORES_OFFSET)，正分整体抬高该值。
pub const NEGATIVE_SCORES_OFFSET: f64 = 0.001;

// =============================================================================
// 本地环境常量（U1：上游依赖内部超时/配置体系，本地显式给出）
// =============================================================================

pub const THUNDER_REQUEST_TIMEOUT_MS: u64 = 500;
pub const UAS_FETCH_TIMEOUT_MS: u64 = 500;
pub const USER_FEATURES_FETCH_TIMEOUT_MS: u64 = 500;
pub const USER_TOPIC_READ_TIMEOUT_MS: u64 = 500;
pub const STRATO_WRITE_TIMEOUT_MS: u64 = 500;
pub const TES_REQUEST_TIMEOUT_MS: u64 = 500;
pub const GIZMODOUCK_REQUEST_TIMEOUT_MS: u64 = 500;
pub const VF_REQUEST_TIMEOUT_MS: u64 = 500;
pub const PHOENIX_RETRIEVAL_TIMEOUT_MS: u64 = 3_000;
pub const PHOENIX_PREDICTION_TIMEOUT_MS: u64 = 5_000;
pub const TOPIC_RETRIEVAL_TIMEOUT_MS: u64 = 500;
pub const VM_RANKER_TIMEOUT_MS: u64 = 500;

/// 显式话题和新用户冷启动话题的单次候选上限（本地话题适配器参数）。
pub const TOPIC_MAX_RESULTS: usize = 100;

/// 本地 UAS 行为窗口（7 天）。上游 config 的 UAS_WINDOW_TIME_MS = 300_000
/// （5 分钟短窗聚合，配合 MaxSeqLength{Scoring,Retrieval} = 1024）；本地
/// demo UAS 数据按天分布，接真实 UAS 时再对齐上游窗口语义。
pub const UAS_WINDOW_TIME_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// 本地 UAS 序列长度上限。上游为 1024（模型 history 更长）；本地发布
/// checkpoint history_seq_len=127，保留 300 拉取上限。
pub const UAS_MAX_SEQUENCE_LENGTH: usize = 300;

// =============================================================================
// 本地状态适配器常量（U2：无上游对应；生产持久化策略在集成阶段确定）
// =============================================================================

/// 本地 P5 适配器保留的最近已下发帖子数。
pub const LOCAL_SERVED_HISTORY_LIMIT: usize = 500;

/// 本地 P5 适配器保留的最近请求时间戳数。
pub const LOCAL_REQUEST_TIMESTAMP_LIMIT: usize = 50;

/// 本地 P5 适配器最多保留的用户数。
pub const LOCAL_STATE_USER_LIMIT: usize = 10_000;
