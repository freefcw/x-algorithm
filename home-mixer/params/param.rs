// 上游 `47c1bcd` home-mixer/params/param.rs 的本地对应（U1）。
//
// 上游通过 `xai_feature_switches::param!` 定义请求级参数并从 FS 配置读取；
// 本地没有 feature-switch 系统，参数以常量承载“上游默认真值”。
// 命名映射：上游 CamelCase 参数名 -> 本地 SCREAMING_SNAKE_CASE 常量，
// 每项注释保留上游 FS key，便于逐项回对。
//
// 收录规则：只收录本地代码实际消费的参数；未落地能力的参数不提前造常量
// （见 upstream-first-maintenance.md 可选集成规则 6）。上游全表 183 项可用
// `git show 47c1bcd:home-mixer/params/param.rs` 查看。

// =============================================================================
// 召回来源上限（上游真值）
// =============================================================================

/// rust_home_mixer_phoenix_max_results = 1000
pub const PHOENIX_MAX_RESULTS: u32 = 1000;
/// rust_home_mixer_thunder_max_results = 1200
pub const THUNDER_MAX_RESULTS: u32 = 1200;
/// rust_home_mixer_tweet_mixer_max_results = 800
pub const TWEET_MIXER_MAX_RESULTS: u32 = 800;
/// rust_home_mixer_phoenix_moe_max_results = 200
pub const PHOENIX_MOE_MAX_RESULTS: u32 = 200;

// =============================================================================
// 打分权重（上游真值；正向头）
// =============================================================================

/// rust_home_mixer_favorite_weight = 0.5
pub const FAVORITE_WEIGHT: f64 = 0.5;
/// rust_home_mixer_reply_weight = 5.0
pub const REPLY_WEIGHT: f64 = 5.0;
/// rust_home_mixer_retweet_weight = 1.0
pub const RETWEET_WEIGHT: f64 = 1.0;
/// rust_home_mixer_photo_expand_weight = 0.05
pub const PHOTO_EXPAND_WEIGHT: f64 = 0.05;
/// rust_home_mixer_video_open_weight = 0.05
pub const VIDEO_OPEN_WEIGHT: f64 = 0.05;
/// rust_home_mixer_click_weight = 0.4
pub const CLICK_WEIGHT: f64 = 0.4;
/// rust_home_mixer_open_link_weight = 0.2
pub const OPEN_LINK_WEIGHT: f64 = 0.2;
/// rust_home_mixer_profile_click_weight = 0.0
pub const PROFILE_CLICK_WEIGHT: f64 = 0.0;
/// rust_home_mixer_vqv_weight = 0.05
pub const VQV_WEIGHT: f64 = 0.05;
/// rust_home_mixer_share_weight = 2.0
pub const SHARE_WEIGHT: f64 = 2.0;
/// rust_home_mixer_share_via_dm_weight = 5.0
pub const SHARE_VIA_DM_WEIGHT: f64 = 5.0;
/// rust_home_mixer_share_via_copy_link_weight = 20.0
pub const SHARE_VIA_COPY_LINK_WEIGHT: f64 = 20.0;
/// rust_home_mixer_dwell_weight = 0.0
pub const DWELL_WEIGHT: f64 = 0.0;
/// rust_home_mixer_quote_weight = 5.0
pub const QUOTE_WEIGHT: f64 = 5.0;
/// rust_home_mixer_quoted_click_weight = 0.05
pub const QUOTED_CLICK_WEIGHT: f64 = 0.05;
/// rust_home_mixer_quoted_vqv_weight = 0.0
pub const QUOTED_VQV_WEIGHT: f64 = 0.0;
/// rust_home_mixer_follow_author_weight = 4.0
pub const FOLLOW_AUTHOR_WEIGHT: f64 = 4.0;

// =============================================================================
// 打分权重（上游真值；连续动作与探索项）
// =============================================================================

/// rust_home_mixer_cont_dwell_time_weight = 0.004
pub const CONT_DWELL_TIME_WEIGHT: f64 = 0.004;
/// rust_home_mixer_cont_click_dwell_time_weight = 0.0
pub const CONT_CLICK_DWELL_TIME_WEIGHT: f64 = 0.0;
/// rust_home_mixer_cont_active_secs_5m_residual_norm_weight = 0.0
pub const CONT_ACTIVE_SECS_5M_RESIDUAL_NORM_WEIGHT: f64 = 0.0;
/// rust_home_mixer_post_unexplored_weight = 0.02
pub const POST_UNEXPLORED_WEIGHT: f64 = 0.02;
/// rust_home_mixer_enable_multiplicative_post_unexplored = false
pub const ENABLE_MULTIPLICATIVE_POST_UNEXPLORED: bool = false;
/// rust_home_mixer_multiplicative_post_unexplored_alpha = 0.0
pub const MULTIPLICATIVE_POST_UNEXPLORED_ALPHA: f64 = 0.0;
/// rust_home_mixer_post_unexplored_weight_in_network_only = true
pub const POST_UNEXPLORED_WEIGHT_IN_NETWORK_ONLY: bool = true;

// =============================================================================
// 打分权重（上游真值；负向头）
// =============================================================================

/// rust_home_mixer_not_interested_weight = -43.2
pub const NOT_INTERESTED_WEIGHT: f64 = -43.2;
/// rust_home_mixer_block_author_weight = -31.2
pub const BLOCK_AUTHOR_WEIGHT: f64 = -31.2;
/// rust_home_mixer_mute_author_weight = -58.8
pub const MUTE_AUTHOR_WEIGHT: f64 = -58.8;
/// rust_home_mixer_report_weight = -234.0
pub const REPORT_WEIGHT: f64 = -234.0;
/// rust_home_mixer_not_dwelled_weight = -0.02
pub const NOT_DWELLED_WEIGHT: f64 = -0.02;

// =============================================================================
// 双向关注加成（上游真值；候选缺 is_mutual_follow_author 数据时不触发）
// =============================================================================

/// rust_home_mixer_bidirectional_follow_reply_weight_boost = 15.0
pub const BIDIRECTIONAL_FOLLOW_REPLY_WEIGHT_BOOST: f64 = 15.0;
/// rust_home_mixer_bidirectional_follow_dwell_weight_boost = 0.0
pub const BIDIRECTIONAL_FOLLOW_DWELL_WEIGHT_BOOST: f64 = 0.0;

// =============================================================================
// 点击停留低点赞率惩罚（上游真值；默认关闭）
// =============================================================================

/// rust_home_mixer_enable_click_dwell_low_fav_rate_penalty = false
pub const ENABLE_CLICK_DWELL_LOW_FAV_RATE_PENALTY: bool = false;
/// rust_home_mixer_click_dwell_low_fav_rate_penalty_baseline = 0.01
pub const CLICK_DWELL_LOW_FAV_RATE_PENALTY_BASELINE: f64 = 0.01;
/// rust_home_mixer_click_dwell_low_fav_rate_penalty_alpha = 0.5
pub const CLICK_DWELL_LOW_FAV_RATE_PENALTY_ALPHA: f64 = 0.5;
/// rust_home_mixer_click_dwell_low_fav_rate_penalty_floor = 0.01
pub const CLICK_DWELL_LOW_FAV_RATE_PENALTY_FLOOR: f64 = 0.01;
/// rust_home_mixer_click_dwell_low_fav_rate_penalty_cap = 1.0
pub const CLICK_DWELL_LOW_FAV_RATE_PENALTY_CAP: f64 = 1.0;

// =============================================================================
// 多样性与网外调整（上游真值）
// =============================================================================

/// rust_home_mixer_enable_author_diversity = true
pub const ENABLE_AUTHOR_DIVERSITY: bool = true;
/// rust_home_mixer_author_diversity_decay = 0.5
pub const AUTHOR_DIVERSITY_DECAY: f64 = 0.5;
/// rust_home_mixer_author_diversity_floor = 0.25
pub const AUTHOR_DIVERSITY_FLOOR: f64 = 0.25;
/// rust_home_mixer_oon_weight_factor = 0.75
pub const OON_WEIGHT_FACTOR: f64 = 0.75;
/// rust_home_mixer_topic_oon_weight_factor = 0.5
pub const TOPIC_OON_WEIGHT_FACTOR: f64 = 0.5;
/// rust_home_mixer_enable_oon_rescore_for_in_network_replies_retweets = true
pub const ENABLE_OON_RESCORE_FOR_IN_NETWORK_REPLIES_RETWEETS: bool = true;
/// rust_home_mixer_new_user_age_threshold_secs = 0（0 表示不启用新用户特判）
pub const NEW_USER_AGE_THRESHOLD_SECS: u64 = 0;

// =============================================================================
// 视频门槛（上游真值）
// =============================================================================

/// rust_home_mixer_min_video_duration_ms = 10_000
pub const MIN_VIDEO_DURATION_MS: i32 = 10_000;
/// rust_home_mixer_enable_quoted_vqv_duration_check = false
pub const ENABLE_QUOTED_VQV_DURATION_CHECK: bool = false;
