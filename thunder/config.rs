// Thunder 服务配置常量
// 这些值从原始代码的使用上下文中推导而来，可根据实际业务需要调整。

/// 删除事件在 PostStore 中使用的特殊 user_id 键
pub const DELETE_EVENT_KEY: i64 = -1;

/// 每个作者最多保留的原创帖子数（非回复、非转发）
pub const MAX_ORIGINAL_POSTS_PER_AUTHOR: usize = 200;

/// 每个作者最多保留的二级帖子数（回复 + 转发）
pub const MAX_REPLY_POSTS_PER_AUTHOR: usize = 50;

/// 每个用户扫描的最大 TinyPost 数量（性能限制）
pub const MAX_TINY_POSTS_PER_USER_SCAN: usize = 500;

/// 每个作者最多保留的视频帖子数
pub const MAX_VIDEO_POSTS_PER_AUTHOR: usize = 50;

/// 输入列表的最大长度（following_user_ids / exclude_tweet_ids）
pub const MAX_INPUT_LIST_SIZE: usize = 5000;

/// 默认返回的最大帖子数
pub const MAX_POSTS_TO_RETURN: usize = 1000;

/// 视频请求返回的最大帖子数
pub const MAX_VIDEOS_TO_RETURN: usize = 200;

/// 视频有效时长最低阈值（毫秒）
pub const MIN_VIDEO_DURATION_MS: i32 = 6000;
