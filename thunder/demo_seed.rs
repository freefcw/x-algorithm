// Demo 种子数据生成器
//
// 无 Kafka 环境下的演示模式数据源：在进程启动时生成一批"最近发布"的
// 模拟帖子灌入 PostStore，让 Thunder 可以独立启动并对外提供查询。
//
// 关键约束：
//   1. 帖子 ID 必须是 Snowflake 格式（高 41 位是毫秒时间戳），
//      否则 home-mixer 的 AgeFilter 会按 ID 解出 1970 年的时间并把帖子全部过滤掉。
//   2. created_at 必须落在 PostStore 保留窗口内（默认 2 天），否则插入时被丢弃。
//   3. 作者集合与 home-mixer 演示模式的关注列表是同一份共享契约
//      （x_algorithm_proto::demo::DEMO_AUTHOR_IDS），否则查询关注流时命中不到任何作者。

use x_algorithm_proto::demo::{snowflake_id, DEMO_AUTHOR_IDS};
use x_algorithm_proto::thunder::LightPost;

/// 生成 `count` 条演示帖子：
/// - 作者在 DEMO_AUTHOR_IDS 中轮转；
/// - 发布时间均匀散布在过去 24 小时内；
/// - 每 5 条中有 1 条是回复（回复到前一条原帖），每 7 条中有 1 条带视频。
pub fn generate_demo_posts(count: usize) -> Vec<LightPost> {
    let now_ms = x_algorithm_proto::demo::now_ms();

    let window_ms: i64 = 24 * 60 * 60 * 1000;
    let step_ms = window_ms / (count.max(1) as i64);

    let mut posts = Vec::with_capacity(count);
    let mut last_original: Option<LightPost> = None;

    for i in 0..count {
        // 第 0 条最旧，最后一条最新，避免全部挤在同一时刻
        let created_at_ms = now_ms - window_ms + step_ms * (i as i64) - 1000;
        let created_at = created_at_ms / 1000;
        let post_id = snowflake_id(created_at_ms, i as i64);
        let author_id = DEMO_AUTHOR_IDS[i % DEMO_AUTHOR_IDS.len()];

        let is_reply = i % 5 == 4 && last_original.is_some();
        let has_video = i % 7 == 6;

        let post = if is_reply {
            let parent = last_original.as_ref().unwrap();
            LightPost {
                post_id,
                author_id,
                created_at,
                in_reply_to_post_id: Some(parent.post_id),
                in_reply_to_user_id: Some(parent.author_id),
                conversation_id: Some(parent.post_id),
                is_retweet: false,
                is_reply: true,
                has_video: false,
                source_post_id: None,
                source_user_id: None,
            }
        } else {
            let post = LightPost {
                post_id,
                author_id,
                created_at,
                in_reply_to_post_id: None,
                in_reply_to_user_id: None,
                conversation_id: Some(post_id),
                is_retweet: false,
                is_reply: false,
                has_video,
                source_post_id: None,
                source_user_id: None,
            };
            last_original = Some(post.clone());
            post
        };

        posts.push(post);
    }

    posts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_demo_posts() {
        let posts = generate_demo_posts(40);
        assert_eq!(posts.len(), 40);

        // ID 唯一
        let mut ids: Vec<i64> = posts.iter().map(|p| p.post_id).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), 40);

        // 全部落在过去 24 小时窗口内
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        for p in &posts {
            assert!(p.created_at < now);
            assert!(now - p.created_at <= 24 * 60 * 60 + 60);
            assert!(DEMO_AUTHOR_IDS.contains(&p.author_id));
        }

        // 回复帖指向真实存在的原帖
        let id_set: std::collections::HashSet<i64> = posts.iter().map(|p| p.post_id).collect();
        for p in posts.iter().filter(|p| p.is_reply) {
            assert!(id_set.contains(&p.in_reply_to_post_id.unwrap()));
        }
    }
}
