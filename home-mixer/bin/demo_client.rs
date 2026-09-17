// 演示客户端：调用 home-mixer 的 GetScoredPosts 并打印结果。
//
// 用法：
//   cargo run -p home-mixer --bin demo-client
//   cargo run -p home-mixer --bin demo-client -- --addr http://localhost:50051 --viewer-id 1
//   cargo run -p home-mixer --bin demo-client -- --topic-id 10
//   cargo run -p home-mixer --bin demo-client -- --cached-posts 8
//   cargo run -p home-mixer --bin demo-client -- --final-feed
//   cargo run -p home-mixer --bin demo-client -- --requests 3
//
// 用于本地验证整条链路（thunder → home-mixer → phoenix）是否真的返回了排序结果，
// 不依赖 grpcurl 等外部工具。

use clap::Parser;
use x_algorithm_proto::home_mixer::for_you_feed_service_client::ForYouFeedServiceClient;
use x_algorithm_proto::home_mixer::scored_posts_service_client::ScoredPostsServiceClient;
use x_algorithm_proto::home_mixer::{feed_item, CachedPost, ScoredPostsQuery, ServedType};

#[derive(Parser, Debug)]
#[command(about = "home-mixer 演示客户端")]
struct Args {
    /// home-mixer gRPC 地址
    #[arg(long, default_value = "http://localhost:50051")]
    addr: String,

    /// 请求的用户 ID（24 位小写 hex；纯数字会按零填充 ObjectId 解释）
    #[arg(long, default_value = "1")]
    viewer_id: String,

    /// 只要网内（关注者）帖子
    #[arg(long, default_value_t = false)]
    in_network_only: bool,

    /// 显式话题 ID；可重复传入
    #[arg(long = "topic-id")]
    topic_ids: Vec<i64>,

    /// 合成指定数量的已补全缓存候选，跳过外部召回
    #[arg(long, default_value_t = 0)]
    cached_posts: usize,

    /// 请求 P4 最终 Feed 服务，而不是 P3 帖子打分服务
    #[arg(long, default_value_t = false)]
    final_feed: bool,

    /// 连续请求次数；后续请求把之前返回的帖子 ID 填进 seen_ids，
    /// 演示客户端去重协议（PreviouslySeenPostsFilter）
    #[arg(long, default_value_t = 1)]
    requests: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let viewer_id = parse_demo_viewer_id(&args.viewer_id)?;
    let now_ms = x_algorithm_proto::demo::now_ms();
    let cached_posts: Vec<CachedPost> = (0..args.cached_posts)
        .map(|index| {
            let index_i64 = i64::try_from(index).expect("demo index fits i64");
            let ts_secs = u32::try_from(((now_ms - index_i64 * 1_000) / 1000).max(0)).unwrap_or(0);
            CachedPost {
                tweet_id: format!("{ts_secs:08x}{:016x}", 2_000_000 + index),
                author_id: x_algorithm_proto::demo::padded_object_id_hex(
                    201 + u64::try_from(index % 40).expect("demo author index fits u64"),
                ),
                served_type: ServedType::ForYouCachedPost as i32,
                tweet_text: format!("Cached demo post {index}"),
                language_code: "en".to_string(),
                ..Default::default()
            }
        })
        .collect();
    let mut seen_ids: Vec<String> = Vec::new();
    for round in 1..=args.requests {
        let request = ScoredPostsQuery {
            viewer_id: viewer_id.clone(),
            client_app_id: 0,
            country_code: "CN".to_string(),
            language_code: "zh".to_string(),
            seen_ids: seen_ids.clone(),
            served_ids: vec![],
            in_network_only: args.in_network_only,
            is_bottom_request: false,
            bloom_filter_entries: vec![],
            topic_ids: args.topic_ids.clone(),
            cached_posts: cached_posts.clone(),
            ..Default::default()
        };

        println!(
            "请求 {} 的推荐 Feed（viewer_id={}，第 {round}/{} 轮，seen_ids={} 条）...\n",
            args.addr,
            args.viewer_id,
            args.requests,
            seen_ids.len()
        );
        let scored_posts = if args.final_feed {
            let mut client = ForYouFeedServiceClient::connect(args.addr.clone()).await?;
            let response = client.get_for_you_feed(request).await?.into_inner();
            let mut posts = Vec::new();
            for item in response.items {
                match item.item {
                    Some(feed_item::Item::Post(post)) => posts.push(post),
                    Some(feed_item::Item::Advertisement(ad)) => {
                        println!("[FeedItem] 广告 {} @ {}", ad.ad_id, item.position)
                    }
                    Some(feed_item::Item::WhoToFollow(module)) => println!(
                        "[FeedItem] 关注推荐 {}（{} 人） @ {}",
                        module.module_id,
                        module.user_ids.len(),
                        item.position
                    ),
                    Some(feed_item::Item::Prompt(prompt)) => {
                        println!("[FeedItem] Prompt {} @ {}", prompt.prompt_id, item.position)
                    }
                    Some(feed_item::Item::PushToHome(push)) => println!(
                        "[FeedItem] Push-to-Home {} @ {}",
                        push.notification_id, item.position
                    ),
                    None => {}
                }
            }
            posts
        } else {
            let mut client = ScoredPostsServiceClient::connect(args.addr.clone()).await?;
            client
                .get_scored_posts(request)
                .await?
                .into_inner()
                .scored_posts
        };

        if scored_posts.is_empty() && round == 1 {
            println!("返回了 0 条帖子。");
            println!("排查提示：");
            println!("  1. thunder 是否用 --demo-seed-posts 启动，端口是否为 50052？");
            println!("  2. home-mixer 是否设置了 HOME_MIXER_MODE=demo？");
            println!("  3. Phoenix gRPC 网关是否在运行（影响网外召回与打分）？");
            std::process::exit(1);
        }
        if scored_posts.is_empty() {
            println!(
            "第 {round} 轮返回 0 条：之前返回的 {} 条都在 seen_ids 里，被客户端去重协议全部滤除。",
            seen_ids.len()
        );
            break;
        }

        println!(
            "{:<4} {:<24} {:<24} {:<10} {:<10} 来源",
            "#", "帖子 ID", "作者", "得分", "网内"
        );
        for (i, post) in scored_posts.iter().enumerate() {
            let source = match ServedType::try_from(post.served_type) {
                Ok(ServedType::ForYouInNetwork) => "Thunder 网内",
                Ok(ServedType::RankedFollowing) => "Thunder 关注流",
                Ok(ServedType::ForYouPhoenixRetrieval) => "Phoenix 网外",
                Ok(ServedType::ForYouPhoenixRetrievalMoe) => "Phoenix MoE",
                Ok(ServedType::ForYouPhoenixTopics) => "Phoenix 话题",
                Ok(ServedType::ForYouTweetMixer) => "Tweet Mixer",
                Ok(ServedType::ForYouCachedPost) => "请求缓存",
                _ => "未知",
            };
            println!(
                "{:<4} {:<24} {:<24} {:<10.4} {:<10} {}",
                i + 1,
                post.tweet_id,
                post.author_id,
                post.score,
                if post.in_network { "是" } else { "否" },
                source
            );
        }

        let in_network = scored_posts.iter().filter(|p| p.in_network).count();
        let oon = scored_posts.len() - in_network;
        println!(
            "\n共 {} 条：网内 {} 条 + 网外 {} 条。{}链路{}。",
            scored_posts.len(),
            in_network,
            oon,
            if args.final_feed { "最终 Feed " } else { "" },
            if round == 1 {
                "打通"
            } else {
                "去重后仍有新鲜结果"
            }
        );

        // 本轮返回的帖子进入下一轮的 seen_ids，演示客户端去重协议。
        seen_ids.extend(scored_posts.iter().map(|post| post.tweet_id.clone()));
    }

    Ok(())
}

fn parse_demo_viewer_id(raw: &str) -> anyhow::Result<String> {
    if raw.chars().all(|c| c.is_ascii_digit()) && raw.len() != 24 {
        let n: u64 = raw.parse()?;
        return Ok(x_algorithm_proto::demo::padded_object_id_hex(n));
    }
    if raw.len() == 24
        && raw.bytes().all(|b| b.is_ascii_hexdigit())
        && !raw.bytes().any(|b| b.is_ascii_uppercase())
    {
        return Ok(raw.to_string());
    }
    anyhow::bail!("viewer-id must be 24 lowercase hex chars or a decimal integer");
}
