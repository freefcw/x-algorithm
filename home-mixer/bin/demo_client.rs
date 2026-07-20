// 演示客户端：调用 home-mixer 的 GetScoredPosts 并打印结果。
//
// 用法：
//   cargo run -p home-mixer --bin demo-client
//   cargo run -p home-mixer --bin demo-client -- --addr http://localhost:50051 --viewer-id 1
//
// 用于本地验证整条链路（thunder → home-mixer → phoenix）是否真的返回了排序结果，
// 不依赖 grpcurl 等外部工具。

use clap::Parser;
use x_algorithm_proto::home_mixer::scored_posts_service_client::ScoredPostsServiceClient;
use x_algorithm_proto::home_mixer::{ScoredPostsQuery, ServedType};

#[derive(Parser, Debug)]
#[command(about = "home-mixer 演示客户端")]
struct Args {
    /// home-mixer gRPC 地址
    #[arg(long, default_value = "http://localhost:50051")]
    addr: String,

    /// 请求的用户 ID
    #[arg(long, default_value = "1")]
    viewer_id: i64,

    /// 只要网内（关注者）帖子
    #[arg(long, default_value_t = false)]
    in_network_only: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let mut client = ScoredPostsServiceClient::connect(args.addr.clone()).await?;
    let request = ScoredPostsQuery {
        viewer_id: args.viewer_id,
        client_app_id: 0,
        country_code: "CN".to_string(),
        language_code: "zh".to_string(),
        seen_ids: vec![],
        served_ids: vec![],
        in_network_only: args.in_network_only,
        is_bottom_request: false,
        bloom_filter_entries: vec![],
    };

    println!(
        "请求 {} 的推荐 Feed（viewer_id={}）...\n",
        args.addr, args.viewer_id
    );
    let response = client.get_scored_posts(request).await?.into_inner();

    if response.scored_posts.is_empty() {
        println!("返回了 0 条帖子。");
        println!("排查提示：");
        println!("  1. thunder 是否用 --demo-seed-posts 启动，端口是否为 50052？");
        println!("  2. home-mixer 是否设置了 HOME_MIXER_DEMO=1？");
        println!("  3. Phoenix gRPC 网关是否在运行（影响网外召回与打分）？");
        std::process::exit(1);
    }

    println!(
        "{:<4} {:<20} {:<8} {:<10} {:<10} 来源",
        "#", "帖子 ID", "作者", "得分", "网内"
    );
    for (i, post) in response.scored_posts.iter().enumerate() {
        let source = match ServedType::try_from(post.served_type) {
            Ok(ServedType::ForYouInNetwork) => "Thunder 网内",
            Ok(ServedType::ForYouPhoenixRetrieval) => "Phoenix 网外",
            _ => "未知",
        };
        println!(
            "{:<4} {:<20} {:<8} {:<10.4} {:<10} {}",
            i + 1,
            post.tweet_id,
            post.author_id,
            post.score,
            if post.in_network { "是" } else { "否" },
            source
        );
    }

    let in_network = response
        .scored_posts
        .iter()
        .filter(|p| p.in_network)
        .count();
    let oon = response.scored_posts.len() - in_network;
    println!(
        "\n共 {} 条：网内 {} 条 + 网外 {} 条。链路打通。",
        response.scored_posts.len(),
        in_network,
        oon
    );

    Ok(())
}
