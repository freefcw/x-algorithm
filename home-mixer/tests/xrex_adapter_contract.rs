//! Wire contract: Home Mixer Rust client -> Python adapter -> fake xrex ranking RPC.

use home_mixer::clients::phoenix_prediction_client::{
    PhoenixPredictionClient, ProdPhoenixPredictionClient,
};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;
use x_algorithm_proto::recsys::{TweetInfo, UserActionSequence};

struct ServerProcess(Child);

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start_adapter() -> (ServerProcess, u16) {
    let phoenix = Path::new(env!("CARGO_MANIFEST_DIR")).join("../phoenix");
    let python = std::env::var_os("PHOENIX_CONTRACT_PYTHON")
        .map(PathBuf::from)
        .unwrap_or_else(|| phoenix.join(".venv/bin/python"));
    let mut child = Command::new(python)
        .arg(phoenix.join("tests/support/xrex_ranking_wire_server.py"))
        .current_dir(&phoenix)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("start Python adapter contract server");
    let stdout = child.stdout.take().expect("Python stdout is piped");
    let process = ServerProcess(child);
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            match line {
                Ok(line) if line.starts_with("ADAPTER_PORT=") => {
                    let result = line["ADAPTER_PORT=".len()..]
                        .parse::<u16>()
                        .map_err(|error| error.to_string());
                    let _ = sender.send(result);
                    return;
                }
                Ok(_) => {}
                Err(error) => {
                    let _ = sender.send(Err(error.to_string()));
                    return;
                }
            }
        }
        let _ = sender.send(Err("Python adapter exited before reporting its port".into()));
    });
    let port = receiver
        .recv_timeout(Duration::from_secs(20))
        .expect("Python adapter startup timed out")
        .expect("Python adapter did not report a valid port");
    (process, port)
}

#[tokio::test]
#[ignore = "requires Python gRPC adapter dependencies"]
async fn ranking_response_preserves_required_log_prob_heads_over_grpc() {
    let (_server, port) = start_adapter();
    std::env::set_var(
        "PHOENIX_PREDICT_GRPC_ADDR",
        format!("http://127.0.0.1:{port}"),
    );
    std::env::set_var(
        "PHOENIX_EXPECTED_MODEL_VERSION",
        "test-checkpoint@0123456789ab",
    );
    std::env::set_var("PHOENIX_EXPECTED_IDENTITY_MAP_SHA256", "a".repeat(64));

    let client = ProdPhoenixPredictionClient::new()
        .await
        .expect("construct Home Mixer prediction client");
    let response = tokio::time::timeout(
        Duration::from_secs(5),
        client.predict(
            11,
            UserActionSequence {
                user_id: 11,
                ..Default::default()
            },
            vec![TweetInfo {
                tweet_id: 22,
                author_id: 33,
                ..Default::default()
            }],
        ),
    )
    .await
    .expect("ranking RPC deadline")
    .expect("Home Mixer accepts the adapted ranking response");

    let distributions = &response.distribution_sets[0].candidate_distributions;
    assert_eq!(distributions.len(), 1);
    assert_eq!(distributions[0].candidate.as_ref().unwrap().tweet_id, 22);
    let log_probs = &distributions[0].top_log_probs;
    assert_eq!(log_probs.len(), 19);
    for (index, expected) in [(1, -0.1_f32), (2, -0.2), (18, -0.3)] {
        assert!(
            (log_probs[index] - expected).abs() < 1e-6,
            "action {index} was not translated: got {}",
            log_probs[index]
        );
    }
    assert_eq!(distributions[0].continuous_actions_values, [0.0, 2.5]);

    let rejected = client
        .predict(
            11,
            UserActionSequence {
                user_id: 11,
                ..Default::default()
            },
            vec![
                TweetInfo {
                    tweet_id: 22,
                    author_id: 33,
                    ..Default::default()
                },
                TweetInfo {
                    tweet_id: 23,
                    author_id: 33,
                    ..Default::default()
                },
            ],
        )
        .await
        .expect_err("missing required action head must fail the whole batch");
    assert!(rejected.to_string().contains("missing required action"));
}
