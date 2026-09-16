//! Transport-boundary policy shared by every RPC entry point: the total
//! request budget and the metrics sink.
//!
//! Pipeline components each carry their own timeout, but nothing bounded the
//! request as a whole, so a chain of slow dependencies could hold a connection
//! open for their sum. This layer applies one budget per request and records
//! how every request ended, independent of which pipeline served it.

use crate::metrics::Metrics;
use log::warn;
use std::cmp;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tonic::metadata::MetadataMap;
use tonic::{Code, Response, Status};

/// Header a gRPC client sends to announce its own deadline.
const GRPC_TIMEOUT_HEADER: &str = "grpc-timeout";

#[derive(Clone)]
pub struct RpcPolicy {
    request_timeout: Duration,
    metrics: Arc<Metrics>,
}

impl Default for RpcPolicy {
    fn default() -> Self {
        Self::new(
            Duration::from_millis(crate::params::REQUEST_TIMEOUT_MS),
            Arc::new(Metrics::new()),
        )
    }
}

impl RpcPolicy {
    pub fn new(request_timeout: Duration, metrics: Arc<Metrics>) -> Self {
        Self {
            request_timeout,
            metrics,
        }
    }

    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    /// Budget for one request: the server default, shortened to the client's
    /// `grpc-timeout` when the client asked for less. A client asking for more
    /// does not extend the server budget.
    pub fn budget(&self, metadata: &MetadataMap) -> Duration {
        match client_timeout(metadata) {
            Some(client) => cmp::min(client, self.request_timeout),
            None => self.request_timeout,
        }
    }

    /// Run one RPC handler and record its terminal status and duration under
    /// `rpc`. A handler cancelled before producing a status is recorded as
    /// `CANCELLED` by the observation's drop.
    pub async fn observe<T>(
        &self,
        rpc: &'static str,
        handler: impl Future<Output = Result<Response<T>, Status>>,
    ) -> Result<Response<T>, Status> {
        let observation = self.metrics.rpc().start(rpc);
        let result = handler.await;
        observation.finish(result.as_ref().map_or_else(Status::code, |_| Code::Ok));
        result
    }
}

/// Bound the pipeline portion of a request.
///
/// On expiry the work future is dropped, which stops every pending stage
/// because the pipeline is fully asynchronous, and the client receives
/// `DeadlineExceeded`. Partial results are never returned: a half-run
/// pipeline has not passed its safety filters.
pub async fn within_budget<T>(
    budget: Duration,
    request_id: &str,
    work: impl Future<Output = Result<T, Status>>,
) -> Result<T, Status> {
    match tokio::time::timeout(budget, work).await {
        Ok(result) => result,
        Err(_) => {
            warn!(
                "request_id={request_id} deadline_exceeded budget_ms={}",
                budget.as_millis()
            );
            Err(Status::deadline_exceeded(format!(
                "request exceeded its {} ms budget",
                budget.as_millis()
            )))
        }
    }
}

/// Parse the client's `grpc-timeout` header.
///
/// Per the gRPC-over-HTTP/2 spec the value is at most eight ASCII digits
/// followed by one unit letter: `H` hours, `M` minutes, `S` seconds,
/// `m` milliseconds, `u` microseconds, `n` nanoseconds. Malformed values are
/// ignored rather than rejected: tonic's own transport layer already applies
/// the header, so an unparseable one simply leaves the server default in
/// force here.
pub fn client_timeout(metadata: &MetadataMap) -> Option<Duration> {
    let raw = metadata.get(GRPC_TIMEOUT_HEADER)?.to_str().ok()?;
    parse_grpc_timeout(raw)
}

fn parse_grpc_timeout(raw: &str) -> Option<Duration> {
    // Byte-wise so a trailing multi-byte character cannot land a split inside
    // a code point; every accepted byte below is ASCII anyway.
    let (unit, digits) = raw.as_bytes().split_last()?;
    if digits.is_empty() || digits.len() > 8 || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let value: u64 = std::str::from_utf8(digits).ok()?.parse().ok()?;
    Some(match unit {
        b'H' => Duration::from_secs(value * 60 * 60),
        b'M' => Duration::from_secs(value * 60),
        b'S' => Duration::from_secs(value),
        b'm' => Duration::from_millis(value),
        b'u' => Duration::from_micros(value),
        b'n' => Duration::from_nanos(value),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    fn metadata_with_timeout(value: &str) -> MetadataMap {
        let mut metadata = MetadataMap::new();
        metadata.insert(
            GRPC_TIMEOUT_HEADER,
            MetadataValue::try_from(value).expect("ascii header"),
        );
        metadata
    }

    #[test]
    fn grpc_timeout_units_follow_the_wire_spec() {
        assert_eq!(parse_grpc_timeout("2H"), Some(Duration::from_secs(7_200)));
        assert_eq!(parse_grpc_timeout("3M"), Some(Duration::from_secs(180)));
        assert_eq!(parse_grpc_timeout("5S"), Some(Duration::from_secs(5)));
        assert_eq!(parse_grpc_timeout("250m"), Some(Duration::from_millis(250)));
        assert_eq!(parse_grpc_timeout("7u"), Some(Duration::from_micros(7)));
        assert_eq!(parse_grpc_timeout("9n"), Some(Duration::from_nanos(9)));
        assert_eq!(
            parse_grpc_timeout("99999999m"),
            Some(Duration::from_millis(99_999_999))
        );
    }

    #[test]
    fn malformed_grpc_timeouts_are_ignored() {
        for raw in [
            "",
            "S",
            "5",
            "5s",
            "5 S",
            "-5S",
            "123456789m",
            "1.5S",
            "٣S",
            "5٣",
        ] {
            assert_eq!(parse_grpc_timeout(raw), None, "{raw:?}");
        }
        assert_eq!(client_timeout(&MetadataMap::new()), None);
    }

    #[test]
    fn budget_is_the_shorter_of_server_and_client_deadlines() {
        let policy = RpcPolicy::new(Duration::from_secs(10), Arc::new(Metrics::new()));
        assert_eq!(policy.budget(&MetadataMap::new()), Duration::from_secs(10));
        assert_eq!(
            policy.budget(&metadata_with_timeout("2S")),
            Duration::from_secs(2)
        );
        assert_eq!(
            policy.budget(&metadata_with_timeout("1M")),
            Duration::from_secs(10),
            "a longer client deadline does not extend the server budget"
        );
        assert_eq!(
            policy.budget(&metadata_with_timeout("garbage")),
            Duration::from_secs(10)
        );
    }

    #[tokio::test]
    async fn work_past_the_budget_becomes_deadline_exceeded() {
        let slow = async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok::<_, Status>("late")
        };
        let error = within_budget(Duration::from_millis(10), "req-1", slow)
            .await
            .expect_err("slow work must be cut");
        assert_eq!(error.code(), Code::DeadlineExceeded);
        assert!(error.message().contains("10 ms"), "{}", error.message());

        let fast = async { Ok::<_, Status>("on time") };
        assert_eq!(
            within_budget(Duration::from_millis(50), "req-2", fast)
                .await
                .unwrap(),
            "on time"
        );
    }

    #[tokio::test]
    async fn observe_records_the_terminal_code_for_success_and_failure() {
        let metrics = Arc::new(Metrics::new());
        let policy = RpcPolicy::new(Duration::from_secs(1), Arc::clone(&metrics));

        policy
            .observe("GetScoredPosts", async { Ok(Response::new(())) })
            .await
            .unwrap();
        policy
            .observe("GetScoredPosts", async {
                Err::<Response<()>, _>(Status::deadline_exceeded("late"))
            })
            .await
            .unwrap_err();

        let text = metrics.encode().unwrap();
        assert!(
            text.contains("home_mixer_rpc_requests_total{code=\"OK\",rpc=\"GetScoredPosts\"} 1")
        );
        assert!(text.contains(
            "home_mixer_rpc_requests_total{code=\"DEADLINE_EXCEEDED\",rpc=\"GetScoredPosts\"} 1"
        ));
        assert!(text.contains("home_mixer_rpc_in_flight{rpc=\"GetScoredPosts\"} 0"));
    }
}
