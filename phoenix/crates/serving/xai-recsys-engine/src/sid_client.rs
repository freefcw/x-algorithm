// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 X.AI Corp.
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use lazy_static::lazy_static;
use log::{info, warn};
use prometheus::{
    CounterVec, HistogramVec, exponential_buckets, register_counter_vec, register_histogram_vec,
};
use pyo3::prelude::*;
use tonic::transport::Endpoint;
use xai_recsys_sid_proto::{LookupSidsRequest, sid_lookup_service_client::SidLookupServiceClient};

const REQUEST_TIMEOUT: Duration = Duration::from_millis(100);
const NUM_CHANNELS: usize = 2;

lazy_static! {
    static ref SID_LOOKUP_LATENCY_SECS: HistogramVec = register_histogram_vec!(
        "recsys_engine_sid_lookup_latency_seconds",
        "Latency of semantic ID lookup gRPC calls",
        &["status"],
        exponential_buckets(0.001, 2.0, 15).unwrap(),
    )
    .unwrap();
    static ref SID_LOOKUP_COUNT: CounterVec = register_counter_vec!(
        "recsys_engine_sid_lookup_count",
        "Number of semantic ID lookups by status",
        &["status"],
    )
    .unwrap();
}

fn build_endpoint(endpoint: &str) -> Endpoint {
    let host_port = endpoint.strip_prefix("http://").unwrap_or(endpoint);
    let url = format!("http://{host_port}");
    Endpoint::from_shared(url)
        .expect("invalid SID endpoint URL")
        .timeout(REQUEST_TIMEOUT)
        .tcp_nodelay(true)
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_timeout(Duration::from_secs(20))
        .keep_alive_while_idle(true)
}

pub struct SemanticIdClient {
    clients: Vec<SidLookupServiceClient<tonic::transport::Channel>>,
    next: AtomicUsize,
    sid_num_levels: usize,
}

impl SemanticIdClient {
    pub fn new(endpoint: &str, sid_num_levels: usize) -> Self {
        let clients = (0..NUM_CHANNELS)
            .map(|_| {
                let channel = build_endpoint(endpoint).connect_lazy();
                SidLookupServiceClient::new(channel)
            })
            .collect();
        Self {
            clients,
            next: AtomicUsize::new(0),
            sid_num_levels,
        }
    }

    pub async fn lookup(&self, post_ids: &[i64]) -> HashMap<i64, Vec<i32>> {
        if post_ids.is_empty() {
            return HashMap::new();
        }

        let start = Instant::now();
        let request = LookupSidsRequest {
            post_ids: post_ids.to_vec(),
        };

        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        let mut client = self.clients[idx].clone();
        match client.lookup_sids(request).await {
            Ok(response) => {
                let elapsed = start.elapsed().as_secs_f64();
                SID_LOOKUP_LATENCY_SECS
                    .with_label_values(&["success"])
                    .observe(elapsed);

                let resp = response.into_inner();
                let mut result = HashMap::with_capacity(post_ids.len());
                let missing = vec![-1i32; self.sid_num_levels];
                for (post_id, post_sids) in post_ids.iter().zip(resp.results.iter()) {
                    if post_sids.codes.is_empty() {
                        SID_LOOKUP_COUNT.with_label_values(&["miss"]).inc();
                        result.insert(*post_id, missing.clone());
                    } else {
                        SID_LOOKUP_COUNT.with_label_values(&["hit"]).inc();
                        result.insert(*post_id, post_sids.codes.clone());
                    }
                }
                result
            }
            Err(e) => {
                let elapsed = start.elapsed().as_secs_f64();
                SID_LOOKUP_LATENCY_SECS
                    .with_label_values(&["error"])
                    .observe(elapsed);
                SID_LOOKUP_COUNT
                    .with_label_values(&["error"])
                    .inc_by(post_ids.len() as f64);
                warn!("SID lookup gRPC call failed: {}", e);
                let missing = vec![-1i32; self.sid_num_levels];
                post_ids.iter().map(|id| (*id, missing.clone())).collect()
            }
        }
    }
}

#[pyclass(module = "xai_recsys_engine")]
pub struct PySemanticIdClient {
    endpoint: String,
    sid_num_levels: usize,
}

impl PySemanticIdClient {
    pub fn build(&self) -> Arc<SemanticIdClient> {
        Arc::new(SemanticIdClient::new(&self.endpoint, self.sid_num_levels))
    }
}

#[pymethods]
impl PySemanticIdClient {
    #[new]
    #[pyo3(signature = (endpoint, sid_num_levels))]
    fn new(endpoint: String, sid_num_levels: usize) -> PyResult<Self> {
        info!(
            "Semantic ID client configured: endpoint={}, sid_num_levels={}",
            endpoint, sid_num_levels
        );
        Ok(Self {
            endpoint,
            sid_num_levels,
        })
    }
}
