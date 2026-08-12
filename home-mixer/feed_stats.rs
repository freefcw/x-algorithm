//! Final-feed composition/position stats and the local sink port (U1).
//!
//! Upstream reports these numbers through `xai_stats`. The local
//! `FeedStatsSink` port keeps composition observable; the SideEffect that
//! feeds it lives in `side_effects/for_you_response_stats_side_effect.rs`.

use crate::models::feed_item::{FeedItem, FeedItemKind};
use log::info;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use tokio::sync::Notify;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeedResponseStats {
    pub request_id: String,
    pub total_items: usize,
    pub counts: BTreeMap<FeedItemKind, usize>,
    pub positions: Vec<(FeedItemKind, usize)>,
}

impl FeedResponseStats {
    pub fn from_items(request_id: String, items: &[FeedItem]) -> Self {
        let mut counts = BTreeMap::new();
        let mut positions = Vec::with_capacity(items.len());
        for item in items {
            *counts.entry(item.kind()).or_insert(0) += 1;
            positions.push((item.kind(), item.position));
        }
        Self {
            request_id,
            total_items: items.len(),
            counts,
            positions,
        }
    }

    pub fn count(&self, kind: FeedItemKind) -> usize {
        self.counts.get(&kind).copied().unwrap_or(0)
    }
}

pub trait FeedStatsSink: Send + Sync {
    fn record(&self, stats: FeedResponseStats) -> Result<(), String>;
}

#[derive(Default)]
pub struct InMemoryFeedStats {
    records: Mutex<Vec<FeedResponseStats>>,
    record_count: AtomicUsize,
    recorded: Notify,
}

impl InMemoryFeedStats {
    pub fn records(&self) -> Vec<FeedResponseStats> {
        self.records.lock().expect("feed stats lock").clone()
    }

    pub async fn wait_for_records(&self, expected: usize) {
        loop {
            let notified = self.recorded.notified();
            if self.record_count.load(Ordering::Acquire) >= expected {
                return;
            }
            notified.await;
        }
    }
}

impl FeedStatsSink for InMemoryFeedStats {
    fn record(&self, stats: FeedResponseStats) -> Result<(), String> {
        self.records
            .lock()
            .map_err(|_| "feed stats lock poisoned".to_string())?
            .push(stats);
        self.record_count.fetch_add(1, Ordering::Release);
        self.recorded.notify_waiters();
        Ok(())
    }
}

pub struct LoggingFeedStats;

impl FeedStatsSink for LoggingFeedStats {
    fn record(&self, stats: FeedResponseStats) -> Result<(), String> {
        info!(
            "For You stats - request_id {} total={} posts={} ads={} prompts={} who_to_follow={} push_to_home={}",
            stats.request_id,
            stats.total_items,
            stats.count(FeedItemKind::Post),
            stats.count(FeedItemKind::Advertisement),
            stats.count(FeedItemKind::Prompt),
            stats.count(FeedItemKind::WhoToFollow),
            stats.count(FeedItemKind::PushToHome),
        );
        Ok(())
    }
}
