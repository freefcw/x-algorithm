// 帖子 embedding 存储，DPP 相似度核使用。
//
// 上游通过 `xai_recsys_mm_server::mm_embedding_client`（O2 对象存储预载的
// 多模态 embedding 内存表）提供查询；该客户端依赖内部 O2/部署环境，本地以
// `MmEmbeddingsClient` trait 保留同名查询契约（U1）：`get(id)` 返回
// f16 embedding。默认内存实现为空表——DPP 对缺失 embedding 使用随机单位
// 向量降级（上游同语义），真实多模态 embedding 源接入后替换注入实现。

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use half::f16;

/// 上游 mm_embedding_client 的查询面。
pub trait MmEmbeddingsLookup: Send + Sync {
    fn get(&self, id: u64) -> Option<Arc<Vec<f16>>>;
}

/// 进程内 embedding 表；空表即"全部缺失"，DPP 走随机向量降级路径。
#[derive(Default)]
pub struct InMemoryMmEmbeddings {
    entries: HashMap<u64, Arc<Vec<f16>>>,
}

impl InMemoryMmEmbeddings {
    pub fn new(entries: HashMap<u64, Arc<Vec<f16>>>) -> Self {
        Self { entries }
    }
}

impl MmEmbeddingsLookup for InMemoryMmEmbeddings {
    fn get(&self, id: u64) -> Option<Arc<Vec<f16>>> {
        self.entries.get(&id).cloned()
    }
}

pub struct MmEmbeddingsClient {
    lookup: Arc<dyn MmEmbeddingsLookup>,
}

impl MmEmbeddingsClient {
    pub fn get(&self, id: u64) -> Option<Arc<Vec<f16>>> {
        self.lookup.get(id)
    }
}

pub struct EmbeddingStore {
    pub(crate) client: MmEmbeddingsClient,
    dim: usize,
}

impl EmbeddingStore {
    pub fn dim(&self) -> usize {
        self.dim
    }
}

/// 上游返回 (store, O2 preload future)；本地内存实现无预载动作，
/// 返回立即完成的 future 以保留调用形状。
pub type O2PreloadFuture = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

pub fn init_store(dim: usize) -> Result<(Arc<EmbeddingStore>, O2PreloadFuture)> {
    init_store_with(dim, Arc::new(InMemoryMmEmbeddings::default()))
}

pub fn init_store_with(
    dim: usize,
    lookup: Arc<dyn MmEmbeddingsLookup>,
) -> Result<(Arc<EmbeddingStore>, O2PreloadFuture)> {
    let store = Arc::new(EmbeddingStore {
        client: MmEmbeddingsClient { lookup },
        dim,
    });
    Ok((store, Box::pin(async {})))
}
