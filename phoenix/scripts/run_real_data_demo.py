# run_real_data_demo.py
import _setup_path  # noqa: F401

import hashlib

import numpy as np
from recsys_model import RecsysBatch, RecsysEmbeddings, HashConfig, PhoenixModelConfig
from runners import RecsysInferenceRunner, ModelRunner, ACTIONS
from grok import TransformerConfig

# ── 超参（与模型配置一致）──────────────────────────────────────────
TABLE_SIZE      = 100_000
EMB_SIZE        = 128
HISTORY_LEN     = 32
NUM_CANDIDATES  = 8
NUM_HASHES      = 2
NUM_ACTIONS     = 19
SURFACE_VOCAB   = 16

# ── 步骤1：嵌入表（随机初始化，生产环境换成加载训练参数）────────────
rng = np.random.default_rng(0)
user_emb_table   = rng.normal(size=(TABLE_SIZE + 1, EMB_SIZE)).astype(np.float32)
post_emb_table   = rng.normal(size=(TABLE_SIZE + 1, EMB_SIZE)).astype(np.float32)
author_emb_table = rng.normal(size=(TABLE_SIZE + 1, EMB_SIZE)).astype(np.float32)
user_emb_table[0] = post_emb_table[0] = author_emb_table[0] = 0.0

# ── 步骤2：哈希函数 ─────────────────────────────────────────────────
def hash_id(entity_id, num_hashes=NUM_HASHES, table_size=TABLE_SIZE):
    if isinstance(entity_id, str):
        entity_id = int(hashlib.md5(entity_id.encode()).hexdigest(), 16)
    return [hash((entity_id, seed)) % table_size + 1 for seed in range(num_hashes)]

# ── 步骤3：你的真实业务数据 ─────────────────────────────────────────
#    每条历史记录：(帖子ID, 作者ID, [19种行为是否发生], 场景ID)
request = {
    "user_id": 987654321,
    "history": [
        (20240101001, 5001, [1,0,0,0,1,0,0,0,0,0,1,0,0,0,0,0,0,0,30.0], 0),
        (20240101002, 5002, [0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, 5.0], 1),
        (20240101003, 5001, [1,1,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,60.0], 0),
        # ... 最多 HISTORY_LEN 条，不足的自动补 0
    ],
    "candidates": [
        # 待排序的 8 个候选帖子：(帖子ID, 作者ID, 场景ID)
        (20240115001, 5003, 0),
        (20240115002, 5004, 0),
        (20240115003, 5001, 0),
        (20240115004, 5005, 0),
        (20240115005, 5002, 0),
        (20240115006, 5006, 0),
        (20240115007, 5003, 0),
        (20240115008, 5007, 0),
    ],
}

# ── 步骤4：构造 RecsysBatch ─────────────────────────────────────────
B = 1  # 这里只处理一个用户的请求

user_hashes             = np.zeros((B, NUM_HASHES),                         dtype=np.int32)
history_post_hashes     = np.zeros((B, HISTORY_LEN, NUM_HASHES),            dtype=np.int32)
history_author_hashes   = np.zeros((B, HISTORY_LEN, NUM_HASHES),            dtype=np.int32)
history_actions         = np.zeros((B, HISTORY_LEN, NUM_ACTIONS),           dtype=np.float32)
history_product_surface = np.zeros((B, HISTORY_LEN),                        dtype=np.int32)
candidate_post_hashes   = np.zeros((B, NUM_CANDIDATES, NUM_HASHES),         dtype=np.int32)
candidate_author_hashes = np.zeros((B, NUM_CANDIDATES, NUM_HASHES),         dtype=np.int32)
candidate_product_surface = np.zeros((B, NUM_CANDIDATES),                   dtype=np.int32)

# 填用户
user_hashes[0] = hash_id(request["user_id"])

# 填历史（超过 HISTORY_LEN 截断，不足保持全零 = padding）
for t, (post_id, author_id, actions, surface) in enumerate(request["history"]):
    if t >= HISTORY_LEN:
        break
    history_post_hashes[0, t]     = hash_id(post_id)
    history_author_hashes[0, t]   = hash_id(author_id)
    history_actions[0, t]         = actions
    history_product_surface[0, t] = surface

# 填候选
for c, (post_id, author_id, surface) in enumerate(request["candidates"]):
    candidate_post_hashes[0, c]     = hash_id(post_id)
    candidate_author_hashes[0, c]   = hash_id(author_id)
    candidate_product_surface[0, c] = surface

batch = RecsysBatch(
    user_hashes=user_hashes,
    history_post_hashes=history_post_hashes,
    history_author_hashes=history_author_hashes,
    history_actions=history_actions,
    history_product_surface=history_product_surface,
    candidate_post_hashes=candidate_post_hashes,
    candidate_author_hashes=candidate_author_hashes,
    candidate_product_surface=candidate_product_surface,
)

# ── 步骤5：查表，构造 RecsysEmbeddings ──────────────────────────────
embeddings = RecsysEmbeddings(
    user_embeddings             = user_emb_table[user_hashes],
    history_post_embeddings     = post_emb_table[history_post_hashes],
    candidate_post_embeddings   = post_emb_table[candidate_post_hashes],
    history_author_embeddings   = author_emb_table[history_author_hashes],
    candidate_author_embeddings = author_emb_table[candidate_author_hashes],
)

# ── 步骤6：初始化模型并推理 ─────────────────────────────────────────
hash_config = HashConfig(num_user_hashes=2, num_item_hashes=2, num_author_hashes=2)
model_config = PhoenixModelConfig(
    emb_size=EMB_SIZE, num_actions=NUM_ACTIONS,
    history_seq_len=HISTORY_LEN, candidate_seq_len=NUM_CANDIDATES,
    hash_config=hash_config, product_surface_vocab_size=SURFACE_VOCAB,
    model=TransformerConfig(emb_size=EMB_SIZE, widening_factor=2, key_size=64,
                            num_q_heads=2, num_kv_heads=2, num_layers=2,
                            attn_output_multiplier=0.125),
)
runner = RecsysInferenceRunner(
    runner=ModelRunner(model=model_config, bs_per_device=0.125),
    name="demo"
)
runner.initialize()

output = runner.rank(batch, embeddings)

# ── 结果 ────────────────────────────────────────────────────────────
import numpy as np
scores = np.array(output.scores[0])           # [8, 19]
ranked = np.array(output.ranked_indices[0])   # [8]
cand_ids = [c[0] for c in request["candidates"]]

print("\n排序结果（按点赞概率）：")
for rank, idx in enumerate(ranked):
    print(f"  #{rank+1}  帖子 {cand_ids[idx]}  点赞概率={scores[idx,0]:.3f}")