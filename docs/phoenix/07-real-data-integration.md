# Phoenix 真实数据接入实操指引

## 1. 这篇文档解决什么问题

`docs/training_data_spec.md` 解决的是"训练数据字段怎么定义"，`06-training-and-data.md` 解决的是"训练侧全局结构分析"。  
本文档解决的是更靠近代码的实操问题：

- 有了原始业务数据，如何一步步构造出 `RecsysBatch` 和 `RecsysEmbeddings`
- 哈希嵌入系统的工作原理和生产落地细节
- 模型参数的保存与加载（情况 A / 情况 B）
- 训练流程的阶段路径

---

## 2. 输入结构速查

模型接收两个对象，职责严格分开：

| 对象 | 存什么 | 类比 |
|---|---|---|
| `RecsysBatch` | 哈希值、行为标记、场景 ID（整数） | 数据库的"主键" |
| `RecsysEmbeddings` | 已查表得到的浮点向量 | 数据库的"内容" |

**为什么要拆开？**  
嵌入表在生产环境中通常是独立的参数服务（数 GB 到数百 GB），与模型推理分开部署。Phoenix 把"查表"从模型内部剥离，让调用方在调用模型前完成查表，嵌入表可以独立热更新而不影响模型推理。

---

## 3. 哈希嵌入系统详解

### 3.1 核心思路

推荐系统的 ID 数量极大（数亿用户、数十亿帖子），不可能为每个 ID 单独维护一个嵌入向量。解法是**多哈希映射**：

```
原始 ID（如 user_id = 987654321）
       │
       ├─ 哈希函数 1：hash(987654321, seed=0) % TABLE_SIZE  → 索引 42371
       └─ 哈希函数 2：hash(987654321, seed=1) % TABLE_SIZE  → 索引 88102
                                                                    │
                            ┌───────────────────────────────────────┤
                            │                                       │
                     嵌入表[42371]                           嵌入表[88102]
                     → 向量 v1 ∈ ℝ^D                         → 向量 v2 ∈ ℝ^D
                            │                                       │
                            └──────────────┬────────────────────────┘
                                           │
                                    线性投影层（proj_mat）
                                           │
                                    用户表示 ∈ ℝ^D
```

两个哈希函数同时碰撞的概率约为 `1/TABLE_SIZE²`，实践中可以忽略。

### 3.2 关键参数

| 参数 | 本项目默认值 | 含义 |
|---|---|---|
| `TABLE_SIZE` | 100,000 | 嵌入表的行数（哈希取模范围）|
| `num_*_hashes` | 2 | 对同一实体用几个哈希函数 |
| `emb_size` (D) | 128 | 嵌入向量维度 |

### 3.3 哈希函数实现

```python
import hashlib

def id_to_hashes(entity_id, num_hashes: int = 2, table_size: int = 100_000) -> list[int]:
    """
    将任意整数或字符串 ID 映射为 num_hashes 个嵌入表索引。
    结果值域 [1, table_size]，0 保留给 padding。
    """
    if isinstance(entity_id, str):
        # 字符串 ID：先用 MD5 转为整数
        entity_id = int(hashlib.md5(entity_id.encode()).hexdigest(), 16)
    return [hash((entity_id, seed)) % table_size + 1 for seed in range(num_hashes)]
```

> 与 `training_data_spec.md §4.1` 中的 MD5 版本等价，两种写法均可，保持训练与推理一致即可。

### 3.4 嵌入表的两种状态

**情况 A（验证流程，当前项目现状）**：随机初始化，输出分数无业务意义，但整个推理路径完整正确。

```python
rng = np.random.default_rng(0)
user_emb_table   = rng.normal(size=(TABLE_SIZE + 1, EMB_SIZE)).astype(np.float32)
post_emb_table   = rng.normal(size=(TABLE_SIZE + 1, EMB_SIZE)).astype(np.float32)
author_emb_table = rng.normal(size=(TABLE_SIZE + 1, EMB_SIZE)).astype(np.float32)
# 第 0 行必须全零（padding 位）
user_emb_table[0] = post_emb_table[0] = author_emb_table[0] = 0.0
```

**情况 B（生产推理）**：从训练产物文件加载。见第 5 节。

---

## 4. 构造 RecsysBatch 和 RecsysEmbeddings

### 4.1 字段一览

```
RecsysBatch
├── user_hashes                [B, 2]        int32   ← 用户 ID 的两路哈希值
├── history_post_hashes        [B, 32, 2]    int32   ← 历史帖子 ID 哈希，不足末尾补 0
├── history_author_hashes      [B, 32, 2]    int32   ← 历史作者 ID 哈希，同上
├── history_actions            [B, 32, 19]   float32 ← 历史互动行为向量（19 种）
├── history_product_surface    [B, 32]       int32   ← 历史场景 ID，范围 [0, 15]
├── candidate_post_hashes      [B, 8, 2]     int32   ← 候选帖子 ID 哈希
├── candidate_author_hashes    [B, 8, 2]     int32   ← 候选作者 ID 哈希
└── candidate_product_surface  [B, 8]        int32   ← 候选场景 ID

RecsysEmbeddings（查表结果）
├── user_embeddings             [B, 2, 128]   float32
├── history_post_embeddings     [B, 32, 2, 128] float32
├── candidate_post_embeddings   [B, 8, 2, 128]  float32
├── history_author_embeddings   [B, 32, 2, 128] float32
└── candidate_author_embeddings [B, 8, 2, 128]  float32
```

### 4.2 19 种行为向量对照表

| 索引 | 字段名 | 含义 | 取值 |
|---|---|---|---|
| 0 | `favorite` | 点赞 | 0/1 |
| 1 | `reply` | 回复 | 0/1 |
| 2 | `repost` | 转发 | 0/1 |
| 3 | `photo_expand` | 图片展开 | 0/1 |
| 4 | `click` | 点击详情 | 0/1 |
| 5 | `profile_click` | 点击作者主页 | 0/1 |
| 6 | `vqv` | 视频播放质量 | 0~1 连续值 |
| 7 | `share` | 分享（任意） | 0/1 |
| 8 | `share_via_dm` | 私信分享 | 0/1 |
| 9 | `share_via_copy_link` | 复制链接分享 | 0/1 |
| 10 | `dwell` | 停留超过阈值 | 0/1 |
| 11 | `quote` | 引用转发 | 0/1 |
| 12 | `quoted_click` | 点击引用内容 | 0/1 |
| 13 | `follow_author` | 关注作者 | 0/1 |
| 14 | `not_interested` | 不感兴趣 | 0/1 |
| 15 | `block_author` | 屏蔽作者 | 0/1 |
| 16 | `mute_author` | 静音作者 | 0/1 |
| 17 | `report` | 举报 | 0/1 |
| 18 | `dwell_time` | 归一化停留时长 | 连续值，建议 `log1p(s)/log1p(300)` |

### 4.3 完整构造代码

参见仓库中的 `phoenix/run_real_data_demo.py`，该文件是本文档的可运行配套示例。

核心流程：

```python
# 1. 准备嵌入表（情况 A 随机，情况 B 从文件加载）
user_emb_table   = ...   # [TABLE_SIZE+1, EMB_SIZE]
post_emb_table   = ...
author_emb_table = ...

# 2. 把业务 ID 转为哈希值
user_hashes[0] = id_to_hashes(user_id)
history_post_hashes[0, t] = id_to_hashes(post_id)
# ...（其余字段同理）

# 3. 用哈希值做 numpy fancy indexing 查表（一行代码）
user_embeddings = user_emb_table[user_hashes]               # [B, 2, 128]
history_post_embeddings = post_emb_table[history_post_hashes]  # [B, 32, 2, 128]
# ...
```

### 4.4 关键约束

| 约束 | 原因 |
|---|---|
| 哈希值不能为 0 | 0 是 padding 标记，模型用 `hash[:,:,0] != 0` 判断有效位 |
| 历史不足 32 条时末尾补 0 | 所有字段（哈希、actions、surface）对应位置均填 0 |
| 候选必须恰好 8 个 | `candidate_seq_len` 固定，不足时用虚拟候选补位 |
| 嵌入表第 0 行必须全零 | 确保 padding 位查表后得到零向量 |

---

## 5. 模型参数的保存与加载（情况 B）

Phoenix 的参数分为两部分，**必须分开保存**：

```
训练完后的产物
├── model_params.npz       ← Transformer 权重 + 投影矩阵（由 runner.params 导出）
└── embedding_tables.npz   ← 三张嵌入表（user / post / author）
```

### 5.1 保存

```python
import jax
import numpy as np

# 保存嵌入表
np.savez("checkpoints/embedding_tables.npz",
    user_emb_table=user_emb_table,
    post_emb_table=post_emb_table,
    author_emb_table=author_emb_table,
)

# 保存模型参数（展平嵌套 dict）
def flatten_dict(d, parent_key="", sep="/"):
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_dict(v, new_key, sep))
        else:
            items[new_key] = np.array(v)
    return items

flat = flatten_dict(jax.tree_util.tree_map(lambda x: np.array(x), runner.params))
np.savez("checkpoints/model_params.npz", **flat)
```

### 5.2 加载

```python
# 加载嵌入表
tables = np.load("checkpoints/embedding_tables.npz")
user_emb_table   = tables["user_emb_table"]
post_emb_table   = tables["post_emb_table"]
author_emb_table = tables["author_emb_table"]

# 加载模型参数（还原嵌套 dict）
def unflatten_dict(flat, sep="/"):
    result = {}
    for key, val in flat.items():
        parts = key.split(sep)
        d = result
        for part in parts[:-1]:
            d = d.setdefault(part, {})
        d[parts[-1]] = val
    return result

raw = np.load("checkpoints/model_params.npz", allow_pickle=False)
runner.initialize()                                      # 先建立参数结构
runner.params = unflatten_dict({k: raw[k] for k in raw.files})  # 再覆盖
```

---

## 6. 训练流程的阶段路径

当前项目只有推理，没有训练脚本。完整训练闭环需要分阶段建设：

```
阶段 1（已完成）
  用随机参数跑通推理流程
  → run_ranker.py / run_retrieval.py 可正常运行

阶段 2（下一步）
  用模拟数据 + 随机 labels 跑通训练循环（不需要真实数据）
  → 验证损失函数、优化器、梯度流动正常
  → 需要安装 optax：uv add optax

阶段 3
  接入真实行为日志数据
  → 参考 docs/training_data_spec.md 的字段规范
  → 参考本文档第 4 节构造 RecsysBatch / RecsysEmbeddings

阶段 4
  保存训练产物，接回推理流程（情况 B）
  → 参考本文档第 5 节的保存/加载代码
```

### 6.1 训练数据来自哪里

详细字段规范见 `docs/training_data_spec.md`。简要说：

**需要采集 3 张表：**

| 表 | 核心字段 | 用途 |
|---|---|---|
| 曝光事件表 | `user_id`, `impression_time`, `candidate_post_ids[]`, `product_surface` | 定义每条训练样本的输入 |
| 互动行为日志表 | `user_id`, `post_id`, `action_type`, `action_time`, `dwell_seconds` | 构造 19 维标签向量 |
| 用户历史序列表 | `user_id`, `post_id`, `author_id`, `action_vector[19]`, `action_time` | 构造历史上下文 |

**一条训练样本的构造逻辑：**

```
选一条曝光事件
  ↓
以曝光时间为界，取该用户此前最近 32 条历史记录 → history_*
  ↓
曝光的候选帖子（正样本）+ 随机采样的未曝光帖子（负样本）凑满 8 个 → candidate_*
  ↓
查行为日志，得到 8 个候选各自的 19 维行为标记 → labels [8, 19]
```

**正负样本比例**：1 个正样本 + 7 个随机负样本，正好对应 `candidate_seq_len=8`。

### 6.2 训练代码骨架

```python
# 需要先：uv add optax
import optax
import jax
import haiku as hk

def loss_fn(batch, embeddings, labels):
    # labels: [B, C, 19]，每个候选每种行为是否真实发生
    from recsys_model import PhoenixModelConfig
    model = PhoenixModelConfig(...).make()
    output = model(batch, embeddings)              # logits: [B, C, 19]
    loss = optax.sigmoid_binary_cross_entropy(output.logits, labels)
    return jax.numpy.mean(loss)

loss_transform = hk.transform(loss_fn)
optimizer = optax.adam(learning_rate=1e-4)

@jax.jit
def train_step(params, opt_state, batch, embeddings, labels):
    loss, grads = jax.value_and_grad(
        lambda p: loss_transform.apply(p, None, batch, embeddings, labels)
    )(params)
    updates, opt_state_new = optimizer.update(grads, opt_state)
    params_new = optax.apply_updates(params, updates)
    return params_new, opt_state_new, loss
```

> 完整训练脚本尚未在仓库中实现，上述为骨架示意。详细讨论见 `06-training-and-data.md §10`。

---

## 7. 调试常见问题

| 现象 | 原因 | 解法 |
|---|---|---|
| 输出全相同分数 | 嵌入表第 0 行没有置零，padding 位被赋了随机向量 | `emb_table[0] = 0.0` |
| 历史被完全忽略 | `history_post_hashes` 全为 0 | 检查哈希函数是否返回了 0 |
| 形状不匹配报错 | 忘记多哈希维度，写成 `[B, S, D]` 而非 `[B, S, 2, D]` | 查表结果是 `[..., num_hashes, D]` |
| 候选评分异常 | 候选数不足 8 个 | 不足时补虚拟候选（哈希值为任意非 0 整数，label 全 0）|

**调试代码片段**：

```python
# 检查有效历史条数
valid = (batch.history_post_hashes[:, :, 0] != 0).sum(axis=1)
print("各用户有效历史条数：", valid)

# 检查嵌入形状
assert embeddings.user_embeddings.shape == (B, 2, 128)
assert embeddings.history_post_embeddings.shape == (B, 32, 2, 128)
assert embeddings.candidate_post_embeddings.shape == (B, 8, 2, 128)

# 确认哈希值不为 0
assert (batch.user_hashes > 0).all()
assert (batch.candidate_post_hashes > 0).all()
```

---

## 8. 相关文档索引

| 文档 | 位置 | 解决的问题 |
|---|---|---|
| 训练数据字段规范 | `docs/training_data_spec.md` | 原始日志字段定义、SQL 示例、Parquet 格式 |
| 训练侧全局分析 | `docs/phoenix/06-training-and-data.md` | 训练与推理一致性、损失函数推断、训练缺口清单 |
| 本文档 | `docs/phoenix/07-real-data-integration.md` | 哈希嵌入实操、构造代码、保存/加载、阶段路径 |
| 可运行示例 | `phoenix/run_real_data_demo.py` | 端到端代码，直接 `uv run run_real_data_demo.py` |
| 双塔模型输入输出 | `phoenix/双塔模型输入输出指引文档.md` | 召回模型专项说明 |
