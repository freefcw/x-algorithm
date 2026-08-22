# Phoenix 真实数据接入实操指引

> 同主题的**完整操作手册**是 [phoenix/docs/真实数据接入指引.md](../../phoenix/docs/真实数据接入指引.md)（字段规范更全、与代码同步更勤），动手时以那份为准；本文侧重从代码结构角度解释"为什么这样接"。

## 1. 这篇文档解决什么问题

[../training/training_data_spec.md](../training/training_data_spec.md) 解决的是"训练数据字段怎么定义"，`06-training-and-data.md` 解决的是"训练侧全局结构分析"。  
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
from data_preprocessor import hash_id_to_ints

# 与训练预处理器同一套：MD5("{id}_hash{i}") % table_size + 1
# 不要用 Python 内置 hash()——结果依赖 PYTHONHASHSEED，也和训练表对不齐
user_hashes = hash_id_to_ints("10001")
```

> 训练和推理必须调用 `data_preprocessor.hash_id_to_ints`（或 [../training/training_data_spec.md](../training/training_data_spec.md) §4.1 的等价 MD5）。`run_real_data_demo.py` 里的 `hash((id, seed))` 只用于演示，不能拿去造训练样本。

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
| 18 | `dwell_time` | 归一化停留时长 | 连续值，`data_preprocessor.py` 用 `seconds / 300` 压到 [0, 1] |

### 4.3 完整构造代码

参见仓库中的 `phoenix/scripts/run_real_data_demo.py`，该文件是本文档的可运行配套示例。

核心流程：

```python
# 1. 准备嵌入表（情况 A 随机，情况 B 从文件加载）
user_emb_table   = ...   # [TABLE_SIZE+1, EMB_SIZE]
post_emb_table   = ...
author_emb_table = ...

# 2. 把业务 ID 转为哈希值
user_hashes[0] = hash_id(user_id)
history_post_hashes[0, t] = hash_id(post_id)
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
| 演示训练张量候选位是 8 | `train_ranker.py` 的 `candidate_seq_len=8`，不足补零；在线 gRPC 网关按 32 分块，不要按 8 去裁 Predict 请求 |
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

> 更新说明：下述阶段 1~4 均已在仓库中落地，操作方式见 [phoenix/docs/训练指引.md](../../phoenix/docs/训练指引.md)。本节保留阶段划分，作为理解训练闭环建设顺序的框架。

```
阶段 1（已完成）
  用随机参数跑通推理流程
  → scripts/run_ranker.py / scripts/run_retrieval.py 可正常运行

阶段 2（已完成）
  用模拟数据 + 随机 labels 跑通训练循环（不需要真实数据）
  → uv run scripts/train_ranker.py（optax 已在依赖中）

阶段 3（已完成，需自备数据）
  接入真实行为日志数据
  → 参考 ../training/training_data_spec.md 的字段规范
  → 工具链：examples/generate_example_data.py + data_preprocessor.py
  → uv run scripts/train_ranker.py --data-dir ./data/training_samples

阶段 4（已完成）
  保存训练产物，接回推理流程
  → 训练自动保存 model_params_step*.npz + embedding_tables.npz
  → 服务加载：scripts/run_services.py / scripts/run_grpc_gateway.py 的 checkpoint 参数
```

### 6.1 训练数据来自哪里

详细字段规范见 [../training/training_data_spec.md](../training/training_data_spec.md)。简要说：

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

> 上述为教学用骨架示意；仓库中可运行的完整实现见 `scripts/train_ranker.py`（含数据加载、多目标损失与 checkpoint 保存）。详细讨论见 `06-training-and-data.md §10`。

---

## 7. 调试常见问题

| 现象 | 原因 | 解法 |
|---|---|---|
| 输出全相同分数 | 嵌入表第 0 行没有置零，padding 位被赋了随机向量 | `emb_table[0] = 0.0` |
| 历史被完全忽略 | `history_post_hashes` 全为 0 | 检查哈希函数是否返回了 0 |
| 形状不匹配报错 | 忘记多哈希维度，写成 `[B, S, D]` 而非 `[B, S, 2, D]` | 查表结果是 `[..., num_hashes, D]` |
| 候选评分异常 | 演示训练候选位不足 8 个 | 训练侧补虚拟候选（哈希非 0，label 全 0）；在线请求由网关分块，不必先裁成 8 |

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
| 训练数据字段规范 | [../training/training_data_spec.md](../training/training_data_spec.md) | 原始日志字段定义、SQL 示例、Parquet 格式 |
| 训练侧全局分析 | `docs/phoenix/06-training-and-data.md` | 训练与推理一致性、损失函数推断、训练缺口清单 |
| 本文档 | `docs/phoenix/07-real-data-integration.md` | 哈希嵌入实操、构造代码、保存/加载、阶段路径 |
| 可运行示例 | `phoenix/scripts/run_real_data_demo.py` | 端到端代码，直接 `uv run scripts/run_real_data_demo.py` |
| 双塔模型输入输出 | `phoenix/docs/双塔模型输入输出指引文档.md` | 召回模型专项说明 |
