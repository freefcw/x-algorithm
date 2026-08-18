# Phoenix 模型训练数据规格说明

状态：`design`

本文档描述从你的业务平台中提取训练数据的具体字段要求、格式规范和 SQL 参考示例。

---

## 1. 核心概念说明

Phoenix 是一个 **Transformer 精排模型**，输入由两部分组成：

- **`RecsysBatch`**：原始 ID / 哈希 / 行为标签（整数 + 浮点数组）
- **`RecsysEmbeddings`**：由业务侧 ID 查出的嵌入向量（浮点数组）

每一条训练样本对应一个"**曝光事件**"：某个用户在某个时刻看到了一批候选帖子，并在其中产生了若干互动行为。

---

## 2. 需要采集的原始业务数据

### 2.1 曝光事件表（核心训练样本）

每一行 = 一次推荐曝光。

| 字段名 | 类型 | 说明 |
| :--- | :--- | :--- |
| `user_id` | INT64 | 被推荐的用户 ID |
| `impression_time` | TIMESTAMP | 曝光发生时间 |
| `candidate_post_ids` | ARRAY\<INT64\> | 这次曝光展示的帖子 ID 列表（顺序对应排名）|
| `candidate_author_ids` | ARRAY\<INT64\> | 候选帖子各自的作者 ID（与上面一一对应）|
| `product_surface` | INT | 曝光场景：0=首页推荐 1=关注流 2=搜索 3=话题 … 最多16类 |

### 2.2 互动行为日志表（训练标签来源）

每一行 = 用户对某帖子的一次互动。

| 字段名 | 类型 | 说明 |
| :--- | :--- | :--- |
| `user_id` | INT64 | 用户 ID |
| `post_id` | INT64 | 帖子 ID |
| `action_type` | STRING | 行为类型（见下方行为编码表）|
| `action_time` | TIMESTAMP | 行为发生时间 |
| `dwell_seconds` | FLOAT | 仅 `dwell` 行为填写，单位秒 |

**行为编码表（对应模型中 19 个预测目标的顺序）：**

| 索引 | `action_type` 枚举值 | 含义 |
| :--- | :--- | :--- |
| 0 | `favorite` | 点赞 |
| 1 | `reply` | 回复 |
| 2 | `repost` | 转发 |
| 3 | `photo_expand` | 点击展开图片 |
| 4 | `click` | 点击帖子正文 |
| 5 | `profile_click` | 点击作者头像/主页 |
| 6 | `vqv` | 视频完整播放（或高质量播放）|
| 7 | `share` | 分享（任意方式）|
| 8 | `share_via_dm` | 私信分享 |
| 9 | `share_via_copy_link` | 复制链接分享 |
| 10 | `dwell` | 停留超过阈值（建议 ≥ 2 秒算 1，否则 0）|
| 11 | `quote` | 引用转发 |
| 12 | `quoted_click` | 点击引用内容 |
| 13 | `follow_author` | 关注作者 |
| 14 | `not_interested` | 点击"不感兴趣" |
| 15 | `block_author` | 屏蔽作者 |
| 16 | `mute_author` | 静音作者 |
| 17 | `report` | 举报 |
| 18 | `dwell_time` | 归一化停留时长（连续值，见§4说明）|

> **MVP 简化建议**：如果初期埋点不完整，至少保证采集 `favorite`、`reply`、`repost`、`click`、`dwell` 这 5 类，其余行为对应位置置 0 即可。

### 2.3 用户历史行为序列表（上下文特征）

每一行 = 用户最近的一条历史互动记录。

| 字段名 | 类型 | 说明 |
| :--- | :--- | :--- |
| `user_id` | INT64 | 用户 ID |
| `post_id` | INT64 | 历史互动帖子 ID |
| `author_id` | INT64 | 该帖子的作者 ID |
| `action_vector` | ARRAY\<FLOAT\>[19] | 该次互动的多热行为向量（0/1）|
| `product_surface` | INT | 互动发生的场景编码 |
| `action_time` | TIMESTAMP | 互动时间（用于按时间倒序截取最近 N 条）|

---

## 3. 数据格式与 Tensor 形状

训练时每个样本（batch size = B = 1）的 Tensor 形状如下：

### 3.1 `RecsysBatch` — 原始特征

| Tensor 字段 | 形状 | dtype | 说明 |
| :--- | :--- | :--- | :--- |
| `user_hashes` | `[B, 2]` | int32 | 用户 ID 经过 2 个独立哈希函数映射后的整数值 |
| `history_post_hashes` | `[B, 32, 2]` | int32 | 最近 32 条历史帖子 ID 的 2 路哈希；不足的位置补 0 |
| `history_author_hashes` | `[B, 32, 2]` | int32 | 历史帖子作者 ID 的 2 路哈希；不足的位置补 0 |
| `history_actions` | `[B, 32, 19]` | float32 | 每条历史记录对应的行为多热向量（0.0 或 1.0）|
| `history_product_surface` | `[B, 32]` | int32 | 历史记录发生的场景编码（0~15）|
| `candidate_post_hashes` | `[B, 8, 2]` | int32 | 候选帖子 ID 的 2 路哈希 |
| `candidate_author_hashes` | `[B, 8, 2]` | int32 | 候选帖子作者 ID 的 2 路哈希 |
| `candidate_product_surface` | `[B, 8]` | int32 | 候选帖子曝光场景编码 |

### 3.2 `RecsysEmbeddings` — 预查嵌入向量

> 嵌入向量由业务侧的 embedding 服务（或离线 embedding 表）根据 ID 查出。
> 训练时推荐先用随机初始化表，稳定后可替换为预训练的内容嵌入（如文本 BERT 向量）。

| Tensor 字段 | 形状 | dtype | 说明 |
| :--- | :--- | :--- | :--- |
| `user_embeddings` | `[B, 2, 128]` | float32 | 用户 2 路哈希各自查出的嵌入（D=128）|
| `history_post_embeddings` | `[B, 32, 2, 128]` | float32 | 历史帖子嵌入 |
| `candidate_post_embeddings` | `[B, 8, 2, 128]` | float32 | 候选帖子嵌入 |
| `history_author_embeddings` | `[B, 32, 2, 128]` | float32 | 历史作者嵌入 |
| `candidate_author_embeddings` | `[B, 8, 2, 128]` | float32 | 候选作者嵌入 |

### 3.3 训练标签

| Tensor 字段 | 形状 | dtype | 说明 |
| :--- | :--- | :--- | :--- |
| `labels` | `[B, 8, 19]` | float32 | 候选帖子上实际发生的互动（0/1），对应 19 个目标行为 |

---

## 4. 特殊字段处理说明

### 4.1 哈希计算方法

将业务 ID（int64）映射为模型输入的 int32 哈希值，使用 2 路独立哈希以减少碰撞：

```python
import hashlib

def id_to_hashes(id_val: int, num_hashes: int = 2, table_size: int = 100_000) -> list[int]:
    hashes = []
    for seed in range(num_hashes):
        raw = hashlib.md5(f"{seed}:{id_val}".encode()).digest()
        h = int.from_bytes(raw[:4], "little") % table_size
        hashes.append(h + 1)  # 0 保留为 padding，所以从 1 开始
    return hashes
```

> 注意：推理侧演示脚本 `phoenix/scripts/run_real_data_demo.py` 的 `hash_id` 用的是 `hash((entity_id, seed))` 实现，与本节 MD5 实现产出的哈希值不同。训练数据准备与推理输入构造必须使用同一种实现，否则嵌入表查找会错位。

### 4.2 `dwell_time`（索引 18）的归一化

`dwell_time` 是一个连续值而非 0/1，建议用对数归一化后压缩到 [0, 1]：

```python
import math

def normalize_dwell(seconds: float, max_seconds: float = 300.0) -> float:
    return min(math.log1p(seconds) / math.log1p(max_seconds), 1.0)
```

### 4.3 历史序列截断与 Padding

- 按 `action_time` **降序**排列，取最近 **32 条**。
- 不足 32 条的，在末尾补零（`post_hash=0`, `author_hash=0`, `actions=[0]*19`, `product_surface=0`）。
- 模型通过 `post_hash == 0` 自动生成 padding mask，补零位置不参与 Attention 计算。

### 4.4 候选集大小

- 单次推理固定为 **8 个候选**（`candidate_seq_len=8`）。
- 如果召回的候选不足 8 个，剩余位置同样补零。

---

## 5. 参考 SQL（以 Hive / Spark SQL 为例）

### 5.1 拼接单条训练样本（曝光 + 互动标签）

```sql
SELECT
    e.impression_id,
    e.user_id,
    e.impression_time,
    e.candidate_post_ids,   -- ARRAY<BIGINT>, 长度固定 8
    e.candidate_author_ids,
    e.product_surface,
    -- 逐帖子逐行为聚合成 label 向量
    COLLECT_LIST(
        STRUCT(a.post_id, a.action_type, a.dwell_seconds)
    ) AS interactions
FROM impressions e
LEFT JOIN action_log a
    ON  e.user_id = a.user_id
    AND a.action_time BETWEEN e.impression_time AND e.impression_time + INTERVAL 30 MINUTES
    AND ARRAY_CONTAINS(e.candidate_post_ids, a.post_id)
GROUP BY 1, 2, 3, 4, 5, 6
```

### 5.2 构建用户历史序列（最近 32 条）

```sql
SELECT
    user_id,
    post_id,
    author_id,
    action_vector,   -- 已预处理为长度 19 的 FLOAT 数组
    product_surface,
    action_time
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY action_time DESC) AS rn
    FROM user_action_sequence
    WHERE action_time < '{inference_time}'
) t
WHERE rn <= 32
ORDER BY user_id, rn
```

---

## 6. 最终文件格式建议

推荐将提取好的样本保存为 **Parquet** 文件（分区按日期）：

```
training_data/
  date=2026-04-01/
    part-00000.parquet
    part-00001.parquet
  date=2026-04-02/
    ...
```

每个 parquet 行包含以下列（Python dict 形式）：

```python
{
    "user_id": 123456,
    "user_hashes": [1024, 8831],                        # shape [2]
    "history_post_hashes": [[h1, h2], ...],             # shape [32, 2]，不足补 [0,0]
    "history_author_hashes": [[h1, h2], ...],           # shape [32, 2]
    "history_actions": [[0,1,0,...], ...],               # shape [32, 19]
    "history_product_surface": [0, 1, 0, ...],          # shape [32]
    "candidate_post_hashes": [[h1, h2], ...],           # shape [8, 2]
    "candidate_author_hashes": [[h1, h2], ...],         # shape [8, 2]
    "candidate_product_surface": [0, 0, ...],           # shape [8]
    "labels": [[0,0,1,...], ...],                       # shape [8, 19]，训练标签
}
```

---

## 7. 数据规模建议（MVP 阶段）

| 指标 | 建议值 |
| :--- | :--- |
| 最少训练样本量 | 50 万曝光事件（正样本率 > 5%）|
| 历史序列长度 | 32（可配置，增大会显著提升显存需求）|
| 候选集大小 | 8（生产环境可调整为 16 或 32）|
| 嵌入维度 D | 128（MVP），扩展时可改为 256 |
| 正负样本比例 | 保持自然分布即可，不需要刻意下采样 |
