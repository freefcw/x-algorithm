# X (Twitter) 推荐算法：双塔模型深度分析

状态：`research`

本文是外部资料和算法背景整理，不作为当前仓库行为的事实来源。当前 Phoenix 召回实现请看 [../phoenix/03-retrieval-pipeline.md](../phoenix/03-retrieval-pipeline.md)。

在 X（原 Twitter）的推荐系统中，双塔模型（Two-Tower Model）是其核心架构的重要组成部分，尤其在处理海量候选 Tweet 的“候选生成”（Candidate Generation，即召回）阶段发挥着至关重要的作用 <cite>[Singhajit](https://singhajit.com/system-design/x-twitter-for-you-algorithm/)</cite><cite>[Github](https://github.com/xai-org/x-algorithm)</cite>。

## 1. 为什么采用双塔模型？

X 需要在极短的时间内（毫秒级）从每日数亿条新增推文中，为用户筛选出最相关的约 1500 条候选推文。面对如此庞大的搜索空间，直接使用复杂的深度模型（如 Transformer）进行全量计算是不现实的。

### 核心采用原因：
*   **计算效率与实时性**：双塔模型通过将用户（User）和候选内容（Candidate/Tweet）解耦，使得海量的推文向量可以预先计算并存储在向量数据库中。在用户请求时，只需计算用户侧的向量，然后进行高效的向量相似度检索 <cite>[Bytebytego](https://blog.bytebytego.com/p/the-algorithm-that-powers-your-x)</cite><cite>[Csdn](https://blog.csdn.net/weixin_48166466/article/details/129113136)</cite>。
*   **非关注关系发现（Out-of-Network）**：双塔模型能够突破“关注关系图谱”的限制，通过语义空间的相似性，为用户发现其未关注但可能感兴趣的内容，这是“For You” feed 流的核心 <cite>[Bytebytego](https://blog.bytebytego.com/p/the-algorithm-that-powers-your-x)</cite><cite>[Github](https://github.com/xai-org/x-algorithm)</cite>。

---

## 2. 双塔模型的基本原理

双塔模型由两个独立的神经网络组成：**用户塔（User Tower）**和**物品塔（Candidate Tower/Tweet Tower）** <cite>[Bytebytego](https://blog.bytebytego.com/p/the-algorithm-that-powers-your-x)</cite>。

*   **用户塔**：将用户的历史互动、关注列表、元数据等特征编码为一个高维向量（User Embedding）。
*   **物品塔**：将推文的文本内容、媒体信息、作者属性等特征编码为同维度的向量（Post Embedding）。
*   **匹配机制**：系统通过计算两个向量的**点积（Dot Product）**或余弦相似度来衡量匹配程度。得分越高，表示该推文与用户的兴趣越吻合 <cite>[Bytebytego](https://blog.bytebytego.com/p/the-algorithm-that-powers-your-x)</cite><cite>[Basenor](https://www.basenor.com/blogs/news/x-recommendation-algorithm-gets-major-update-and-goes-open-source?srsltid=AfmBOor8bkTQBwxqN5S01ZcEmsCjBAoHWCfUHfuYOxyPkplgxQfJIviY)</cite>。

---

## 3. 双塔模型的优势与劣势

双塔模型是在“精度”与“速度”之间进行的一种战略性折中。

### 优势 (Benefits)
*   **极高的检索速度**：得益于向量化检索（如 ANN 算法），模型能在毫秒内扫描数百万个候选者 <cite>[Bytebytego](https://blog.bytebytego.com/p/the-algorithm-that-powers-your-x)</cite>。
*   **强大的扩展性**：支持对海量内容的实时召回，适合 X 这样高并发、高更新率的社交平台 <cite>[Substack](https://machinelearningatscale.substack.com/p/xai-recommendation-system-deep-dive)</cite>。
*   **模型解耦**：物品侧向量可以离线批量更新，不占用在线请求的计算资源 <cite>[Csdn](https://blog.csdn.net/weixin_48166466/article/details/129113136)</cite>。

### 劣势 (Drawbacks)
*   **缺乏特征交叉**：由于用户塔和物品塔在最终计算相似度之前是完全独立的，模型无法捕捉到用户与推文之间的复杂交互特征（Cross Features），例如“特定用户对特定推文关键词的敏感度” <cite>[Bytebytego](https://blog.bytebytego.com/p/the-algorithm-that-powers-your-x)</cite><cite>[Techbeat](https://www.techbeat.net/article-info?id=4223)</cite>。
*   **表达能力受限**：相比于后续的“精排模型（Heavy Ranker）”，双塔模型的逻辑相对简单，精度较低 <cite>[Bytebytego](https://blog.bytebytego.com/p/the-algorithm-that-powers-your-x)</cite><cite>[Tencent](https://cloud.tencent.com/developer/article/1905527)</cite>。
*   **向量压缩损失**：将用户丰富多变的历史行为压缩为一个单一向量，可能会损失一些细节兴趣 <cite>[Medium](https://medium.com/@sherrysun/a-visual-dissection-of-xs-recommendation-algorithm-understanding-the-transformer-behind-it-2ff89436a0c7)</cite>。

---

## 4. 解决了什么核心问题？

在 X 的算法流水线中，双塔模型主要解决了以下挑战：

1.  **大规模召回难题**：解决了如何从数亿候选池中快速筛选出初步候选集的问题 <cite>[Substack](https://machinelearningatscale.substack.com/p/xai-recommendation-system-deep-dive)</cite><cite>[Medium](https://thegowtham.medium.com/deep-dive-inside-x-fka-twitter-s-recommendation-algorithm-460b2bd4e26a)</cite>。
2.  **内容发现效率**：通过数学空间建模，解决了用户在没有强社交关系引导下的兴趣发现问题 <cite>[Bytebytego](https://blog.bytebytego.com/p/the-algorithm-that-powers-your-x)</cite>。
3.  **系统资源平衡**：它作为“轻量级过滤器”，滤掉了 99% 以上的不相关内容，使得计算资源昂贵的“精排模型”只需处理最有潜力的推文，平衡了性能与效果 <cite>[Bytebytego](https://blog.bytebytego.com/p/the-algorithm-that-powers-your-x)</cite><cite>[Mintlify](https://www.mintlify.com/twitter/the-algorithm/how-it-works)</cite>。

---

## 5. 与精排模型（Heavy Ranker）的区别

| 特性 | 双塔模型 (Candidate Generation) | 精排模型 (Heavy Ranker) |
| :--- | :--- | :--- |
| **阶段** | 召回 / 粗选 | 排序 / 打分 |
| **处理规模** | 数百万推文 | 数千推文 |
| **计算复杂度** | 低（向量点积） | 高（深度神经网络/Transformer） |
| **特征交互** | 无（只有最终点积） | 全特征交互 <cite>[Bytebytego](https://blog.bytebytego.com/p/the-algorithm-that-powers-your-x)</cite> |
| **目标** | 快速找到“可能相关”的内容 | 精确预测点击、转发等行为概率 <cite>[Dev](https://dev.to/axrisi/the-ultimate-guide-to-getting-recommended-by-twitters-algorithm-99m)</cite><cite>[Medium](https://thegowtham.medium.com/deep-dive-inside-x-fka-twitter-s-recommendation-algorithm-460b2bd4e26a)</cite> |

综上所述，双塔模型是 X 推荐系统中负责“大步快跑”的部分，它通过牺牲一部分模型复杂度，换取了处理海量数据的能力，为后续的精细化排序奠定了坚实基础 <cite>[Singhajit](https://singhajit.com/system-design/x-twitter-for-you-algorithm/)</cite><cite>[Bytebytego](https://blog.bytebytego.com/p/the-algorithm-that-powers-your-x)</cite>。
