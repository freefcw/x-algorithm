在 X 的推荐系统中，Params 模块（通常指 home-mixer/params）是控制算法行为的“神经中枢”。

简单来说，如果 home-mixer 是推荐系统的骨架（逻辑），那么 Params 就是填充在骨架上的肌肉和细节（数据参数）。由于这些参数包含高度敏感的商业机密和防滥用逻辑，它们被明确排除在开源版本之外（参见 home-mixer/lib.rs:5）。

以下是 Params 模块的具体作用：

1. 存储评分权重 (Ranking Weights)
它是 WeightedScorer（加权评分器）的数据源。

作用：决定点赞、转推、回复、点击等行为各自占多少分。
示例：Params 可能定义了 REPOST_WEIGHT = 2.0，而 LIKE_WEIGHT = 1.0。
影响：直接决定了你的推文是靠“有点赞”排在前面，还是靠“有人讨论”排在前面。
2. 定义过滤阈值 (Filtering Thresholds)
它是 AgeFilter 等过滤器的配置中心。

作用：规定哪些内容“不合格”。
代码体现：在 phoenix_candidate_pipeline.rs:112 中，过滤器使用 params::MAX_POST_AGE 来决定推文的最长有效期（比如超过 24 小时的推文自动剔除）。
3. 控制召回规模 (Candidate Sizes)
它决定了每个阶段处理多少数据。

作用：平衡性能与准确性。
示例：params::TOP_K_CANDIDATES_TO_SELECT 决定最终筛选出多少条推文展示给用户（通常是 20-30 条），而 params::RESULT_SIZE 决定初始召回多少候选者（通常是 800 条左右）。
4. 系统运行参数 (System Constants)
包含底层性能相关的配置。

作用：如 MAX_GRPC_MESSAGE_SIZE 规定了服务器之间通信的最大数据包大小。
为什么在 simulator 里它很重要？
因为 Params 模块缺失，原始的 X 算法代码在本地是无法直接跑出真实效果的（甚至无法通过编译，因为到处都在 use crate::params）。

simulator 解决这个问题的方法是：

重写逻辑：在 simulator/src/scoring/ 中重写了原本依赖 Params 的评分逻辑。
配置文件化：将原本硬编码在 Params 模块里的“秘密参数”改为了用户可调节的 config/scoring.toml。
启发式替代：用用户自定义的权重（Heuristic weights）来模拟生产环境的效果。
总结： Params 模块就像是推荐系统的调音台面板。原始代码里这个面板被“锁上”并移除了，而 simulator 为你造了一个新的、可以自由拨动的调音台。