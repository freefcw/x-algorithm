# 提交 `fee1d0f` 能力清点与吸收结果

> 上游范围：`75d93d9..fee1d0f`；可移植项已逐项对照。

提交规模：33 files，`+1587/-391`。

| 上游变化 | 分类 | 处置 |
|---|---|---|
| `phoenix/xrex/models/recsys_two_tower_model.py` padding segment id 传播 | U0 | 已吸收 `PADDING_SEGMENT_ID` 与 `padding_mask` 语义。 |
| `phoenix/crates/serving/xai-recsys-engine/src/python.rs` logits list 响应映射 | U3 | 依赖本地 proto 与 `PredictRequestItem` 均不存在的 `return_logits_list` / `requested_action_logits` 字段；试移植会编译失败，已撤回，待公开合同整体出现后重入。 |
| 生产两份 `recsys.proto` 的实验/产品枚举字段 | U3 | 本地 slim 协议冻结且无消费方，不引入；不能只补字段制造半套合同。 |
| two-tower 训练侧其余生产配置调整 | U3 | 依赖生产数据/模型配置，保持记录。 |
| visibility-filtering、广告日志、under-the-hood | 不适用/U3 | 本地无对应服务或广告槽位未装配。 |

运行受影响 Phoenix Rust/Python 测试；生产 proto 未消费增量明确保持 U3。
