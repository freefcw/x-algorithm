# 提交 `75d93d9` 能力清点与吸收结果

> 上游范围：`49815da..75d93d9`；可移植 xrex 变更已吸收。

提交规模：28 files，`+801/-252`。

| 上游变化 | 分类 | 处置 |
|---|---|---|
| `phoenix/xrex/configs/xrecsys.py` 删除过时配置 | U0 | 已吸收。 |
| `phoenix/xrex/inference/model_runner.py` NUMA 绑定改用 `schedulable_cpus` + affinity | U0/U1 | 已吸收；Linux NUMA 分支未在 macOS 验证。 |
| `phoenix/xrex/models/recsys_model.py` `enable_day_of_week` | U0 | 已吸收。 |
| `phoenix/xrex/utils/gpu.py` CPU 列表解析、隔离 CPU 排除与 affinity | U0 | 已吸收；Linux sysfs 分支仅完成源码检查。 |
| VF hydrator/filter 与 visibility-filtering | U3 | 依赖本地不存在的 VF 合同，不移植；不以 GPU 为理由跳过纯源码。 |
| `util/urt/ad_marshaller.rs` | U3 | `xai_urt_thrift` 未公开，保持延期。 |
| 其余 grox / proto 变化 | 不适用/U3 | 不在本地主链路或缺少公开消费方。 |

验证：运行受影响 Python 静态检查与测试；NUMA/GPU 分支未在 macOS 执行，属于验证限制而非移植拒绝。
