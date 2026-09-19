# Phoenix 真实数据接入

> **状态：`current-code`**

本文说明真实事件如何进入 xrex 训练与推理。它不是旧 Demo 的数据预处理教程，也不提供随机输入、旧 gateway 或 synthetic corpus 的启动命令。

## 1. 输入边界

当前生产数据有两类：

| 输入 | 用途 | 事实来源 |
| --- | --- | --- |
| served-candidates 事件 | 定义用户实际看到的候选和曝光位置 | `docs/implementation/served-candidates-event-contract.md`、`phoenix/scripts/build_training_inputs.py` |
| UAS 行为事件 | 构造请求前的用户历史和监督标签 | `docs/implementation/uas-event-contract.md`、xrex streaming loader |

事件必须能按用户、帖子、时间和请求关联。缺少曝光事实时，互动日志不能直接当作曝光样本；缺少统一 ID 空间时，训练和 serving 会产生不可见的错配。

## 2. 数据处理原则

- 使用稳定的生产 ID 编码和同一套 feature schema；不要使用 Python 内置 `hash()`；
- 以请求时间截断历史，归因窗口之后发生的行为只能作为标签；
- 对删除、不可见、无权限内容执行数据治理，而不是在训练阶段静默保留；
- 对坏事件、重复事件和未知 action 做计数并隔离；
- 记录输入分区、版本、延迟和丢弃原因，保证样本可回放。

## 3. 接入验收

在将数据交给训练器前，至少完成：

1. schema/版本兼容检查；
2. 用户、帖子、作者 ID 的稳定性检查；
3. 时间窗口和归因窗口测试；
4. 正负样本比例、缺失率和重复率报告；
5. 小批量 Parquet 读取、重启恢复和大批量吞吐测试；
6. 与 serving 的固定样本逐字段对照。

训练和部署入口统一见 [06-training-and-data.md](./06-training-and-data.md) 与 [08-production-handbook.md](./08-production-handbook.md)。Home Mixer 当前 Phoenix 请求合同与 xrex serving 合同尚未适配完成，因此完成数据接入不等于完成在线模型接入。
