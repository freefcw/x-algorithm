# 提交 `6bb4594` 能力清点与吸收结果

> 上游范围：`fee1d0f..6bb4594`；CPU 可移植项已吸收，外部数据面按 U3 保留。

提交规模：24 files，`+1397/-338`。

| 上游变化 | 分类 | 处置 |
|---|---|---|
| `phoenix/xrex/data/streaming/{kafkaconsumer,kafkaloader}.py`、`rust_kafka_recsys.py` KafkaAuth/mTLS 与 CA 探测 | U0/U1 | 已吸收认证抽象、mTLS/SASL 参数传递、平台 CA 路径与 Rust provider 对接；真实 Kafka/TLS 集群未验证。 |
| `vm-ranker/dpp.rs`、`scoring/dpp_model.rs`、`metrics.rs` DPP/指标调整 | U0/U3 | 已吸收纯 Rust DPP/指标算法；上游 `seed_tweet_id` 依赖本地 `RankRequest` 不存在，`build_seed_input` 保持移除并维持 DPP `None` 语义，待协议契约出现后重入。VM Ranker 仍按本地开关控制。 |
| 生产 `recsys.proto` 的 `returnLogitsList`、实验/产品枚举等 | U3 | 本地 slim 协议无消费方，保持协议冻结；已有可移植响应映射已同步。 |
| `home-mixer/filters/brazil_2026_election_filter.rs` | U5 | 产品不适用，专用过滤器不移植。 |
| `home-mixer/side_effects/phoenix_experiments_side_effect.rs` | U3 | Kafka/实验端口未提供，不装配。 |
| xrex Kafka 生产部署与引擎数据面 | U3 | 源码已吸收，认证、主题、保留策略与生产验收仍需外部依赖。 |

验证：运行 vm-ranker Rust 测试及 Phoenix 受影响测试；Kafka、GPU、生产服务仅完成源码检查，不宣称已上线。
