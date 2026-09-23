# Home Mixer 身份边界五阶段加固 — 修复后复核报告

## 复核范围

本次复核针对 `docs/implementation/id-boundary-hardening-review.md` 原报告列出的 B/H/M/L 问题，核对当前工作树源码、测试、文档和质量门禁，并完成可直接修复项。

## 当前结论

原报告中的阻断项和质量门禁问题已修复，身份边界相关的主要设计问题也已处理。

| 项目 | 当前状态 |
|---|---|
| 集成测试跨 crate FRU 私有字段 E0451 | ✅ 已通过 builder / 显式 fixture 修复 |
| `cargo fmt --all -- --check` | ✅ 通过 |
| `cargo check -p home-mixer --tests` | ✅ 通过 |
| `cargo clippy -p home-mixer --all-targets -- -D warnings` | ✅ 通过 |
| `cargo test --workspace` | ✅ 546 passed / 23 ignored |
| `cargo test -p home-mixer --no-default-features --lib` | ✅ 426 passed |
| `cargo test -p id-service` | ✅ 38 passed / 2 ignored |

## 已完成修复

### 1. 清理 clippy 门禁错误

- 删除 `RegistryClient` 记录调用中的多余 `&method`。
- 删除 `mrpyq_adapters` 装配链中已无消费者的 `identity` 参数及相关调用点。
- 删除无效的 `client_identity` 测试变量。
- 删除同步 `impl Default for IdentityContext` 上错误的 `#[tonic::async_trait]`。
- 删除已无生产调用方的旧 `within_budget` helper 及其测试，统一使用绝对 deadline 的 `within_deadline`。

### 2. 修复 `ScoredPostsQuery` 的隐式测试默认值

移除了：

```rust
impl Default for ScoredPostsQuery {
    fn default() -> Self {
        Self::test_default()
    }
}
```

所有 Home Mixer 内部 query fixture 改为显式使用：

```rust
ScoredPostsQuery::test_default()
```

这样测试身份 resolver 和写能力不会再通过 `Default::default()` 隐式进入生产代码路径。

同时更新了字段注释，并将 `with_request_identity` 的 reader/registration 同源校验从 `debug_assert!` 改为 release 构建也生效的 `assert!`。

### 3. 收紧 `ForYouFeedOutput` 的身份能力暴露

`ForYouFeedOutput.identity_context` 已从公开字段改为：

```rust
pub(crate) identity_context: Arc<IdentityContext>
```

外部 crate 不再能直接取得 request-local identity context。

### 4. 修正 viewer mapping 缺失错误码

QueryBuilder 中 viewer mapping 缺失现在返回：

```rust
Status::not_found("viewer ID mapping missing")
```

不再返回 `Unavailable`，避免客户端对确定性缺失进行无效重试。新增了 `missing_viewer_mapping_returns_not_found` 回归测试，并同步更新请求生命周期文档。

### 5. partial resolve 增加负缓存

forward cache 现在区分：

```rust
enum ForwardCacheEntry {
    Found(SnowflakeId),
    Missing,
}
```

partial resolve 返回 `None` 后会记录 `Missing`，同一 request context 再次解析相同未知 hint 不会重复访问 Registry。partial 结果也改为一次性批量写入缓存，避免逐条加锁。

严格 `resolve` 和 `allocate` 仍会重试已知的 negative entry，避免 partial resolve 的暂时性缺失阻塞后续需要强一致结果的路径。

新增回归测试：

- `request_identity_context_caches_partial_misses`

### 6. 拆分主路径和 side effect 统计

`IdentityContext::for_side_effect` 继续共享：

- forward/reverse cache
- single-flight 状态

但现在使用独立的 `IdentityContextStatsAtomic`。因此：

- 主路径快照只统计主路径调用
- side effect 快照只统计 side effect 调用
- 两份账单不再共享累计计数

新增/更新回归测试：

- `side_effect_identity_view_restarts_budget_but_shares_cache_with_independent_bill`

### 7. 修正文档

更新了：

- `docs/home-mixer/02-request-lifecycle.md`
  - 明确 viewer mapping 缺失返回 `NotFound`
- `docs/home-mixer/09-debugging-and-observability.md`
  - 明确主路径和 side effect 统计独立、不可混淆
- `docs/implementation/id-boundary-hardening-goal.md`
  - 补充 clippy、no-default-features、独立 stats、negative cache 等最终验证项
  - Completion Note 改为与实际验证结果一致

## 当前仍保留的有意设计

### `IdentityContext::default`

`IdentityContext` 自身仍保留默认实现，供现有测试/fixture 使用；但 `ScoredPostsQuery` 已不再实现 `Default`，生产 query 不会通过 query 默认构造隐式获得 padded identity context。

### `IdentityReader::resolve_batch_partial` 默认 fallback

trait 默认实现仍保留逐条 fallback，用于兼容测试和 legacy resolver。生产 `RegistryClient`、`IdentityContext` 等实现使用真正的 partial batch 路径。该 fallback 仍应避免被新的生产 resolver 无意继承；后续可考虑将其改为显式必实现接口。

### `within_deadline`

所有 RPC 多阶段路径使用绝对 deadline。旧的 `within_budget` 已删除，避免调用者误以为后续阶段可以重新开始一段完整预算。

## 验证命令

```text
cargo fmt --all -- --check
cargo check -p home-mixer --tests
cargo clippy -p home-mixer --all-targets -- -D warnings
cargo test --workspace
cargo test -p home-mixer --no-default-features --lib
cargo test -p id-service
```

结果：全部通过。

## 后续修复补充

在上述复核之后又完成了两项结构性收口：

- `IdentityContext` 的 reverse cache 扫描与 single-flight 计划提取为共享 helper，避免加锁前后复制整段扫描逻辑。
- `ScoredPostsQuery` 将 reader 与 registration capability 收拢到 `QueryIdentity` 聚合对象中，避免两个字段被独立替换；所有 query fixture 继续使用显式 `ScoredPostsQuery::test_default()`。

本次补充验证：

```text
cargo fmt --all
cargo check -p home-mixer --tests
cargo clippy -p home-mixer --all-targets -- -D warnings
cargo test --workspace
```

上述命令均通过。per-key single-flight 仍保留为有意设计；当前尚未引入复杂 batch coalescer，部分重叠 batch 的延迟收益仍需线上数据验证。
