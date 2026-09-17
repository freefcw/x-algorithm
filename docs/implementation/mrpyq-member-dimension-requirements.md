# 推荐链路皮维度对齐：对 mrpyq 的接口要求

> **状态**：`proposed`（待 mrpyq 侧排期，推荐侧改动同步待启动）
> **日期**：2026-09-14（2026-09-17 更新）
> **读者**：mrpyq feed/webapi 后端、rec-bff 维护者、推荐服务开发
> **前提**：相关功能尚无历史数据，所有改动不考虑存量迁移与兼容期
> **事实边界**：本文「要求」是推荐侧提出的接口约定，未经 mrpyq 侧确认。
>
> **2026-09-17 架构变更说明**：本文初版假设合同由 mrpyq 仓库直接实现（当时引用的 `app/feed/service/internal/service/recommendation_data.go` 并未合入 mrpyq `dev`，现该文件不存在）。当前合同承载方是 **rec-bff**（`recommend/rec-bff`，独立 Go 门面）：它逐字实现 `recommendation_data.proto` + `viewer_relation.proto`，翻译到 mrpyq Feed / Account RPC。因此：
> - **§2.2（NETWORK 读皮维度收件箱）已由 rec-bff 满足**（直接调 `ListMemberFollowInboxV1`；且不看 7 天 TTL 的 `is_init` 标记，`source_ready` 语义与 proto 注释的偏差记录在 rec-bff README「为什么不看 is_init」）；
> - §2.1 字段改名、§3 作者字段收敛、§4 viewer 关系换皮、§5 底层改造**仍然待做**，且改动时必须与 rec-bff 同批（它实现对的是当前合同，字段一改两边编译期就对不上，不会静默错位）；
> - §6.1 的同批上线约束从「推荐侧 ↔ mrpyq」扩展为「推荐侧 ↔ rec-bff ↔ mrpyq」三方，见该节重写。

---

## 1. 背景与原则

推荐系统的一切实体都以**皮（member）**为单位：召回的 viewer 是皮，关注关系是皮与皮之间的，帖子的作者是皮，可见性判断是皮对皮。**账号（account）在推荐系统里不是一个概念**，不应出现在任何接口上。

这一点在 mrpyq 现有实现里只对了一半。帖子侧是对的——发帖入口 `app/webapi/interface/internal/data/feed.go:435` 写的是 `MemberId: profile.Id`，`do.Profile` 就是皮，落库后 `po.Feed.MemberId` 的注释也直接写着 `// 皮id`。但召回和可见性两条链路都还停在账号维度，且皮在 mrpyq 内部存在两种编码（皮自己的 ObjectId `member_id`，以及 `user_id + user_no` 拼成的 `member_key`），不同存储各用一种，接口上也混着出。

因此本次对齐的贯穿原则是：

> **接口层只出现皮的 `member_id`（皮自己的 ObjectId，24 位小写 hex），不出现 `account_id`、`user_id`、`user_no`。**

推荐系统内部的身份类型就是 ObjectId，`Feed.member_id` 已经是这个形态，直接对得上，中间不需要任何翻译层。mrpyq 内部要转 `member_key` 是其自身实现细节，不应漏到接口上。

---

## 2. `ListRecommendationCandidates` — 召回入口

### 2.1 请求入参换成皮

| 字段 | 现状 | 要求 |
|---|---|---|
| `account_id` | 字段名沿用 proto；home-mixer 把 viewer 皮 `member_id` 放进这个字段，rec-bff 按皮维度读收件箱，语义已对、名字未对 | 删掉，换成 `viewer_member_id`（皮，NETWORK 必填）；与 rec-bff 同批改 |

### 2.2 NETWORK 源改读皮维度关注收件箱

**已由 rec-bff 满足**（2026-09-17）：rec-bff 的 `ListRecommendationCandidates(source=NETWORK)` 直接调皮维度的 `ListMemberFollowInboxV1`（mrpyq service 层 `app/feed/service/internal/service/feed_follow_member_v1.go`，data 层 `app/feed/service/internal/data/feed_member_follow_inbox_v1.go`），不再走账号维度的 `ListAccountFollowInbox`。

两个已记录的语义偏差（见 rec-bff README「为什么不看 is_init」）：

- rec-bff 不检查 7 天 TTL 的 `is_init` 标记（该标记过期后无任何路径刷新，收件箱本体由 HBase 持续维护，看标记会把 7 天未打开关注页的皮判成本初始化）；
- 因此 `source_ready` 实际语义是「本次读取成功」，与 proto 注释「NETWORK is false when its inbox has not been initialized」有出入。若要恢复门控语义，需在字段改名时一并定版。

### 2.3 不需要改的

`RecommendationCandidate` 维持现状，只有 `feed_id`、`source`、`source_score` 即可，作者信息从内容接口获取。

---

## 3. `BatchGetRecommendationContents` — 内容水合

### 3.1 作者字段收敛到一个

`RecommendationContent` 现在同时给出四个作者字段：

```proto
string creator_account_id = 2;
string creator_member_id  = 3;
string creator_user_id    = 4;
int32  creator_user_no    = 5;
```

要求**只保留 `creator_member_id`**，其余三个删除。

### 3.2 `creator_member_id` 必须非空

现状 `po.Feed.MemberId` 是 `bson:"member_id,omitempty"`，且完全由上游调用方填充（`ProtoToFeed` 里 `MemberId: feed.MemberId`，服务端无默认值）。推荐侧拿到空串只能丢弃该候选。

要求 mrpyq：

1. 保证所有发帖路径一定写入 `member_id`；
2. 在本接口上，对没有 `member_id` 的帖子直接判为 `recommendation_eligible = false`，而不是返回空串——让推荐侧自己去判断空值，等于把 mrpyq 的数据质量问题转嫁到调用方。

---

## 4. `GetViewerRelations` — 可见性关系

该接口用于让推荐侧一次性取到 viewer 的全部屏蔽关系，在本地完成过滤，避免每批候选都回一次 RPC。

### 4.1 入参与返回全部换成皮

| 字段 | 现状 | 要求 |
|---|---|---|
| 入参 `account_id` | viewer 账号 | `viewer_member_id`（皮） |
| `blocked_account_ids` | 账号列表 | `blocked_member_ids`——这个皮选择不看的作者皮 |
| `blocked_by_account_ids` | 账号列表 | `blocked_by_member_ids`——拦住了这个皮的作者皮 |
| `muted_account_ids` | 空 | 删除，mrpyq 无独立于 not-see 的 mute 概念 |
| `muted_keywords` | 空 | 删除，mrpyq 无 per-viewer 关键词列表 |

### 4.2 截断必须报错，不能静默返回半份

两个列表都需要明确上限。**列表被截断时必须返回错误**：推荐侧无法区分「被截断了」和「这个皮谁都没屏蔽」，后者会导致本该拦掉的内容被放行。

- 「我不看谁」产品上已有 3000 条上限（`not_see.go:158`），天然有界；
- 「谁拦了我」是反向查询，产品上没有任何上限，要求 mrpyq 补一个上限，或者改成分页接口。

---

## 5. 为使上述接口成立，mrpyq 底层需要的改动

以下三项推荐侧不可见，但不改则接口无法给出正确答案。

### 5.1 `member_not_see`（我不看谁）viewer 侧改为按皮存储

现状从协议开始就没有皮的位置（`api/feed/service/v1/not_see.proto:85`）：

```proto
message NotSeeReq{
  string account_id = 1;                    // viewer 只有账号
  common.v1.MemberRef target_member = 2;    // 只有目标是皮
}
```

上游 `app/webapi/interface/internal/biz/account.go:4131` 的 `CreateNotSee(ctx, accountId, targetMember)` 同样只传账号；Mongo 写 `deviceid` = 账号（`not_see.go:131`）；Redis key 是 `members_not_see_{账号}`（`po/notsee.go:23`）。

结果是「我不看谁」为账号级：同一账号下所有皮共用一份不看名单，换个皮登录照样看不到。而「我不让谁看我」是皮级的，两者语义不对称。

要求 viewer 侧一路改成皮：proto 增加 viewer 皮字段、webapi biz/data 层传皮、Mongo 存储维度、Redis key 结构全部按皮。无历史数据，直接替换即可。

### 5.2 `member_blocklist`（不让谁看我）判断改为皮级

这张表的**数据本来就是皮级的**——Mongo 存了 viewer 的 `device/user/no`（`not_allow_see.go:87`），Redis set 的成员就是 viewer 的 member_key（`po/notsee.go:100`）。是判断逻辑把精度丢了（`not_see.go:273`）：

```go
notAllowSee: pipe.SCard(ctx, po.GetMpAccountNotAllowSeeSetName(publisher, accountID)),
...
result[check.accountID] = !check.notSee.Val() && check.notAllowSee.Val() == 0
```

`SCard > 0` 的语义是「这个账号下任一皮被拦了就算拦」。改成 `SIsMember(key, viewerMemberKey)` 即为皮级判断，**存量数据不需要任何改动**。`ExistNotAllowSeeByAccount`（`not_allow_see.go:187`）同理。

### 5.3 关系表需要存 `member_id`

两张关系表的 `member_id` 字段在 schema 里存在但永远为空，根因在 `app/feed/service/internal/data/po/feed_comment.go:163`：

```go
func ProtoToMemberDict(m *v1.MemberInfo) (*MemberDict, error) {
	...
	return &MemberDict{
		Account: &mongoDB.DBRef{Collection: "device", Id: accountIdHex},
		User:    &mongoDB.DBRef{Collection: "user", Id: userIdHex},
		UserNo:  m.UserNo,
	}, nil   // 入参带了 MemberId，这里没带上
}
```

入参 `v1.MemberInfo` 有 `MemberId`，该函数直接丢弃。帖子那条路径没走 `MemberDict`（`ProtoToFeed` 是 `MemberId: feed.MemberId` 直接赋值），所以帖子有、关系表没有。

补上这一个字段后，关系表即可按 `member_id` 出入，§4 的接口不再需要 `member_id ↔ user_no` 的翻译。

---

## 6. 推荐侧自己的改动

不涉及 mrpyq，但同属本次对齐，列在此处以明确边界：

1. ~~**`home-mixer/clients/mrpyq_adapters.rs:614`** 的 `parse_object_id(&content.creator_account_id)` 改为 `creator_member_id`~~ —— **已完成**。`creator_member_id` 在推荐侧 proto 与客户端结构体中早已就位，rec-bff 也一直在填，只有此处取错了列，属纯本地改动。
2. **可见性过滤挂到 VF 端口**（`MrpyqFirstStageEligibilityClient`），而非 Strato 端口——待做。
3. **Strato 端口退回 Disabled**，或等 §4 接口定型后重接——待做，且必须与 mrpyq §5.1 / rec-bff 同批（见 6.1）。

### 6.1 上线顺序约束（重要，2026-09-17 重写为三方）

**mrpyq 的 §5.1 不能先于 rec-bff 查询键改造与推荐侧的第 2、3 条上线。**

当前链路是：home-mixer `MrpyqStratoClient` 把皮的 `member_id` 填进 `account_id` 字段发给 rec-bff；rec-bff 的 `GetViewerRelations` 实现是「皮 → 账号（`GetMember`）→ 按**账号**翻 `ListNotSee` → 目标换回皮 id（`BatchGetMembersByKeys`）」。代码注释（`home-mixer/clients/mrpyq_adapters.rs`）已明确记录这个风险：成功返回**空列表**会被当成「谁都没屏蔽」放行（fail-open）。

若 mrpyq 先把 not-see 改成按皮存储并部署，而 rec-bff 仍按账号键查，账号键下查不到任何东西 → rec-bff 静默返回空名单 → 该拦的内容全部放行，不报错、不告警。

因此三方必须同批上线，缺一不可：

1. mrpyq：§5.1 not-see 按皮存、按皮查；
2. rec-bff：`GetViewerRelations` 查询键从账号换成皮、删掉 `BatchGetMembersByKeys` 翻译步；
3. 推荐侧：Strato 端口迁到 VF 端口（或先退回 Disabled），字段改名 `viewer_member_id` 一并定版。

在 1–3 完成前，任何一方单方面改动都会把过滤静默失效或编译错位。这条要写进对接清单。

### 6.2 发布前的可执行校验（金丝雀）

上述约束只写在文档里靠人遵守是不可靠的，推荐侧已提供一条默认忽略的集成测试把失效变成显性失败：

```sh
VIEWER_RELATION_CANARY_ADDR=http://<rec-bff>:9000 \
VIEWER_RELATION_CANARY_VIEWER_ID=<拉黑方皮的 member_id> \
VIEWER_RELATION_CANARY_BLOCKED_AUTHOR_ID=<被拉黑作者的 member_id> \
cargo test -p home-mixer --test viewer_relation_canary -- --ignored --nocapture
```

它走生产同款路径（`MrpyqStratoClient` → `GetViewerRelations`），断言一对**预置了拉黑关系**的测试皮中，被拉黑作者仍出现在 `blocked_user_ids` 里。调用成功但已知关系消失 = 恰好就是 §6.1 的静默 fail-open，测试会直接失败并指明原因；调用本身报错则是 fail-closed（空 feed），按服务可达性另行排查。

使用要求：staging 环境常驻一对测试皮并维护其拉黑关系；**三方中任何一方发布键语义相关改动（§5.1 / §5.2 / rec-bff 翻译层 / 推荐侧端口迁移）前后各跑一次**，灰度期间可定时执行。金丝雀不依赖监控指标——空名单分不清「没人拉黑」和「拉黑了但读不到」，只有已知关系能区分。

---

## 7. 改动代价速览

| 项 | 代价 | 阻塞点 |
|---|---|---|
| §2 召回换皮维度收件箱 | **已完成**（rec-bff 直接读 `ListMemberFollowInboxV1`） | 剩余：字段改名与 `source_ready` 语义定版，需与 rec-bff 同批 |
| §3 内容作者字段收敛 | 低，字段已在传 | 需 mrpyq 保证 `member_id` 必填 |
| §4 可见性接口换皮 | 中，依赖 §5 | 依赖 §5.1 / §5.2 |
| §5.1 not_see 按皮存 | 中高，proto → webapi → data → Redis 全链路 | 无历史数据，无迁移成本；上线受 §6.1 三方同批约束 |
| §5.2 not_allow_see 皮级判断 | 极低，改判断方式即可 | 无，数据已是皮级 |
| §5.3 关系表存 member_id | 极低，一个字段 | 无 |
