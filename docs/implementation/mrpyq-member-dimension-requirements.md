# 推荐链路皮维度对齐：对 mrpyq 的接口要求

> **状态**：`proposed`（待 mrpyq 侧排期，推荐侧改动同步待启动）
> **日期**：2026-09-14
> **读者**：mrpyq feed/webapi 后端、推荐服务开发
> **前提**：相关功能尚无历史数据，所有改动不考虑存量迁移与兼容期
> **事实边界**：本文所有「现状」均引自 mrpyq `dev` 分支源码，已标注文件与行号；「要求」是推荐侧提出的接口约定，未经 mrpyq 侧确认

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
| `account_id` | viewer 账号，NETWORK 必填 | 删掉，换成 `viewer_member_id`（皮，NETWORK 必填） |

### 2.2 NETWORK 源改读皮维度关注收件箱

现状 `app/feed/service/internal/service/recommendation_data.go:95` 走的是账号维度的关注收件箱：

```go
inbox, err := s.followReader.ListAccountFollowInbox(ctx, &v1.ListAccountFollowInboxReq{
    AccountId: accountID, ...
```

`ListAccountFollowInboxReq` 只有 `account_id`。一个账号下有多个皮、各自关注的人不同，按账号取会把它们混在一起——A 皮会刷到 B 皮关注的人的帖子。

要求改读皮维度的 `ListMemberFollowInboxV1`。**这套 mrpyq 已经实现完毕**，service 层在 `app/feed/service/internal/service/feed_follow_member_v1.go:18`，data 层在 `app/feed/service/internal/data/feed_member_follow_inbox_v1.go:43`，并有配套测试，只是推荐链路没接过去。

`source_ready` 的语义随之变为「**这个皮**的关注收件箱是否已初始化」，对应 `GetMemberFollowInboxStatus`。

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

1. ~~**`home-mixer/clients/mrpyq_adapters.rs:614`** 的 `parse_object_id(&content.creator_account_id)` 改为 `creator_member_id`~~ —— **已完成**。`creator_member_id` 在推荐侧 proto 与客户端结构体中早已就位，mrpyq `recommendation_data.go:211` 也一直在填，只有此处取错了列，属纯本地改动。
2. **可见性过滤挂到 VF 端口**（`MrpyqFirstStageEligibilityClient`），而非 Strato 端口。
3. **Strato 端口退回 Disabled**——mrpyq 没有对应的 `UserFeatures` 关注图 / 粉丝数契约，保留装配没有意义。

第 2、3 条依赖 §4 的接口定型，尚未动工。

### 6.1 上线顺序约束（重要）

**mrpyq 的 §5.1 不能先于推荐侧的第 2、3 条上线。**

`MrpyqStratoClient` 目前是装配活跃的（`mrpyq_adapters.rs:82` 注入，`phoenix_candidate_pipeline.rs:423` 在非 demo 且有 mrpyq 地址时选用），其 `get_user_features` 的实现是：

```rust
async fn get_user_features(&self, user_id: UserId) -> Result<Vec<u8>, anyhow::Error> {
    let relations = match self.client.get_viewer_relations(user_id.to_string()).await {
```

`user_id` 是 pipeline 传入的 viewer，语义上是皮；`.to_string()` 之后作为 `account_id` 发给 mrpyq。当前 mrpyq 侧 `ViewerRelationService` 尚未上线，该调用不可达，因此不发作。

但若 mrpyq 先按 §5.1 把 not-see 改成皮维度并部署接口，而推荐侧仍是这段代码，行为是 **fail-open**：拿皮的 id 去查一个已经改成按皮键但语义位置仍为 `account_id` 的接口，取不到任何关系 → 屏蔽列表为空 → 该拦的内容全部放行。不报错、不告警，只是静默失效。

两侧必须同批上线，或由推荐侧先将 Strato 端口退回 Disabled 再等 mrpyq 发布。

---

## 7. 改动代价速览

| 项 | 代价 | 阻塞点 |
|---|---|---|
| §2 召回换皮维度收件箱 | 低，皮维度实现已就绪，属接线 | 无 |
| §3 内容作者字段收敛 | 低，字段已在传 | 需 mrpyq 保证 `member_id` 必填 |
| §4 可见性接口换皮 | 中，依赖 §5 | 依赖 §5.1 / §5.2 |
| §5.1 not_see 按皮存 | 中高，proto → webapi → data → Redis 全链路 | 无历史数据，无迁移成本 |
| §5.2 not_allow_see 皮级判断 | 极低，改判断方式即可 | 无，数据已是皮级 |
| §5.3 关系表存 member_id | 极低，一个字段 | 无 |
