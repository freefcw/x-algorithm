# UAS 客户端用户行为上报定义

> **状态**：当前实现对应的客户端上报说明  
> **读者**：iOS、Android、Web 客户端，埋点 SDK，mrpyq 行为转发服务  
> **更新日期**：2026-09-17  
> **相关实现**：[UAS 事件校验](../home-mixer/clients/uas_fetcher.rs)、[行为类型与场景范围](../home-mixer/recsys_compat/mod.rs)、[ActionName 定义](../proto/definitions/phoenix_recsys.proto)  
> **配套合同**：[UAS 事件流合同](implementation/uas-event-contract.md)

## 1. 目的与数据流

UAS（User Action Sequence）记录用户最近对帖子的行为，供 Phoenix 召回和精排使用。客户端不直接访问 Redis 或 Kafka，只负责在动作完成后生成事件，并通过现有客户端埋点通道发送给 mrpyq；mrpyq 再把事件原样写入 UAS Kafka topic。

```text
客户端动作
  → 客户端埋点通道
  → mrpyq 行为转发
  → Kafka：一条消息一个 JSON 对象
  → uas-worker
  → Redis UAS 序列
  → Home Mixer 聚合
  → Phoenix
```

这条流是**行为历史流**，不是曝光流。用户只是看到、刷到或划过帖子时不要上报；曝光和训练归因应使用独立事件流。

## 2. 客户端需要上报什么

### 2.1 上报责任

客户端负责：

- 客户端才能确认的帖子行为：点击详情、展开图片、有效观看视频、分享、复制链接、进入作者主页、停留等。
- 从某条帖子入口触发、服务端无法关联原帖的行为：关注作者、拉黑作者、静音作者。若服务端事件已经能准确关联到原帖，则由服务端单独上报，客户端不可重复上报。
- 每条事件的 `product_surface`，即动作发生时用户所在的产品入口。

服务端负责：

- 点赞：点赞成功并落库后上报 `action_type=1`。
- 评论或回复：评论成功并落库后上报 `action_type=2`。
- 如果服务端已经负责关注、拉黑、静音或举报事件，客户端只上报埋点给业务侧，不再把同一动作转发到 UAS。

同一个动作只能有一个 UAS 生产者。客户端在按钮点击时提前上报、服务端在落库成功后再次上报，会造成重复行为或记录未成功的动作。

### 2.2 事件粒度

- 一次动作一条事件；不要把多个动作放进数组。
- 同一帖子先点击详情、再分享、再复制链接，应分别上报三条事件。
- 一次分享可以同时产生通用分享和渠道分享两个事件，见 [动作定义](#4-action_type动作定义)。
- 相同事件重复发送是安全的：消费端以完整事件内容作为 Redis 成员，重复投递不会新增重复成员。但客户端仍应使用本地事件队列、重试和去重，避免无意义流量。

## 3. 事件格式

Kafka message 的 value 必须是 UTF-8 编码的 JSON 对象，不带外层 envelope，不是数组，也不是 JSON Lines 中的一条多事件数组。客户端可以通过 mrpyq 既有通道批量传输，但落到 Kafka 后必须恢复为“一条消息一个对象”。

### 3.1 字段定义

| 字段 | JSON 类型 | 必填 | 定义与取值来源 | 校验要求 |
|---|---|---:|---|---|
| `user_id` | string | 是 | 执行动作的当前皮（`member_id`），必须等于推荐请求中的 `viewer_id` | 24 位小写十六进制 ObjectId；不能是全 0 |
| `tweet_id` | string | 是 | 被操作帖子的 `feed_id` | 24 位小写十六进制 ObjectId；不能是全 0 |
| `author_id` | string | 是 | 被操作帖子的作者 `creator_member_id` | 24 位小写十六进制 ObjectId；不能是全 0 |
| `action_time_ms` | integer | 是 | 动作实际完成时间，UTC Unix epoch 毫秒 | 必须大于 0；不能使用发送时间、入队时间或消费时间 |
| `action_type` | integer | 是 | [proto `ActionName`](../proto/definitions/phoenix_recsys.proto) 的数值 | 当前入口接受 `1..18`；不能传字符串 |
| `product_surface` | integer | 是 | 动作发生时所在的产品入口 | 当前接受 `0..15`；不能传字符串；缺失时消费端兼容为 `0`，新客户端仍必须显式发送 |

三个 ID 必须来自同一“皮”身份空间：不要把登录账号 ID、`account_id`、展示用数字号、`user_no` 或其他用户体系的 ID 填入这些字段。`author_id` 必须取帖子数据本身的作者 ID，不能用当前登录用户 ID 替代。

### 3.2 示例

```json
{
  "user_id": "66f1a2b3c4d5e6f708192a3b",
  "tweet_id": "66f1a2b3c4d5e6f708192a3c",
  "author_id": "66f1a2b3c4d5e6f708192a3d",
  "action_time_ms": 1789516800500,
  "action_type": 6,
  "product_surface": 2
}
```

上例表示：用户在搜索结果入口打开了一条帖子详情。

### 3.3 时间与投递规则

- 客户端应使用设备当前 UTC epoch 毫秒，并尽量与服务端时钟同步。
- `action_time_ms` 早于消费端当前时间 7 天的事件不会写入 UAS。
- `action_time_ms` 晚于消费端当前时间 5 分钟的事件会被判为未来事件并跳过。
- Kafka 不要求客户端保证全局顺序；消费端按 `action_time_ms` 排序。若可以设置 Kafka partition key，建议使用 `user_id`。
- 建议使用 at-least-once 投递。网络失败时可以重试，不要因为担心重复而丢弃动作。
- 建议在 mrpyq 转发层保留 `event_id` 作为排障和后续幂等键，但当前 UAS 消费端忽略该字段；`event_id` 不能替代上述六个字段。

## 4. `action_type` 动作定义

编号从 1 开始，与训练文档中可能出现的 0 起始编码不同。客户端和转发服务必须使用下面的 proto 编号。

### 4.1 客户端行为

| 值 | proto 名称 | 客户端动作 | 触发时机 | 当前客户端策略 |
|---:|---|---|---|---|
| 5 | `CLIENT_TWEET_PHOTO_EXPAND` | 展开帖子图片 | 图片成功进入大图状态后 | 有能力就报 |
| 6 | `CLIENT_TWEET_CLICK` | 打开帖子详情 | 从 Feed、列表或卡片进入详情页后 | 应报 |
| 7 | `CLIENT_TWEET_CLICK_PROFILE` | 打开作者主页 | 成功进入作者主页后 | 有能力就报 |
| 8 | `CLIENT_TWEET_VIDEO_QUALITY_VIEW` | 视频有效观看（VQV） | 连续观看至少 10 秒或视频播放完成；每帖每会话最多一次 | 有能力就报，阈值需产品确认 |
| 9 | `CLIENT_TWEET_SHARE` | 分享帖子 | 分享面板中的任一渠道完成后 | 应报 |
| 10 | `CLIENT_TWEET_CLICK_SEND_VIA_DIRECT_MESSAGE` | 通过私信/站内消息分享 | 私信发送成功后 | 选择私信时额外报 |
| 11 | `CLIENT_TWEET_SHARE_VIA_COPY_LINK` | 复制帖子链接 | 系统确认复制成功后 | 复制链接时额外报 |
| 12 | `CLIENT_TWEET_RECAP_DWELLED` | 停留（Dwell） | 帖子在有效可视区累计或连续停留至少 2 秒；每帖上报一次 | 有能力就报，阈值需产品确认 |
| 13 | `CLIENT_QUOTED_TWEET_CLICK` | 打开引用帖子 | 进入引用帖详情后 | 协议可接收；当前产品无对应入口时不要伪造 |
| 14 | `CLIENT_TWEET_FOLLOW_AUTHOR` | 从帖子入口关注作者 | 关注成功后，并且客户端是唯一能把动作关联到该帖的生产者 | 服务端已关联时不要报 |
| 15 | `CLIENT_TWEET_NOT_INTERESTED_IN` | 对帖子标记“不感兴趣” | 用户操作成功后 | 产品有该入口且没有服务端重复事件时才报 |
| 16 | `CLIENT_TWEET_BLOCK_AUTHOR` | 拉黑作者 | 拉黑成功后，并且动作来自该帖入口 | 服务端已关联时不要报 |
| 17 | `CLIENT_TWEET_MUTE_AUTHOR` | 静音作者 | 静音成功后，并且动作来自该帖入口 | 服务端已关联时不要报 |
| 18 | `CLIENT_TWEET_REPORT` | 举报帖子 | 举报提交成功后 | 应报；服务端若已生成同一事件则不要重复 |

分享场景的组合规则：

- 普通分享：报 `9`。
- 私信分享：报 `9` 和 `10` 两条。
- 复制链接：报 `9` 和 `11` 两条。
- 同一动作组合产生的事件应使用相同的 `user_id`、`tweet_id`、`author_id` 和 `product_surface`；时间戳可以相同或相差几毫秒。

### 4.2 服务端行为：客户端不要重复上报

| 值 | proto 名称 | 服务端触发条件 |
|---:|---|---|
| 1 | `SERVER_TWEET_FAV` | 点赞成功并落库 |
| 2 | `SERVER_TWEET_REPLY` | 评论/回复成功并落库 |
| 3 | `SERVER_TWEET_RETWEET` | 产品实际支持转发且服务端成功落库 |
| 4 | `SERVER_TWEET_QUOTE` | 产品实际支持引用转发且服务端成功落库 |

当前产品若不支持 `3`、`4`，不要发送。取消点赞、取消关注、解除拉黑、解除静音等撤销动作没有对应的 `ActionName`，不要自行定义负数或新字符串。

### 4.3 与当前模型版本的关系

UAS 接收端接受 `1..18`，表示协议可以存储和聚合这些动作；这不等于当前 Phoenix checkpoint 会为每个动作输出非零排序权重。当前 Home Mixer 发布配置中，非零权重的行为是：

- `1` 点赞：正向权重 `0.5`；
- `2` 回复：正向权重 `5.0`；
- `18` 举报：负向权重 `-234.0`。

其他已接收的动作仍会进入历史序列，供后续模型或特征版本使用，但当前版本不应据此承诺排序分数一定变化。模型升级时，以服务端发布的 `supported-actions` 和对应模型合同为准。

## 5. `product_surface` 入口定义

`product_surface` 描述**动作发生时用户正在使用的入口**，不是帖子属性，也不是动作类型。相同的点赞动作，在首页和搜索中发生时 `action_type` 都是 `1`，只改变 `product_surface`。

| 值 | 入口 | 判定规则 |
|---:|---|---|
| 0 | 首页推荐 / For You | 首页推荐 Feed 或默认推荐列表 |
| 1 | 关注流 / Following | 已关注用户的时间线 |
| 2 | 搜索 | 搜索结果页或搜索结果中的帖子 |
| 3 | 话题 / 标签 | 话题页、标签页或对应聚合列表 |
| 4–15 | 预留 | 未经推荐侧确认不得使用 |

判定示例：

- 从首页打开详情后点赞：使用 `product_surface=0`。
- 从搜索结果打开详情后分享：使用 `product_surface=2`。
- 从关注流打开详情后复制链接：使用 `product_surface=1`。
- 站外推送或深链打开详情且没有明确入口：暂用 `0`，并在联调时说明来源；不要发送 `99`、`"push"` 或其他自定义值。
- 同一帖子在不同入口分别发生行为时分别上报；消费端按帖子聚合时，会保留该帖子最早行为的入口码。

入口上下文应在帖子卡片创建或导航时保存，不能等详情页加载完成后用默认值覆盖。这样才能区分“从搜索进入详情”和“直接打开首页详情”。

## 6. 消费端处理方式及客户端影响

UAS 消费端会：

1. 校验 JSON、三个 ID、时间、动作编号和入口编号；
2. 跳过超过 7 天或未来超过 5 分钟的事件；
3. 每个用户保留最多 600 条原始行为，Redis TTL 为 7 天；
4. 按帖子合并行为，生成 `action_mask`，并按最早行为时间排序；
5. 每个帖子只保留最早一条行为的 `product_surface`；
6. Home Mixer 读取聚合后的序列，当前最多保留 300 个帖子，Phoenix 网关再使用其模型历史长度。

事件校验失败会记录 `invalid` 并推进 offset，不会等待重试。因此客户端字段错误的结果不是“稍后生效”，而是该行为直接不进入推荐历史。常见错误包括：

- 把 `action_type` 或 `product_surface` 编成字符串；
- 把毫秒传成秒；
- 使用登录账号 ID 而不是当前皮的 `member_id`；
- `author_id` 使用当前 viewer ID 或展示名；
- 把曝光、刷过、取消动作当成 UAS 行为发送；
- 设备时间错误，导致事件落在时间窗口之外。

## 7. 客户端实现清单

- [ ] 每次动作完成后生成一条 JSON 对象。
- [ ] 六个协议字段全部存在；`action_type` 和 `product_surface` 是 JSON 数字。
- [ ] `user_id` 使用当前皮，并与推荐请求的 `viewer_id` 一致。
- [ ] `tweet_id` 和 `author_id` 直接使用帖子数据中的 `feed_id`、`creator_member_id`。
- [ ] 所有 ID 是 24 位小写 hex ObjectId，且不是全 0。
- [ ] `action_time_ms` 使用动作完成时间，单位为 UTC epoch 毫秒。
- [ ] 详情页动作沿用进入详情前的 `product_surface`。
- [ ] 私信分享发送 `9+10`；复制链接发送 `9+11`。
- [ ] 点赞和评论不由客户端向 UAS 重复发送。
- [ ] 不发送曝光、刷到、划过和撤销动作。
- [ ] 网络失败时进入可靠队列并重试；不要因为重试可能重复而静默丢弃。
- [ ] 联调时确认 Kafka 中是一条消息一个 JSON 对象，并检查 `invalid` 为 0。

## 8. 联调样例

下面是可以交给转发层做格式自检的两条事件（时间戳需替换成测试时的当前时间）：

```json
{"user_id":"66f1a2b3c4d5e6f708192a3b","tweet_id":"66f1a2b3c4d5e6f708192a3c","author_id":"66f1a2b3c4d5e6f708192a3d","action_time_ms":1789516800000,"action_type":6,"product_surface":0}
{"user_id":"66f1a2b3c4d5e6f708192a3b","tweet_id":"66f1a2b3c4d5e6f708192a3c","author_id":"66f1a2b3c4d5e6f708192a3d","action_time_ms":1789516800500,"action_type":9,"product_surface":0}
```

本地可通过 stdin 模式验证投影格式：

```bash
cat events.jsonl \
  | UAS_REDIS_URL=redis://localhost:6379/ \
    RUST_LOG=info \
    cargo run -p home-mixer --bin uas-worker
```

验证时应满足：

- 日志中没有 `dropping invalid UAS event`；
- Redis 中能看到对应用户的 UAS ZSET 成员；
- 重放相同事件后，成员数量不增加；
- 对用户发起推荐请求时，Home Mixer 能读取到行为序列。
