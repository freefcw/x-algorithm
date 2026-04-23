已经将生产的部分用户事件流消息已经导出为本地cvs
文件为 `/Users/hejun/work/mp/recommend/data/feed_doc_kafka/feed_event.csv` 

文件的格式为：
account_id,feed_id,author_id,event_time,event_type,event_source

event_type为
| 0 | `favorite_score` | 点赞 | 0 或 1 |
| 1 | `reply_score` | 回复 | 0 或 1 |
| 2 | `repost_score` | 转发 | 0 或 1 |
| 3 | `photo_expand_score` | 图片展开 | 0 或 1 |
| 4 | `click_score` | 点击详情 | 0 或 1 |
| 5 | `profile_click_score` | 点击作者主页 | 0 或 1 |
| 6 | `vqv_score` | 视频播放质量分 | 0~1 连续值 |
| 7 | `share_score` | 分享（任意方式） | 0 或 1 |
| 8 | `share_via_dm_score` | 私信分享 | 0 或 1 |
| 9 | `share_via_copy_link_score` | 复制链接分享 | 0 或 1 |
| 10 | `dwell_score` | 是否停留超过时间阈值 | 0 或 1 |
| 11 | `quote_score` | 引用转发 | 0 或 1 |
| 12 | `quoted_click_score` | 点击引用内容 | 0 或 1 |
| 13 | `follow_author_score` | 关注作者 | 0 或 1 |
| 14 | `not_interested_score` | 标记不感兴趣 | 0 或 1 |
| 15 | `block_author_score` | 屏蔽作者 | 0 或 1 |
| 16 | `mute_author_score` | 静音作者 | 0 或 1 |
| 17 | `report_score` | 举报 | 0 或 1 |
| 18 | `dwell_time` | 具体停留时长（秒，归一化后） | 连续值 |

现在需要将这些数据导入到phoenix中，用于训练和评估模型。需要对这个csv文件进行处理，生成训练和评估所需的格式。需要生成 data_preprocessor.py 脚本，参考 data_preprocessor.py 的实现方式，将csv文件转换为phoenix所需的格式。