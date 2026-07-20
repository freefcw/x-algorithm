Created feed_csv_importer.py — converts the Kafka CSV export into the parquet format that data_preprocessor.py expects.

Key design decisions:

 - 保留逐事件行：仅对同秒内的 (user, post) 事件做合并，保留完整时间线供训练管线构造历史序列
 - 不编造连续值：vqv/dwell_time 在 CSV 中无实际数值，设为 0 避免污染 label
 - 分 chunk 流式处理：29M 行数据按 50 万行分块读取，按日期分区写出
 - 自动生成元数据：从事件中推断 post_metadata 和 user_metadata

使用方式：

 cd phoenix
 
 # 1. 导入 CSV → parquet
 uv run feed_csv_importer.py \
   --csv /Users/hejun/work/mp/recommend/data/feed_doc_kafka/feed_event.csv \
   --output-dir data/real_data
 
 # 2. 预处理 → 训练样本
 uv run data_preprocessor.py \
   --behavior-dir data/real_data/behavior_logs \
   --post-meta data/real_data/post_metadata.parquet \
   --output-dir data/training_samples

 ⚠️ 注意：当前 CSV 数据仅包含 event_type 0 (点赞) 和 1 (回复)，其他行为头在训练时信号会很弱。29M 行的完整导入预计需要几分钟。