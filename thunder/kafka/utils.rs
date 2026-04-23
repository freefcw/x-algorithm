use anyhow::Result;

use crate::metrics;

/// Kafka 消息简易封装
pub struct KafkaMessage {
    pub payload: Option<Vec<u8>>,
}

/// 批量反序列化 Kafka 消息
pub fn deserialize_kafka_messages<T, F>(
    messages: Vec<KafkaMessage>,
    deserializer: F,
) -> Result<Vec<T>>
where
    F: Fn(&[u8]) -> Result<T>,
{
    let _timer = metrics::Timer::new(metrics::BATCH_PROCESSING_TIME.clone());

    let mut kafka_data = Vec::with_capacity(messages.len());

    for msg in messages.iter() {
        if let Some(payload) = &msg.payload {
            match deserializer(payload) {
                Ok(deserialized_msg) => {
                    kafka_data.push(deserialized_msg);
                }
                Err(e) => {
                    log::error!("Failed to parse Kafka message: {}", e);
                    metrics::KAFKA_MESSAGES_FAILED_PARSE.inc();
                }
            }
        }
    }

    Ok(kafka_data)
}
