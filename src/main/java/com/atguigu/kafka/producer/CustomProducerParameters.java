package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author: 邓明维
 * @date: 2022/12/11
 * @description:
 */
public class CustomProducerParameters {
    public static void main(String[] args) {
        // 1. 创建 kafka 生产者的配置对象
        Properties properties = new Properties();
        // 2. 给 kafka 配置对象添加配置信息：bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:9092");
        // key,value 序列化（必须）：key.serializer，value.serializer
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        // 修改batch.size 16k
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // 修改linger.ms
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 2);
        // 修改压缩snappy,gzip,lz4,zstd
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        // 修改RecordAccumulator-buffer.memory
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        // 3.创建kafka生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        // 4.调用send方法，发送消息
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("first", "提升生产效率:" + i));
        }
        // 5.关闭资源
        kafkaProducer.close();
    }
}
