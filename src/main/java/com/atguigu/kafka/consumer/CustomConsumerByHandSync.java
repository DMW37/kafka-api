package com.atguigu.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author: 邓明维
 * @date: 2022/12/12
 * @description:
 */
public class CustomConsumerByHandSync {
    public static void main(String[] args) {
        // 1. 创建 kafka 消费者配置类
        Properties properties = new Properties();
        // 2. 添加配置参数
        // bootstrap-server
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"node01:9092");
        // 配置序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        // 配置消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test");
        // 是否自动提交offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        // 3.创建消费者
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        // 4.设置消费主题
        consumer.subscribe(Arrays.asList("first"));
        while (true) {
            ConsumerRecords<String,String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            consumerRecords.forEach(consumerRecord-> System.out.println(consumerRecord));
            // 同步提交offset
            consumer.commitSync();
            // 异步提交
            //consumer.commitAsync();
        }
    }
}
