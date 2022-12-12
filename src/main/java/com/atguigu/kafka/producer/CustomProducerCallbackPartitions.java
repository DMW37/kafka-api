package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author: 邓明维
 * @date: 2022/12/10
 * @description:
 */
public class CustomProducerCallbackPartitions {
    public static void main(String[] args) throws InterruptedException {
        // 1.创建kafka生成者配置对象
        Properties properties = new Properties();
        // 2.给kafka配置对象配置信息
        // bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:9092");
        // key.serializer &　value.serializer
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 添加自定义的分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,MyPartitioner.class);
        // 3.创建kafka生产对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        // 4.调用send方法发送信息
        for (int i = 0; i < 5; i++) {
            // 添加回调
            kafkaProducer.send(new ProducerRecord<>("first",  "atguigu " + i), new Callback() {
                // 该方法在 Producer 收到 ack 时调用，为异步调用
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (null == exception) {
                        // 没有异常，将元数据返回 topic partition
                        System.out.println("主题:" + metadata.topic() + " 分区:" + metadata.partition() + " offset:" + metadata.offset());
                    } else {
                        // 出现异常，打印
                        exception.printStackTrace();
                    }
                }
            });
            // 延迟发送消息，会发送到不同的分区
            Thread.sleep(2);
        }
        // 关闭资源
        kafkaProducer.close();
    }
}
