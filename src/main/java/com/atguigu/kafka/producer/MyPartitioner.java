package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author: 邓明维
 * @date: 2022/12/10
 * @description:
 */

/**
 * 1. 实现接口 Partitioner
 * 2. 实现 3 个方法:partition,close,configure
 * 3. 编写 partition 方法,返回分区号
 */
public class MyPartitioner implements Partitioner {
    /**
     * @param topic      主题
     * @param key        消息的key
     * @param keyBytes   消息的key序列化后的字节数组
     * @param value      消息的value
     * @param valueBytes 消息的value序列化后的字节数组
     * @param cluster    集群元数据可以查看分区信息
     * @return
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 1.获取信息
        String msgValue = value.toString();
        // 2.穿件partition
        int partition;
        // 判断是否包含atguigu
        if (msgValue.contains("atguigu")) {
            partition = 0;
        } else {
            partition = 1;
        }
        // 返回分区号
        return partition;
    }

    // 关闭资源
    @Override
    public void close() {

    }

    // 配置方法
    @Override
    public void configure(Map<String, ?> configs) {

    }
}
