package com.zyp.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * create by
 *
 * @author zouyuanpeng
 * @date 2020/11/7 15:32
 */
//手动提交offset，异步提交offset
public class ConsumerDemo3 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //kafka集群
        properties.put("bootstrap.servers","ip:port");	//填写自己的ip地址:和端口port
        //消费者组，只要group.id相同，就属于同一个消费者组
        properties.put("group.id", "test");
        //关闭自动提交offset
        properties.put("enable.auto.commit", "false");
        //键值的反序列化方式
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //创建kafka生产者
        Consumer<String, String> consumer = new KafkaConsumer<>(properties);
        //消费者订阅主题
        consumer.subscribe(Collections.singletonList("first"));
        while (true) {
            //消费者拉取数据
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            //输出数据的详细信息
            consumerRecords.forEach(consumerRecord -> System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n",
                    consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(), consumerRecord.value()));
            //异步提交
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                    if (e != null) {
                        System.out.println("Commit failed for " + map);
                    }
                }
            });
        }
    }
}
