package com.zyp.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

/**
 * create by
 *
 * @author zouyuanpeng
 * @date 2020/11/7 13:54
 */
//自动提交offset
public class ConsumerDemo1_1 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //kafka集群
        properties.put("bootstrap.servers","ip:port");	//填写自己的ip地址:和端口port
        //消费者组
        properties.put("group.id","test");
        //是否开启自动提交offset功能
        properties.put("enable.auto.commit","true");
        //自动提交offset的时间间隔
        properties.put("enable.commit.interval.ms","1000");
        //键值的反序列化方式
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //创建Kafka消费者
        Consumer<String,String> consumer = new KafkaConsumer<>(properties);
        //消费者所订阅的主题,singletonList一个仅包含指定对象的不可变列表
        consumer.subscribe(Collections.singletonList("first"));
        while(true){
            //消费者拉取到的数据记录对象的集合
            ConsumerRecords<String,String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String,String> consumerRecord : consumerRecords){
                //输出消费的数据信息
                System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n",
                        consumerRecord.partition(),consumerRecord.offset(),consumerRecord.key(),consumerRecord.value());
            }
        }
    }
}
