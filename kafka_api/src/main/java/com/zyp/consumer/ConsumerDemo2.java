package com.zyp.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * create by
 *
 * @author zouyuanpeng
 * @date 2020/11/7 14:55
 */
//手动提交offset，同步提交offset
public class ConsumerDemo2 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //Kafka集群
        properties.put("bootstrap.servers","ip:port");	//填写自己的ip地址:和端口port
        //消费者组，只要group.id相同，就属于同一个消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test");
        //关闭自动提交offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        //键值的反序列化方式
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        //创建kafka消费者
        Consumer<String,String> consumer = new KafkaConsumer<>(properties);
        //消费者订阅的主题
        consumer.subscribe(Collections.singletonList("first"));
        while (true){
            //消费者拉取数据
            ConsumerRecords<String,String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            //输出数据的详细信息
            consumerRecords.forEach(consumerRecord-> System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n",
                    consumerRecord.partition(),consumerRecord.offset(),consumerRecord.key(),consumerRecord.value()));
            //同步提交，当前线程会阻塞直到offset提交成功
            consumer.commitSync();
        }
    }
}
