package com.zyp.mysql_offset;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Properties;

/**
 * create by
 *
 * @author zouyuanpeng
 * @date 2020/11/7 19:23
 */
public class ProducerOffset {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //kafka集群
        properties.put("bootstrap.servers","ip:port");	//填写自己的ip地址:和端口port
        //ack应答机制
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        //重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, "3");
        //批次大小
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        //等待时间
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //RecordAccumulator缓冲区大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 335544323);
        //键和值的序列化方式
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //创建kafka生产者
        Producer<String, String> producer = new KafkaProducer<>(properties);
        //发送消息到broker
        for (int i = 0; i < 10; i++) {
            //把每一条消封装成ProducerRecord
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first", Integer.toString(i), Integer.toString(i));
            //发送消息
            producer.send(producerRecord);
            System.out.println("has sent msg  " + i );
        }
        //关闭生产者
        producer.close();
    }
}
