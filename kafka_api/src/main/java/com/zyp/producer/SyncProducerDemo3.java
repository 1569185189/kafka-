package com.zyp.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * create by
 *
 * @author zouyuanpeng
 * @date 2020/10/27 15:35
 */
//同步发送API
public class SyncProducerDemo3 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        //kafka集群，bootstrap.server
        properties.put("bootstrap.servers","ip:port");	//填写自己的ip地址:和端口port
        //ack机制
        properties.put("acks","all");
        //重试次数
        properties.put("retries",1);
        //批次大小
        properties.put("batch.size",16384);
        //等待时间
        properties.put("linger.ms",1);
        //RecordAccumulator缓冲区大小
        properties.put("buffer.memory",33554432);
        //键值的序列化方式
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //创建kafka生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i=0;i<10;i++){
            //把每条数据封装成一个ProducerRecord对象
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first", Integer.toString(i), Integer.toString(i));
            //发送消息
            //future.get()方法是一个阻塞方法
            producer.send(producerRecord).get();
            /*Future<RecordMetadata> future = producer.send(producerRecord);
            try {
                //future.get()方法是一个阻塞方法
                RecordMetadata recordMetadata = future.get();
                System.out.println("recordMetadata："+recordMetadata);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }*/
        }
        //关闭消费者
        producer.close();
    }
}
