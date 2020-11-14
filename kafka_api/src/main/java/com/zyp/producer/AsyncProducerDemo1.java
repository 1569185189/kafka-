package com.zyp.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * create by
 *
 * @author zouyuanpeng
 * @date 2020/10/27 11:55
 */
//不带回调函数的API
public class AsyncProducerDemo1 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //kafka集群，bootstrap.servers(*重要*)
        properties.put("bootstrap.servers","ip:port");	//填写自己的ip地址:和端口port
        //ack应答机制(*重要*)
        properties.put("acks","all");
        //重试次数
        properties.put("retries","1");
        //批次大小
        properties.put("batch.size",16384);
        //等待时间
        properties.put("linger.ms",1);
        //RecordAccumulator缓冲区大小
        properties.put("buffer.memory",335544323);
        //键和值的序列化方式(*重要*)
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //创建生产者，发送数据
        Producer<String,String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0;i<10;i++){
            //把每条数据封装成一个ProducerRecord对象
            ProducerRecord<String, String> first = new ProducerRecord<>("first", Integer.toString(i));
            //发送消息
            producer.send(first);

            System.out.println("has sent msg [" + i + "]");
        }
        //关闭生产者
        producer.close();
    }
}
